use sqlx::PgPool;
use uuid::Uuid;

use crate::services::agent::AgentRegistry;
use crate::services::notifications;

const MAX_LIVE_ATTEMPTS: usize = 2;
const MAX_RECORDED_FAILURES: i64 = 3;

#[derive(Debug)]
pub(crate) struct VerificationOutcome {
    pub(crate) passed: bool,
    pub(crate) checks_run: i32,
    pub(crate) checks_passed: i32,
    pub(crate) duration_ms: i32,
    pub(crate) details: serde_json::Value,
    pub(crate) error_message: Option<String>,
}

impl VerificationOutcome {
    pub(crate) fn from_agent_response(data: &serde_json::Value) -> Self {
        let passed = data
            .get("passed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let checks_run = data.get("checks_run").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
        let checks_passed = data
            .get("checks_passed")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        let duration_ms = data
            .get("duration_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        let details = data
            .get("details")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        let error_message = (!passed)
            .then(|| {
                details
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|check| {
                        !check
                            .get("passed")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .map(|check| {
                        let name = check
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("check");
                        let message = check
                            .get("message")
                            .and_then(|v| v.as_str())
                            .unwrap_or("failed");
                        format!("{name}: {message}")
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .filter(|message| !message.is_empty());

        Self {
            passed,
            checks_run,
            checks_passed,
            duration_ms,
            details,
            error_message,
        }
    }
}

pub(crate) fn verification_state_key(backup_type: &str, backup_id: Uuid) -> String {
    format!("{backup_type}:{backup_id}")
}

pub(crate) async fn post_verification_with_retry(
    agent: &crate::services::agent::AgentHandle,
    path: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    for attempt in 1..=MAX_LIVE_ATTEMPTS {
        let response = agent
            .post(path, Some(body.clone()))
            .await
            .map_err(|e| e.to_string());
        let should_retry = match &response {
            Ok(data) => !data
                .get("passed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            Err(_) => true,
        };

        if !should_retry || attempt == MAX_LIVE_ATTEMPTS {
            return response;
        }

        tracing::warn!(
            "Backup verification attempt {attempt}/{MAX_LIVE_ATTEMPTS} failed on {path}; retrying once"
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    unreachable!("the verification retry loop always returns")
}

/// Keep one alert per archive and close it as soon as the same archive passes.
/// Legacy rows used an empty key; migrate only the matching title before resolving
/// so a recovery cannot hide a different failed backup.
pub(crate) async fn reconcile_verification_alert(
    db: &PgPool,
    user_id: Uuid,
    backup_type: &str,
    backup_id: Uuid,
    name: &str,
    filename: &str,
    passed: bool,
    checks_passed: i32,
    checks_run: i32,
    error_message: Option<&str>,
) {
    let state_key = verification_state_key(backup_type, backup_id);
    let failure_title = format!("Backup verification failed: {name}");

    if passed {
        if let Err(e) = sqlx::query(
            "UPDATE alerts SET state_key = $3 \
             WHERE user_id = $1 AND alert_type = 'backup_verification_failed' \
             AND state_key = '' AND title = $2 AND status = 'firing'",
        )
        .bind(user_id)
        .bind(&failure_title)
        .bind(&state_key)
        .execute(db)
        .await
        {
            tracing::warn!("Failed to scope legacy backup verification alert: {e}");
        }

        notifications::resolve_alert(
            db,
            user_id,
            None,
            None,
            "backup_verification_failed",
            &state_key,
            &format!("Backup verification recovered: {name}"),
            &format!("The {backup_type} backup '{filename}' for {name} now passes verification."),
        )
        .await;
        return;
    }

    let already_firing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM alerts \
         WHERE user_id = $1 AND alert_type = 'backup_verification_failed' \
         AND state_key = $2 AND status = 'firing')",
    )
    .bind(user_id)
    .bind(&state_key)
    .fetch_one(db)
    .await
    .unwrap_or(false);

    if already_firing {
        return;
    }

    let detail = error_message
        .map(|message| format!(" Reason: {message}."))
        .unwrap_or_default();
    notifications::fire_alert(
        db,
        user_id,
        None,
        None,
        "backup_verification_failed",
        &state_key,
        "warning",
        &failure_title,
        &format!(
            "The {backup_type} backup '{filename}' for {name} failed verification \
             ({checks_passed}/{checks_run} checks passed).{detail}"
        ),
    )
    .await;
}

/// Run the backup verifier — every 6 hours, picks unverified backups and verifies them.
///
/// Every driving query is fleet-wide, so the verifier gets the REGISTRY: an archive is
/// read on the host that wrote it. Verifying a member's backup against the panel's disk
/// finds nothing, marks a good archive `failed`, and — because the driving queries skip
/// any backup that already has a verification row — never looks at it again.
pub async fn run(
    db: PgPool,
    agents: AgentRegistry,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    tracing::info!("Backup verifier started");

    // Initial delay: 5 minutes after startup
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(300)) => {}
        _ = shutdown_rx.recv() => {
            tracing::info!("Backup verifier shutting down (initial delay)");
            return;
        }
    }

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600)); // 6 hours

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = tick(&db, &agents).await {
                    tracing::error!("Backup verifier error: {e}");
                }
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Backup verifier shutting down gracefully");
                break;
            }
        }
    }
}

async fn tick(db: &PgPool, agents: &AgentRegistry) -> Result<(), String> {
    // Find policies that have verify_after_backup enabled
    let policies_exist: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM backup_policies WHERE verify_after_backup = TRUE AND enabled = TRUE",
    )
    .fetch_one(db)
    .await
    .map_err(|e| e.to_string())?;

    if policies_exist.0 == 0 {
        return Ok(());
    }

    // Pick up to 3 site backups that have not passed yet. A failed row remains
    // eligible for a bounded number of later cycles instead of permanently burning
    // the archive's only verification attempt.
    // `s.server_id` rides the JOIN this query already makes — the same shape
    // `preview_cleanup` uses, and the reason no migration is needed for `backups`.
    // It is NOT NULL (20260319000000_multi_server.sql:76) and a site never moves
    // hosts: nothing in the codebase issues `UPDATE sites SET server_id`.
    let site_backups: Vec<(Uuid, String, String, Uuid)> = sqlx::query_as(
        "SELECT b.id, s.domain, b.filename, s.server_id FROM backups b \
         JOIN sites s ON s.id = b.site_id \
         WHERE b.created_at > NOW() - INTERVAL '7 days' \
         AND NOT EXISTS (SELECT 1 FROM backup_verifications bv \
             WHERE bv.backup_type = 'site' AND bv.backup_id = b.id \
             AND bv.status IN ('passed', 'running')) \
         AND (SELECT COUNT(*) FROM backup_verifications bv \
             WHERE bv.backup_type = 'site' AND bv.backup_id = b.id \
             AND bv.status = 'failed') < $1 \
         ORDER BY b.created_at DESC LIMIT 3",
    )
    .bind(MAX_RECORDED_FAILURES)
    .fetch_all(db)
    .await
    .map_err(|e| e.to_string())?;

    for (backup_id, domain, filename, server_id) in &site_backups {
        verify_one(
            db, agents, *server_id, "site", *backup_id, domain, filename, None, None,
        )
        .await;
    }

    // Database backups resolve their server through the site that owns the database,
    // not through `database_backups.server_id`: that column is nullable and a policy
    // with no server writes NULL into it. `sites.server_id` is NOT NULL, so this join
    // cannot produce an ambiguous host. Same join `create_db_backup` already makes.
    let db_backups: Vec<(Uuid, String, String, String, Uuid)> = sqlx::query_as(
        "SELECT db.id, db.db_type, db.db_name, db.filename, s.server_id FROM database_backups db \
         JOIN databases d ON d.id = db.database_id \
         JOIN sites s ON s.id = d.site_id \
         WHERE db.created_at > NOW() - INTERVAL '7 days' \
         AND NOT EXISTS (SELECT 1 FROM backup_verifications bv \
             WHERE bv.backup_type = 'database' AND bv.backup_id = db.id \
             AND bv.status IN ('passed', 'running')) \
         AND (SELECT COUNT(*) FROM backup_verifications bv \
             WHERE bv.backup_type = 'database' AND bv.backup_id = db.id \
             AND bv.status = 'failed') < $1 \
         ORDER BY db.created_at DESC LIMIT 3",
    )
    .bind(MAX_RECORDED_FAILURES)
    .fetch_all(db)
    .await
    .map_err(|e| e.to_string())?;

    for (backup_id, db_type, db_name, filename, server_id) in &db_backups {
        verify_one(
            db,
            agents,
            *server_id,
            "database",
            *backup_id,
            db_name,
            filename,
            Some(db_type),
            None,
        )
        .await;
    }

    // A volume backup has no site to join through, so `vb.server_id` is the only
    // handle — which is why `create_volume_backup` had to stop stamping it from
    // "some online server". Rows written before that fix may still carry a wrong or
    // NULL id; NULL ones are skipped below rather than silently verified here.
    let vol_backups: Vec<(Uuid, String, String, Option<Uuid>)> = sqlx::query_as(
        "SELECT vb.id, vb.container_name, vb.filename, vb.server_id FROM volume_backups vb \
         WHERE vb.created_at > NOW() - INTERVAL '7 days' \
         AND NOT EXISTS (SELECT 1 FROM backup_verifications bv \
             WHERE bv.backup_type = 'volume' AND bv.backup_id = vb.id \
             AND bv.status IN ('passed', 'running')) \
         AND (SELECT COUNT(*) FROM backup_verifications bv \
             WHERE bv.backup_type = 'volume' AND bv.backup_id = vb.id \
             AND bv.status = 'failed') < $1 \
         ORDER BY vb.created_at DESC LIMIT 2",
    )
    .bind(MAX_RECORDED_FAILURES)
    .fetch_all(db)
    .await
    .map_err(|e| e.to_string())?;

    for (backup_id, container_name, filename, server_id) in &vol_backups {
        let Some(sid) = server_id else {
            // Deliberately NOT `for_server_or_local`. An unscoped volume backup is a
            // row we cannot place, and defaulting it to this host is exactly the
            // wrong-host read being removed. Leaving it unverified keeps it eligible.
            tracing::warn!(
                "Volume backup {backup_id} ({filename}) has no server_id — leaving it \
                 unverified rather than reading this host's disk for it."
            );
            continue;
        };
        verify_one(
            db,
            agents,
            *sid,
            "volume",
            *backup_id,
            container_name,
            filename,
            None,
            Some(container_name.as_str()),
        )
        .await;
    }

    // "considered", not "verified": a row whose server is unreachable is skipped
    // above, and the old wording printed the same line whether every backup passed
    // or every one errored.
    let total = site_backups.len() + db_backups.len() + vol_backups.len();
    if total > 0 {
        tracing::info!("Backup verifier: considered {total} backups");
    }

    Ok(())
}

async fn verify_one(
    db: &PgPool,
    agents: &AgentRegistry,
    server_id: Uuid,
    backup_type: &str,
    backup_id: Uuid,
    name: &str,
    filename: &str,
    db_type: Option<&str>,
    container_name: Option<&str>,
) {
    // Resolve BEFORE inserting the record. A verification row is written 'running'
    // and the driving queries skip any backup that already has one, so a row created
    // for a server we then decline to reach would burn this backup's only chance of
    // ever being verified.
    let agent = match agents.for_server(server_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(
                "Not verifying {backup_type} backup {filename} ({backup_id}): its server \
                 {server_id} is unreachable ({e}). Leaving it unverified so a later tick \
                 can retry, rather than reading this host's disk for another host's archive."
            );
            return;
        }
    };

    // `server_id` is bound: the row records which host actually read the archive.
    let verif_id: Uuid = match sqlx::query_scalar(
        "INSERT INTO backup_verifications (backup_type, backup_id, server_id, status, started_at) \
         VALUES ($1, $2, $3, 'running', NOW()) RETURNING id",
    )
    .bind(backup_type)
    .bind(backup_id)
    .bind(server_id)
    .fetch_one(db)
    .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("Failed to create verification record: {e}");
            return;
        }
    };

    let result = match backup_type {
        "site" => {
            let body = serde_json::json!({ "domain": name, "filename": filename });
            post_verification_with_retry(&agent, "/backups/verify/site", body).await
        }
        "database" => {
            let body = serde_json::json!({
                "db_type": db_type.unwrap_or("postgres"),
                "db_name": name,
                "filename": filename,
            });
            post_verification_with_retry(&agent, "/backups/verify/database", body).await
        }
        "volume" => {
            let body = serde_json::json!({
                "container_name": container_name.unwrap_or(name),
                "filename": filename,
            });
            post_verification_with_retry(&agent, "/backups/verify/volume", body).await
        }
        _ => return,
    };

    match result {
        Ok(data) => {
            let outcome = VerificationOutcome::from_agent_response(&data);
            let status = if outcome.passed { "passed" } else { "failed" };

            let _ = sqlx::query(
                "UPDATE backup_verifications SET \
                 status = $2, checks_run = $3, checks_passed = $4, \
                 details = $5, duration_ms = $6, error_message = $7, completed_at = NOW() \
                 WHERE id = $1",
            )
            .bind(verif_id)
            .bind(status)
            .bind(outcome.checks_run)
            .bind(outcome.checks_passed)
            .bind(&outcome.details)
            .bind(outcome.duration_ms)
            .bind(&outcome.error_message)
            .execute(db)
            .await;

            if let Ok(Some((user_id,))) =
                sqlx::query_as::<_, (Uuid,)>("SELECT id FROM users WHERE role = 'admin' LIMIT 1")
                    .fetch_optional(db)
                    .await
            {
                reconcile_verification_alert(
                    db,
                    user_id,
                    backup_type,
                    backup_id,
                    name,
                    filename,
                    outcome.passed,
                    outcome.checks_passed,
                    outcome.checks_run,
                    outcome.error_message.as_deref(),
                )
                .await;
            }

            tracing::info!("Verification {status}: {backup_type} backup {filename} for {name}");
        }
        Err(e) => {
            let err_msg = e.to_string();
            let _ = sqlx::query(
                "UPDATE backup_verifications SET status = 'failed', error_message = $2, completed_at = NOW() WHERE id = $1"
            ).bind(verif_id).bind(&err_msg).execute(db).await;

            if let Ok(Some((user_id,))) =
                sqlx::query_as::<_, (Uuid,)>("SELECT id FROM users WHERE role = 'admin' LIMIT 1")
                    .fetch_optional(db)
                    .await
            {
                reconcile_verification_alert(
                    db,
                    user_id,
                    backup_type,
                    backup_id,
                    name,
                    filename,
                    false,
                    0,
                    0,
                    Some(&err_msg),
                )
                .await;
            }

            tracing::error!("Verification failed for {backup_type} {filename}: {err_msg}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{VerificationOutcome, verification_state_key};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn alert_key_is_unique_per_backup() {
        let first = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let second = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

        assert_ne!(
            verification_state_key("database", first),
            verification_state_key("database", second)
        );
        assert_ne!(
            verification_state_key("database", first),
            verification_state_key("site", first)
        );
    }

    #[test]
    fn failed_agent_response_exposes_the_failed_check() {
        let outcome = VerificationOutcome::from_agent_response(&json!({
            "passed": false,
            "checks_run": 3,
            "checks_passed": 2,
            "duration_ms": 5767,
            "details": [
                {"name": "file_exists", "passed": true, "message": "Dump file present"},
                {"name": "temp_container", "passed": false, "message": "Database container was not ready"},
                {"name": "restore", "passed": true, "message": "Restore completed"}
            ]
        }));

        assert!(!outcome.passed);
        assert_eq!(
            outcome.error_message.as_deref(),
            Some("temp_container: Database container was not ready")
        );
    }

    #[test]
    fn passed_agent_response_has_no_error_message() {
        let outcome = VerificationOutcome::from_agent_response(&json!({
            "passed": true,
            "checks_run": 4,
            "checks_passed": 4,
            "duration_ms": 42,
            "details": []
        }));

        assert!(outcome.passed);
        assert!(outcome.error_message.is_none());
    }
}
