use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};
use ulid::Ulid;

use crate::config::database::Dialect;
use crate::state::AppState;
use crate::store;
use crate::store::Db;
use crate::time::iso_now;
use crate::ws::messages::ServerMessage;

static INSTANCE_ID: Lazy<String> = Lazy::new(|| Ulid::new().to_string());

pub fn instance_id() -> &'static str {
    &INSTANCE_ID
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplicationEvent {
    origin: String,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    trigger: String,
}

/// Fire-and-forget: publish an event to all other registry instances.
/// SQLite deployments are single-instance; publish is a no-op for them.
pub fn publish(db: &Db, event: &str, id: Option<&str>, trigger: &str) {
    let payload = match serde_json::to_string(&ReplicationEvent {
        origin: INSTANCE_ID.clone(),
        event: event.to_string(),
        id: id.map(str::to_string),
        trigger: trigger.to_string(),
    }) {
        Ok(s) => s,
        Err(_) => return,
    };

    match db.dialect {
        Dialect::Postgres => {
            let db = db.clone();
            tokio::spawn(async move {
                if let Err(e) = sqlx::query(db.dialect.prep("SELECT pg_notify('nexus_changes', ?)"))
                    .bind(&payload)
                    .execute(db.pool())
                    .await
                {
                    warn!("[replication] pg_notify failed: {}", e);
                }
            });
        }
        Dialect::MySql => {
            let db = db.clone();
            let row_id = Ulid::new().to_string();
            let created_at = iso_now();
            let origin = INSTANCE_ID.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    sqlx::query(db.dialect.prep(
                        "INSERT INTO event_queue (id, payload, origin, created_at) VALUES (?, ?, ?, ?)",
                    ))
                    .bind(&row_id)
                    .bind(&payload)
                    .bind(&origin)
                    .bind(&created_at)
                    .execute(db.pool())
                    .await
                {
                    warn!("[replication] event_queue insert failed: {}", e);
                }
            });
        }
        Dialect::Sqlite => {}
    }
}

/// Spawn the background listener appropriate for the configured database.
/// Should be called once after AppState is fully built.
pub fn start_listener(state: AppState, db_url: String) {
    info!("[replication] instance: {}", instance_id());
    match state.db.dialect {
        Dialect::Postgres => {
            tokio::spawn(async move {
                postgres_listener(state, db_url).await;
            });
        }
        Dialect::MySql => {
            tokio::spawn(async move {
                mysql_poller(state).await;
            });
        }
        Dialect::Sqlite => {
            info!("[replication] SQLite: single-instance mode, replication disabled");
        }
    }
}

async fn postgres_listener(state: AppState, db_url: String) {
    loop {
        match sqlx::postgres::PgListener::connect(&db_url).await {
            Ok(mut listener) => {
                if let Err(e) = listener.listen("nexus_changes").await {
                    warn!("[replication] LISTEN failed: {e}");
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
                info!("[replication] Postgres LISTEN/NOTIFY active");
                loop {
                    match listener.recv().await {
                        Ok(note) => process_event(&state, note.payload()).await,
                        Err(e) => {
                            warn!("[replication] listener error, reconnecting: {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("[replication] cannot connect listener: {e}");
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn mysql_poller(state: AppState) {
    let cursor: String = sqlx::query_scalar::<_, String>("SELECT COALESCE(MAX(id), '') FROM event_queue")
        .fetch_one(state.db.pool())
        .await
        .unwrap_or_default();

    let mut cursor = cursor;
    let mut prune_tick: u32 = 0;
    info!("[replication] MySQL event_queue poller active");

    loop {
        sleep(Duration::from_secs(1)).await;

        let sql = state
            .db
            .dialect
            .render("SELECT id, payload FROM event_queue WHERE id > ? ORDER BY id ASC LIMIT 50");
        let rows = match sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
            .bind(&cursor)
            .fetch_all(state.db.pool())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("[replication] event_queue poll failed: {e}");
                continue;
            }
        };

        for row in &rows {
            let Ok(id) = row.try_get::<String, _>("id") else {
                continue;
            };
            let Ok(payload) = row.try_get::<String, _>("payload") else {
                continue;
            };
            process_event(&state, &payload).await;
            cursor = id;
        }

        prune_tick += 1;
        if prune_tick >= 30 {
            prune_tick = 0;
            let cutoff = chrono::Utc::now()
                .checked_sub_signed(chrono::Duration::seconds(60))
                .unwrap_or_else(chrono::Utc::now)
                .to_rfc3339();
            let sql = state
                .db
                .dialect
                .render("DELETE FROM event_queue WHERE created_at < ?");
            if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
                .bind(&cutoff)
                .execute(state.db.pool())
                .await
            {
                warn!("[replication] event_queue prune failed: {e}");
            }
        }
    }
}

async fn process_event(state: &AppState, raw: &str) {
    let Ok(event) = serde_json::from_str::<ReplicationEvent>(raw) else {
        warn!("[replication] unparseable event: {raw}");
        return;
    };

    if event.origin == *INSTANCE_ID {
        return;
    }

    let tx = &state.broadcast_tx;
    let timestamp = iso_now();

    match event.event.as_str() {
        "remotes_changed" => {
            if tx.receiver_count() == 0 {
                return;
            }
            let remotes = match store::list(&state.db, None).await {
                Ok((r, _)) => r,
                Err(e) => {
                    warn!("[replication] failed to load remotes: {e}");
                    return;
                }
            };
            let _ = tx.send(ServerMessage::RemotesChanged {
                timestamp,
                remotes,
                trigger: format!("replicated:{}", event.trigger),
            });
        }
        "host_changed" => {
            let Some(id) = &event.id else {
                return;
            };
            let host = match store::get_host(&state.db, id).await {
                Ok(Some(h)) => h,
                Ok(None) => return,
                Err(e) => {
                    warn!("[replication] failed to load host {id}: {e}");
                    return;
                }
            };
            let _ = tx.send(ServerMessage::HostChanged {
                timestamp,
                host,
                trigger: format!("replicated:{}", event.trigger),
            });
        }
        "gate_changed" => {
            let Some(name) = &event.id else {
                return;
            };
            let gate = match store::get_gate(&state.db, name).await {
                Ok(Some(g)) => g,
                Ok(None) => return,
                Err(e) => {
                    warn!("[replication] failed to load gate {name}: {e}");
                    return;
                }
            };
            let _ = tx.send(ServerMessage::GateChanged {
                timestamp,
                gate,
                trigger: format!("replicated:{}", event.trigger),
                old_host_id: None,
                new_host_id: None,
            });
        }
        other => {
            warn!("[replication] unknown event type: {other}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_is_stable() {
        let id1 = instance_id();
        let id2 = instance_id();
        assert!(!id1.is_empty());
        assert_eq!(id1, id2);
    }

    #[test]
    fn replication_event_round_trips() {
        let payload = serde_json::to_string(&ReplicationEvent {
            origin: "ORIGINTEST".into(),
            event: "remotes_changed".into(),
            id: None,
            trigger: "create".into(),
        })
        .unwrap();
        let ev: ReplicationEvent = serde_json::from_str(&payload).unwrap();
        assert_eq!(ev.origin, "ORIGINTEST");
        assert_eq!(ev.event, "remotes_changed");
        assert!(ev.id.is_none());
    }

    #[test]
    fn replication_event_with_id_round_trips() {
        let payload = serde_json::to_string(&ReplicationEvent {
            origin: "ORIGINTEST".into(),
            event: "host_changed".into(),
            id: Some("host-123".into()),
            trigger: "update".into(),
        })
        .unwrap();
        let ev: ReplicationEvent = serde_json::from_str(&payload).unwrap();
        assert_eq!(ev.id.as_deref(), Some("host-123"));
    }
}
