// Host + gate CRUD. Kept in a sibling file to sqlite.rs to keep that file
// focused on the remotes table and migration plumbing.

use sqlx::Row;

use crate::store::sqlite::{is_unique_violation_pub as is_unique_violation, Db, ListPage, StoreError};
use crate::types::{Gate, GateWithHost, Host, HostWithGateCount, UpdateGateRequest, UpdateHostRequest};

// ============================================================================
// Hosts
// ============================================================================

fn row_to_host(r: &sqlx::any::AnyRow) -> Result<Host, sqlx::Error> {
    Ok(Host {
        id: r.try_get("id")?,
        name: r.try_get("name")?,
        url: r.try_get("url")?,
        framework: r.try_get("framework")?,
        remote_entry: r.try_get("remote_entry")?,
        exposed_module: r.try_get("exposed_module")?,
        enabled: r.try_get::<i64, _>("enabled")? != 0,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

pub async fn list_hosts(db: &Db, page: Option<&ListPage>) -> Result<(Vec<HostWithGateCount>, u64), StoreError> {
    const BASE: &str =
        "SELECT h.id, h.name, h.url, h.framework, h.remote_entry, h.exposed_module, \
         h.enabled, h.created_at, h.updated_at, COUNT(g.id) AS gate_count \
         FROM hosts h LEFT JOIN gates g ON g.host_id = h.id \
         GROUP BY h.id, h.name, h.url, h.framework, h.remote_entry, h.exposed_module, \
                  h.enabled, h.created_at, h.updated_at \
         ORDER BY h.created_at";

    fn extract(rows: Vec<sqlx::any::AnyRow>) -> Result<Vec<HostWithGateCount>, sqlx::Error> {
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let host = row_to_host(&r)?;
            let gate_count: i64 = r.try_get("gate_count")?;
            out.push(HostWithGateCount { host, gate_count });
        }
        Ok(out)
    }

    match page {
        None => {
            let sql = db.dialect.render(BASE);
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
                .fetch_all(db.pool())
                .await?;
            let items = extract(rows).map_err(StoreError::Db)?;
            let total = items.len() as u64;
            Ok((items, total))
        }
        Some(p) => {
            let count_sql = db.dialect.render("SELECT COUNT(*) FROM hosts");
            let total: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql.as_ref()))
                .fetch_one(db.pool())
                .await?;
            let paged = format!("{BASE} LIMIT ? OFFSET ?");
            let paged_sql = db.dialect.render(&paged);
            let rows = sqlx::query(sqlx::AssertSqlSafe(paged_sql.as_ref()))
                .bind(p.limit as i64)
                .bind(p.offset as i64)
                .fetch_all(db.pool())
                .await?;
            let items = extract(rows).map_err(StoreError::Db)?;
            Ok((items, total as u64))
        }
    }
}

pub async fn get_host(db: &Db, id_or_name: &str) -> Result<Option<Host>, StoreError> {
    let sql = db.dialect.render(
        "SELECT id, name, url, framework, remote_entry, exposed_module, enabled, created_at, updated_at \
         FROM hosts WHERE id = ? OR name = ? LIMIT 1",
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(db.pool())
        .await?;
    match row {
        Some(r) => Ok(Some(row_to_host(&r)?)),
        None => Ok(None),
    }
}

pub async fn host_exists(db: &Db, id: &str) -> Result<bool, StoreError> {
    let sql = db.dialect.render("SELECT 1 FROM hosts WHERE id = ? LIMIT 1");
    let row: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(id)
        .fetch_optional(db.pool())
        .await?;
    Ok(row.is_some())
}

pub async fn insert_host(db: &Db, host: &Host) -> Result<(), StoreError> {
    let sql = db.dialect.render(
        "INSERT INTO hosts (id, name, url, framework, remote_entry, exposed_module, enabled, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(&host.id)
        .bind(&host.name)
        .bind(&host.url)
        .bind(&host.framework)
        .bind(&host.remote_entry)
        .bind(&host.exposed_module)
        .bind(host.enabled as i64)
        .bind(&host.created_at)
        .bind(&host.updated_at)
        .execute(db.pool())
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db_err)) if is_unique_violation(&*db_err) => {
            Err(StoreError::Conflict(host.name.clone()))
        }
        Err(e) => Err(StoreError::Db(e)),
    }
}

pub async fn update_host(
    db: &Db,
    id_or_name: &str,
    patch: &UpdateHostRequest,
    now: &str,
) -> Result<Option<Host>, StoreError> {
    let mut tx = db.pool().begin().await?;

    let select_sql = db.dialect.render(
        "SELECT id, name, url, framework, remote_entry, exposed_module, enabled, created_at, updated_at \
         FROM hosts WHERE id = ? OR name = ? LIMIT 1",
    );
    let existing = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let existing = row_to_host(&existing)?;

    let merged = Host {
        id: existing.id.clone(),
        name: patch.name.clone().unwrap_or(existing.name),
        url: patch.url.clone().unwrap_or(existing.url),
        framework: patch.framework.clone().unwrap_or(existing.framework),
        remote_entry: patch.remote_entry.clone().unwrap_or(existing.remote_entry),
        exposed_module: patch.exposed_module.clone().unwrap_or(existing.exposed_module),
        enabled: patch.enabled.unwrap_or(existing.enabled),
        created_at: existing.created_at,
        updated_at: now.to_string(),
    };

    let update_sql = db.dialect.render(
        "UPDATE hosts SET name = ?, url = ?, framework = ?, remote_entry = ?, exposed_module = ?, \
         enabled = ?, updated_at = ? WHERE id = ?",
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(update_sql.as_ref()))
        .bind(&merged.name)
        .bind(&merged.url)
        .bind(&merged.framework)
        .bind(&merged.remote_entry)
        .bind(&merged.exposed_module)
        .bind(merged.enabled as i64)
        .bind(&merged.updated_at)
        .bind(&merged.id)
        .execute(&mut *tx)
        .await;
    if let Err(sqlx::Error::Database(db_err)) = &res {
        if is_unique_violation(&**db_err) {
            return Err(StoreError::Conflict(merged.name));
        }
    }
    res.map_err(StoreError::Db)?;
    tx.commit().await?;
    Ok(Some(merged))
}

pub async fn gate_names_for_host(db: &Db, host_id: &str) -> Result<Vec<String>, StoreError> {
    let sql = db
        .dialect
        .render("SELECT name FROM gates WHERE host_id = ? ORDER BY name");
    let names: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(host_id)
        .fetch_all(db.pool())
        .await?;
    Ok(names)
}

pub enum DeleteHostOutcome {
    Deleted(Host),
    Blocked(Vec<String>),
    NotFound,
}

pub async fn delete_host(db: &Db, id_or_name: &str) -> Result<DeleteHostOutcome, StoreError> {
    let host = match get_host(db, id_or_name).await? {
        Some(h) => h,
        None => return Ok(DeleteHostOutcome::NotFound),
    };
    let blocking = gate_names_for_host(db, &host.id).await?;
    if !blocking.is_empty() {
        return Ok(DeleteHostOutcome::Blocked(blocking));
    }
    let sql = db.dialect.render("DELETE FROM hosts WHERE id = ?");
    let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(&host.id)
        .execute(db.pool())
        .await?;
    if res.rows_affected() > 0 {
        Ok(DeleteHostOutcome::Deleted(host))
    } else {
        Ok(DeleteHostOutcome::NotFound)
    }
}

pub async fn toggle_host(db: &Db, id_or_name: &str, now: &str) -> Result<Option<Host>, StoreError> {
    let mut tx = db.pool().begin().await?;
    let select_sql = db.dialect.render(
        "SELECT id, name, url, framework, remote_entry, exposed_module, enabled, created_at, updated_at \
         FROM hosts WHERE id = ? OR name = ? LIMIT 1",
    );
    let existing = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let mut host = row_to_host(&existing)?;
    let next = !host.enabled;
    let update_sql = db
        .dialect
        .render("UPDATE hosts SET enabled = ?, updated_at = ? WHERE id = ?");
    sqlx::query(sqlx::AssertSqlSafe(update_sql.as_ref()))
        .bind(next as i64)
        .bind(now)
        .bind(&host.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    host.enabled = next;
    host.updated_at = now.to_string();
    Ok(Some(host))
}

// ============================================================================
// Gates
// ============================================================================

fn row_to_gate(r: &sqlx::any::AnyRow) -> Result<Gate, sqlx::Error> {
    Ok(Gate {
        id: r.try_get("id")?,
        name: r.try_get("name")?,
        domain: r.try_get("domain")?,
        host_id: r.try_get("host_id")?,
        enabled: r.try_get::<i64, _>("enabled")? != 0,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

fn row_to_gate_with_host(r: &sqlx::any::AnyRow) -> Result<GateWithHost, sqlx::Error> {
    let gate = Gate {
        id: r.try_get("g_id")?,
        name: r.try_get("g_name")?,
        domain: r.try_get("g_domain")?,
        host_id: r.try_get("g_host_id")?,
        enabled: r.try_get::<i64, _>("g_enabled")? != 0,
        created_at: r.try_get("g_created_at")?,
        updated_at: r.try_get("g_updated_at")?,
    };
    let host_id: Option<String> = r.try_get("h_id")?;
    let host = if let Some(id) = host_id {
        Some(Host {
            id,
            name: r.try_get("h_name")?,
            url: r.try_get("h_url")?,
            framework: r.try_get("h_framework")?,
            remote_entry: r.try_get("h_remote_entry")?,
            exposed_module: r.try_get("h_exposed_module")?,
            enabled: r.try_get::<i64, _>("h_enabled")? != 0,
            created_at: r.try_get("h_created_at")?,
            updated_at: r.try_get("h_updated_at")?,
        })
    } else {
        None
    };
    Ok(GateWithHost { gate, host })
}

const GATE_WITH_HOST_SELECT: &str =
    "SELECT g.id AS g_id, g.name AS g_name, g.domain AS g_domain, g.host_id AS g_host_id, \
     g.enabled AS g_enabled, g.created_at AS g_created_at, g.updated_at AS g_updated_at, \
     h.id AS h_id, h.name AS h_name, h.url AS h_url, h.framework AS h_framework, \
     h.remote_entry AS h_remote_entry, h.exposed_module AS h_exposed_module, \
     h.enabled AS h_enabled, h.created_at AS h_created_at, h.updated_at AS h_updated_at \
     FROM gates g LEFT JOIN hosts h ON h.id = g.host_id";

pub async fn list_gates(db: &Db, page: Option<&ListPage>) -> Result<(Vec<GateWithHost>, u64), StoreError> {
    fn extract(rows: Vec<sqlx::any::AnyRow>) -> Result<Vec<GateWithHost>, sqlx::Error> {
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_gate_with_host(&r)?);
        }
        Ok(out)
    }

    match page {
        None => {
            let raw = format!("{GATE_WITH_HOST_SELECT} ORDER BY g.created_at");
            let sql = db.dialect.render(&raw);
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
                .fetch_all(db.pool())
                .await?;
            let items = extract(rows).map_err(StoreError::Db)?;
            let total = items.len() as u64;
            Ok((items, total))
        }
        Some(p) => {
            let count_sql = db.dialect.render("SELECT COUNT(*) FROM gates");
            let total: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql.as_ref()))
                .fetch_one(db.pool())
                .await?;
            let paged = format!("{GATE_WITH_HOST_SELECT} ORDER BY g.created_at LIMIT ? OFFSET ?");
            let paged_sql = db.dialect.render(&paged);
            let rows = sqlx::query(sqlx::AssertSqlSafe(paged_sql.as_ref()))
                .bind(p.limit as i64)
                .bind(p.offset as i64)
                .fetch_all(db.pool())
                .await?;
            let items = extract(rows).map_err(StoreError::Db)?;
            Ok((items, total as u64))
        }
    }
}

pub async fn get_gate(db: &Db, id_or_name: &str) -> Result<Option<GateWithHost>, StoreError> {
    let raw = format!("{} WHERE g.id = ? OR g.name = ? LIMIT 1", GATE_WITH_HOST_SELECT);
    let sql = db.dialect.render(&raw);
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(db.pool())
        .await?;
    match row {
        Some(r) => Ok(Some(row_to_gate_with_host(&r)?)),
        None => Ok(None),
    }
}

pub async fn get_gate_by_domain(db: &Db, domain: &str) -> Result<Option<GateWithHost>, StoreError> {
    let raw = format!("{} WHERE g.domain = ? LIMIT 1", GATE_WITH_HOST_SELECT);
    let sql = db.dialect.render(&raw);
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(domain)
        .fetch_optional(db.pool())
        .await?;
    match row {
        Some(r) => Ok(Some(row_to_gate_with_host(&r)?)),
        None => Ok(None),
    }
}

pub async fn insert_gate(db: &Db, gate: &Gate) -> Result<(), StoreError> {
    let sql = db.dialect.render(
        "INSERT INTO gates (id, name, domain, host_id, enabled, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(&gate.id)
        .bind(&gate.name)
        .bind(&gate.domain)
        .bind(&gate.host_id)
        .bind(gate.enabled as i64)
        .bind(&gate.created_at)
        .bind(&gate.updated_at)
        .execute(db.pool())
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(db_err)) if is_unique_violation(&*db_err) => {
            Err(StoreError::Conflict(gate.name.clone()))
        }
        Err(e) => Err(StoreError::Db(e)),
    }
}

/// Returns (new_gate, Some(old_host_id) if host changed — inner Option<String> is the previous value).
pub async fn update_gate(
    db: &Db,
    id_or_name: &str,
    patch: &UpdateGateRequest,
    now: &str,
) -> Result<Option<(Gate, Option<Option<String>>)>, StoreError> {
    let mut tx = db.pool().begin().await?;
    let select_sql = db.dialect.render(
        "SELECT id, name, domain, host_id, enabled, created_at, updated_at \
         FROM gates WHERE id = ? OR name = ? LIMIT 1",
    );
    let existing = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let existing = row_to_gate(&existing)?;

    let old_host_id = existing.host_id.clone();
    let merged = Gate {
        id: existing.id.clone(),
        name: patch.name.clone().unwrap_or(existing.name),
        domain: patch.domain.clone().unwrap_or(existing.domain),
        host_id: patch.host_id.clone().or(existing.host_id),
        enabled: patch.enabled.unwrap_or(existing.enabled),
        created_at: existing.created_at,
        updated_at: now.to_string(),
    };

    let update_sql = db.dialect.render(
        "UPDATE gates SET name = ?, domain = ?, host_id = ?, enabled = ?, updated_at = ? WHERE id = ?",
    );
    let res = sqlx::query(sqlx::AssertSqlSafe(update_sql.as_ref()))
        .bind(&merged.name)
        .bind(&merged.domain)
        .bind(&merged.host_id)
        .bind(merged.enabled as i64)
        .bind(&merged.updated_at)
        .bind(&merged.id)
        .execute(&mut *tx)
        .await;
    if let Err(sqlx::Error::Database(db_err)) = &res {
        if is_unique_violation(&**db_err) {
            return Err(StoreError::Conflict(merged.name));
        }
    }
    res.map_err(StoreError::Db)?;
    tx.commit().await?;

    let host_changed = if merged.host_id != old_host_id {
        Some(old_host_id)
    } else {
        None::<Option<String>>
    };
    Ok(Some((merged, host_changed)))
}

pub async fn delete_gate(db: &Db, id_or_name: &str) -> Result<Option<Gate>, StoreError> {
    let mut tx = db.pool().begin().await?;
    let select_sql = db.dialect.render(
        "SELECT id, name, domain, host_id, enabled, created_at, updated_at \
         FROM gates WHERE id = ? OR name = ? LIMIT 1",
    );
    let existing = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let existing = row_to_gate(&existing)?;
    let delete_sql = db.dialect.render("DELETE FROM gates WHERE id = ?");
    sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_ref()))
        .bind(&existing.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(existing))
}

fn placeholders(n: usize) -> String {
    std::iter::repeat_n("?", n).collect::<Vec<_>>().join(", ")
}

/// Sets `enabled` for each host ID in a single UPDATE. Returns affected count.
pub async fn toggle_hosts_many(db: &Db, ids: &[String], enabled: bool, now: &str) -> Result<u64, StoreError> {
    if ids.is_empty() {
        return Ok(0);
    }
    let raw = format!(
        "UPDATE hosts SET enabled = ?, updated_at = ? WHERE id IN ({})",
        placeholders(ids.len())
    );
    let sql = db.dialect.render(&raw);
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(enabled as i64)
        .bind(now);
    for id in ids {
        q = q.bind(id.as_str());
    }
    let res = q.execute(db.pool()).await?;
    Ok(res.rows_affected())
}

/// Sets `enabled` for each gate ID in a single UPDATE. Returns affected count.
pub async fn toggle_gates_many(db: &Db, ids: &[String], enabled: bool, now: &str) -> Result<u64, StoreError> {
    if ids.is_empty() {
        return Ok(0);
    }
    let raw = format!(
        "UPDATE gates SET enabled = ?, updated_at = ? WHERE id IN ({})",
        placeholders(ids.len())
    );
    let sql = db.dialect.render(&raw);
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.as_ref()))
        .bind(enabled as i64)
        .bind(now);
    for id in ids {
        q = q.bind(id.as_str());
    }
    let res = q.execute(db.pool()).await?;
    Ok(res.rows_affected())
}

pub async fn toggle_gate(db: &Db, id_or_name: &str, now: &str) -> Result<Option<Gate>, StoreError> {
    let mut tx = db.pool().begin().await?;
    let select_sql = db.dialect.render(
        "SELECT id, name, domain, host_id, enabled, created_at, updated_at \
         FROM gates WHERE id = ? OR name = ? LIMIT 1",
    );
    let existing = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_ref()))
        .bind(id_or_name)
        .bind(id_or_name)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let mut gate = row_to_gate(&existing)?;
    let next = !gate.enabled;
    let update_sql = db
        .dialect
        .render("UPDATE gates SET enabled = ?, updated_at = ? WHERE id = ?");
    sqlx::query(sqlx::AssertSqlSafe(update_sql.as_ref()))
        .bind(next as i64)
        .bind(now)
        .bind(&gate.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    gate.enabled = next;
    gate.updated_at = now.to_string();
    Ok(Some(gate))
}
