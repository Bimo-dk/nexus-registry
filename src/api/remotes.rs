use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info};

use crate::correlation::CorrelationId;
use crate::http_error::error_response;
use crate::state::AppState;
use crate::store::{self, audit, versions, ListPage, StoreError};
use crate::time::iso_now;
use crate::types::{
    AddRemoteRequest, BulkDeleteRequest, BulkToggleRequest, RegistryResponse, RemoteConfig,
    UpdateRemoteRequest,
};
use crate::validators::{is_valid_remote_name, is_valid_route_path, is_valid_url_or_path, parse_visibility};
use crate::ws::broadcast_remotes_changed;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list).post(create))
        .route("/bulk-toggle", post(bulk_toggle))
        .route("/bulk-delete", post(bulk_delete))
        .route("/{name}", get(detail).put(update).delete(remove))
        .route("/{name}/toggle", post(toggle))
        .route("/{name}/redeploy", post(redeploy))
        .route("/{name}/versions", get(list_versions))
        .route("/{name}/rollback", post(rollback))
}

const MAX_PAGE_SIZE: u32 = 200;
const MAX_BULK: usize = 100;

#[derive(Deserialize)]
struct ListQuery {
    #[serde(rename = "host_id")]
    host_id: Option<String>,
    page: Option<u32>,
    limit: Option<u32>,
}

fn parse_page(q_page: Option<u32>, q_limit: Option<u32>) -> Option<ListPage> {
    match (q_page, q_limit) {
        (None, None) => None,
        (pg, lim) => {
            let limit = lim.unwrap_or(50).clamp(1, MAX_PAGE_SIZE) as u64;
            let page = pg.unwrap_or(1).max(1) as u64;
            Some(ListPage { limit, offset: (page - 1) * limit })
        }
    }
}

fn pagination_fields(lp: &Option<ListPage>, total: u64) -> (Option<u32>, Option<u32>, Option<u32>) {
    match lp {
        None => (None, None, None),
        Some(p) => {
            let page_num = (p.offset / p.limit + 1) as u32;
            let page_count = if p.limit > 0 { ((total + p.limit - 1) / p.limit) as u32 } else { 0 };
            (Some(page_num), Some(p.limit as u32), Some(page_count))
        }
    }
}

async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let lp = parse_page(q.page, q.limit);
    let result = match q.host_id.as_deref() {
        Some(host_id) => store::list_for_host(&state.db, host_id, lp.as_ref()).await,
        None => store::list(&state.db, lp.as_ref()).await,
    };
    match result {
        Ok((remotes, total)) => {
            let enabled = remotes.iter().filter(|r| r.enabled).count();
            let (page, page_size, page_count) = pagination_fields(&lp, total);
            Json(RegistryResponse {
                remotes,
                total: total as usize,
                enabled,
                page,
                page_size,
                page_count,
            })
            .into_response()
        }
        Err(e) => server_error(e, "<no-correlation-id>"),
    }
}

async fn detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::get(&state.db, &name).await {
        Ok(Some(remote)) => Json(remote).into_response(),
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Remote \"{}\" not found", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, cid.as_str()),
    }
}

/// Validates the `visibility` string format and (if host-specific) confirms
/// the referenced host actually exists. Returns `Ok(canonical_value)` or `Err(message)`.
async fn validate_visibility(db: &store::Db, raw: &str) -> Result<String, String> {
    match parse_visibility(raw) {
        Ok(None) => Ok("global".to_string()),
        Ok(Some(host_id)) => {
            let exists = store::host_exists(db, host_id)
                .await
                .map_err(|e| format!("database error checking host: {}", e))?;
            if !exists {
                return Err(format!(
                    "visibility references host_id \"{}\" which does not exist",
                    host_id
                ));
            }
            Ok(raw.to_string())
        }
        Err(msg) => Err(msg.to_string()),
    }
}

async fn create(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<AddRemoteRequest>>,
) -> Response {
    let Some(Json(body)) = body else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "JSON body required",
            cid.as_str(),
        );
    };

    let name = body.name.unwrap_or_default();
    let url = body.url.unwrap_or_default();
    let route_path = body.route_path.unwrap_or_default();
    let exposed_module = body.exposed_module.unwrap_or_else(|| "./RemoteEntry".to_string());
    let enabled = body.enabled.unwrap_or(true);
    let raw_visibility = body.visibility.unwrap_or_else(|| "global".to_string());

    if let Some(msg) = validate_new(&name, &url, &route_path, &exposed_module) {
        error!("[remotes] [{}] POST validation failed: {}", cid.as_str(), msg);
        return error_response(StatusCode::BAD_REQUEST, "validation_failed", msg, cid.as_str());
    }
    let visibility = match validate_visibility(&state.db, &raw_visibility).await {
        Ok(v) => v,
        Err(msg) => {
            error!("[remotes] [{}] POST visibility failed: {}", cid.as_str(), msg);
            return error_response(StatusCode::BAD_REQUEST, "validation_failed", msg, cid.as_str());
        }
    };

    let remote = RemoteConfig {
        name,
        url,
        exposed_module,
        route_path,
        enabled,
        added_at: iso_now(),
        upstream_url: body.upstream_url,
        health_status: None,
        last_health_check: None,
        visibility,
    };

    match store::insert(&state.db, &remote).await {
        Ok(()) => {
            let trigger = format!("add:{}", remote.name);
            audit::append(state.db.clone(), "remote", &remote.name, "created", cid.as_str(), None);
            versions::record(&state.db, &remote).await;
            let res = (StatusCode::CREATED, Json(remote)).into_response();
            broadcast_remotes_changed(&state, trigger).await;
            res
        }
        Err(StoreError::Conflict(name)) => {
            error!(
                "[remotes] [{}] POST conflict: Remote \"{}\" already exists",
                cid.as_str(),
                name
            );
            error_response(
                StatusCode::CONFLICT,
                "conflict",
                format!("Remote \"{}\" already exists", name),
                cid.as_str(),
            )
        }
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn update(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<UpdateRemoteRequest>>,
) -> Response {
    let Some(Json(mut body)) = body else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "JSON body required",
            cid.as_str(),
        );
    };

    if let Some(ref rp) = body.route_path {
        if !is_valid_route_path(rp) {
            return error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "routePath must be kebab-case starting with a lowercase letter",
                cid.as_str(),
            );
        }
    }
    if let Some(ref u) = body.url {
        if !is_valid_url_or_path(u) {
            return error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "url must be a valid http(s) URL or absolute path",
                cid.as_str(),
            );
        }
    }
    if let Some(ref v) = body.visibility {
        match validate_visibility(&state.db, v).await {
            Ok(canonical) => body.visibility = Some(canonical),
            Err(msg) => {
                return error_response(StatusCode::BAD_REQUEST, "validation_failed", msg, cid.as_str());
            }
        }
    }

    match store::update(&state.db, &name, body).await {
        Ok(Some(remote)) => {
            let trigger = format!("update:{}", remote.name);
            audit::append(state.db.clone(), "remote", &remote.name, "updated", cid.as_str(), None);
            versions::record(&state.db, &remote).await;
            let res = Json(remote).into_response();
            broadcast_remotes_changed(&state, trigger).await;
            res
        }
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Remote \"{}\" not found", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn remove(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::delete(&state.db, &name).await {
        Ok(true) => {
            let trigger = format!("delete:{}", name);
            audit::append(state.db.clone(), "remote", &name, "deleted", cid.as_str(), None);
            let res = StatusCode::NO_CONTENT.into_response();
            broadcast_remotes_changed(&state, trigger).await;
            res
        }
        Ok(false) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Remote \"{}\" not found", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn toggle(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::toggle(&state.db, &name).await {
        Ok(Some(remote)) => {
            let trigger = format!("toggle:{}", remote.name);
            audit::append(state.db.clone(), "remote", &remote.name, "toggled", cid.as_str(), None);
            let res = Json(remote).into_response();
            broadcast_remotes_changed(&state, trigger).await;
            res
        }
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Remote \"{}\" not found", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn redeploy(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::get(&state.db, &name).await {
        Ok(Some(remote)) => {
            let ts = iso_now();
            info!(
                "[registry] [{}] Redeploy signal for \"{}\" at {}",
                cid.as_str(),
                remote.name,
                ts
            );
            audit::append(state.db.clone(), "remote", &remote.name, "redeployed", cid.as_str(), None);
            (
                StatusCode::ACCEPTED,
                Json(json!({
                    "accepted": true,
                    "remote": remote.name,
                    "timestamp": ts,
                    "correlationId": cid.as_str(),
                    "note": "Redeploy is logged. Container orchestration (Docker Swarm/K8s) is responsible for actually redeploying.",
                })),
            )
                .into_response()
        }
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Remote \"{}\" not found", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn bulk_toggle(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<BulkToggleRequest>>,
) -> Response {
    let Some(Json(body)) = body else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_body", "JSON body required", cid.as_str());
    };
    if body.names.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "names must be a non-empty array",
            cid.as_str(),
        );
    }
    if body.names.len() > MAX_BULK {
        return error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("bulk operations are limited to {} items", MAX_BULK),
            cid.as_str(),
        );
    }
    match store::toggle_many(&state.db, &body.names, body.enabled).await {
        Ok(affected) => {
            audit::append(
                state.db.clone(),
                "remote",
                "bulk",
                "bulk_toggle",
                cid.as_str(),
                Some(json!({ "names": body.names, "enabled": body.enabled })),
            );
            broadcast_remotes_changed(&state, "bulk_toggle").await;
            Json(json!({ "affected": affected, "enabled": body.enabled })).into_response()
        }
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn bulk_delete(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<BulkDeleteRequest>>,
) -> Response {
    let Some(Json(body)) = body else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_body", "JSON body required", cid.as_str());
    };
    if body.names.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "names must be a non-empty array",
            cid.as_str(),
        );
    }
    if body.names.len() > MAX_BULK {
        return error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("bulk operations are limited to {} items", MAX_BULK),
            cid.as_str(),
        );
    }
    match store::delete_many(&state.db, &body.names).await {
        Ok(affected) => {
            audit::append(
                state.db.clone(),
                "remote",
                "bulk",
                "bulk_delete",
                cid.as_str(),
                Some(json!({ "names": body.names })),
            );
            broadcast_remotes_changed(&state, "bulk_delete").await;
            Json(json!({ "affected": affected })).into_response()
        }
        Err(e) => server_error(e, cid.as_str()),
    }
}

async fn list_versions(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::get(&state.db, &name).await {
        Ok(None) => return error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("Remote \"{}\" not found", name),
            cid.as_str(),
        ),
        Err(e) => return server_error(e, cid.as_str()),
        Ok(Some(_)) => {}
    }
    match versions::list_for_remote(&state.db, &name).await {
        Ok(vers) => {
            let total = vers.len();
            Json(json!({ "remote": name, "versions": vers, "total": total })).into_response()
        }
        Err(e) => server_error(e, cid.as_str()),
    }
}

#[derive(serde::Deserialize)]
struct RollbackRequest {
    version: u32,
}

async fn rollback(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<RollbackRequest>>,
) -> Response {
    let Some(Json(body)) = body else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "JSON body required",
            cid.as_str(),
        );
    };
    match versions::restore(&state.db, &name, body.version).await {
        Ok(Some((remote, restored_version))) => {
            audit::append(
                state.db.clone(),
                "remote",
                &name,
                "rollback",
                cid.as_str(),
                Some(json!({ "toVersion": restored_version })),
            );
            versions::record(&state.db, &remote).await;
            let res = Json(remote).into_response();
            broadcast_remotes_changed(&state, format!("rollback:{}", name)).await;
            res
        }
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!(
                "Remote \"{}\" or version {} not found",
                name, body.version
            ),
            cid.as_str(),
        ),
        Err(e) => server_error(e, cid.as_str()),
    }
}

fn validate_new(name: &str, url: &str, route_path: &str, exposed_module: &str) -> Option<String> {
    if name.is_empty() || !is_valid_remote_name(name) {
        return Some("name must be camelCase starting with a lowercase letter".to_string());
    }
    if url.is_empty() || !is_valid_url_or_path(url) {
        return Some("url must be a valid http(s) URL or absolute path (starting with /)".to_string());
    }
    if route_path.is_empty() || !is_valid_route_path(route_path) {
        return Some("routePath must be kebab-case starting with a lowercase letter".to_string());
    }
    if !exposed_module.starts_with("./") {
        return Some("exposedModule must start with \"./\"".to_string());
    }
    None
}

fn server_error(err: StoreError, cid: &str) -> Response {
    error!("[remotes] [{}] store error: {}", cid, err);
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_server_error",
        err.to_string(),
        cid,
    )
}
