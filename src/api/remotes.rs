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
use crate::store::{self, StoreError};
use crate::time::iso_now;
use crate::types::{AddRemoteRequest, RegistryResponse, RemoteConfig, UpdateRemoteRequest};
use crate::validators::{is_valid_remote_name, is_valid_route_path, is_valid_url_or_path, parse_visibility};
use crate::ws::broadcast_remotes_changed;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list).post(create))
        .route("/{name}", get(detail).put(update).delete(remove))
        .route("/{name}/toggle", post(toggle))
        .route("/{name}/redeploy", post(redeploy))
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(rename = "host_id")]
    host_id: Option<String>,
}

async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let result = match q.host_id.as_deref() {
        Some(host_id) => store::list_for_host(&state.db, host_id).await,
        None => store::list(&state.db).await,
    };
    match result {
        Ok(remotes) => {
            let enabled = remotes.iter().filter(|r| r.enabled).count();
            let total = remotes.len();
            Json(RegistryResponse {
                remotes,
                total,
                enabled,
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
