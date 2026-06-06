use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use serde_json::json;
use tracing::error;
use ulid::Ulid;

use crate::correlation::CorrelationId;
use crate::http_error::error_response;
use crate::state::AppState;
use crate::store::{self, DeleteHostOutcome, StoreError};
use crate::time::iso_now;
use crate::types::{CreateHostRequest, Host, UpdateHostRequest};
use crate::validators::{is_valid_entity_name, is_valid_framework, is_valid_host_url, is_valid_remote_entry};
use crate::ws::{broadcast_host_changed, broadcast_remotes_changed};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list).post(create))
        .route("/{id}", get(detail).put(update).delete(remove))
        .route("/{id}/remotes", get(list_remotes_for_host))
        .route("/{id}/toggle", post(toggle))
}

async fn list(State(state): State<AppState>, Extension(cid): Extension<CorrelationId>) -> Response {
    match store::list_hosts(&state.db).await {
        Ok(hosts) => Json(hosts).into_response(),
        Err(e) => server_error(e, &cid),
    }
}

async fn detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::get_host(&state.db, &id).await {
        Ok(Some(host)) => Json(host).into_response(),
        Ok(None) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

async fn list_remotes_for_host(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    let host = match store::get_host(&state.db, &id).await {
        Ok(Some(h)) => h,
        Ok(None) => return not_found(&id, &cid),
        Err(e) => return server_error(e, &cid),
    };
    let host_visibility = format!("host:{}", host.id);
    match store::list_for_host(&state.db, &host.id).await {
        Ok(remotes) => {
            let with_source: Vec<_> = remotes
                .into_iter()
                .map(|r| {
                    let source = if r.visibility == "global" {
                        "global"
                    } else {
                        "host-specific"
                    };
                    let mut value = serde_json::to_value(&r).unwrap_or_default();
                    if let Some(obj) = value.as_object_mut() {
                        obj.insert("source".to_string(), serde_json::Value::String(source.into()));
                    }
                    value
                })
                .collect();
            // Touch `host_visibility` so the binding is observed by the compiler — used as
            // the source label boundary above.
            let _ = host_visibility;
            Json(json!({
                "hostId": host.id,
                "remotes": with_source,
                "total": with_source.len(),
            }))
            .into_response()
        }
        Err(e) => server_error(e, &cid),
    }
}

async fn create(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<CreateHostRequest>>,
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
    let framework = body.framework.unwrap_or_default();
    let remote_entry = body.remote_entry.unwrap_or_default();
    let exposed_module = body.exposed_module.unwrap_or_default();
    let enabled = body.enabled.unwrap_or(true);

    if let Some(msg) = validate_host(&name, &url, &framework, &remote_entry, &exposed_module) {
        return error_response(StatusCode::BAD_REQUEST, "validation_failed", msg, cid.as_str());
    }

    let now = iso_now();
    let host = Host {
        id: Ulid::new().to_string(),
        name,
        url,
        framework,
        remote_entry,
        exposed_module,
        enabled,
        created_at: now.clone(),
        updated_at: now,
    };

    match store::insert_host(&state.db, &host).await {
        Ok(()) => {
            let res = (StatusCode::CREATED, Json(host.clone())).into_response();
            broadcast_host_changed(&state, host, "created");
            res
        }
        Err(StoreError::Conflict(name)) => error_response(
            StatusCode::CONFLICT,
            "conflict",
            format!("host \"{}\" already exists", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, &cid),
    }
}

async fn update(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<UpdateHostRequest>>,
) -> Response {
    let Some(Json(body)) = body else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_body",
            "JSON body required",
            cid.as_str(),
        );
    };

    if let Some(ref n) = body.name {
        if !is_valid_entity_name(n) {
            return validation_err("name must match [a-zA-Z][a-zA-Z0-9]*", &cid);
        }
    }
    if let Some(ref u) = body.url {
        if !is_valid_host_url(u) {
            return validation_err("url must be a valid http(s) URL with no trailing slash", &cid);
        }
    }
    if let Some(ref f) = body.framework {
        if !is_valid_framework(f) {
            return validation_err("framework must be one of: angular, vue, react", &cid);
        }
    }
    if let Some(ref re) = body.remote_entry {
        if !is_valid_remote_entry(re) {
            return validation_err("remoteEntry must start with / or be a full https URL", &cid);
        }
    }
    if let Some(ref em) = body.exposed_module {
        if !em.starts_with("./") {
            return validation_err("exposedModule must start with \"./\"", &cid);
        }
    }

    let now = iso_now();
    match store::update_host(&state.db, &id, &body, &now).await {
        Ok(Some(host)) => {
            let res = Json(host.clone()).into_response();
            broadcast_host_changed(&state, host, "updated");
            res
        }
        Ok(None) => not_found(&id, &cid),
        Err(StoreError::Conflict(name)) => error_response(
            StatusCode::CONFLICT,
            "conflict",
            format!("host name \"{}\" already exists", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, &cid),
    }
}

async fn remove(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::delete_host(&state.db, &id).await {
        Ok(DeleteHostOutcome::Deleted(host)) => {
            broadcast_host_changed(&state, host, "deleted");
            // Also tell hosts that any host-specific remote rows might have moved (no-op
            // delete cascading here, but `remotes_changed` is the standard cache-bust signal).
            broadcast_remotes_changed(&state, "host_deleted").await;
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(DeleteHostOutcome::Blocked(gates)) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "conflict",
                "message": format!("host has {} gate(s) referencing it", gates.len()),
                "correlationId": cid.as_str(),
                "blockingGates": gates,
            })),
        )
            .into_response(),
        Ok(DeleteHostOutcome::NotFound) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

async fn toggle(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    let now = iso_now();
    match store::toggle_host(&state.db, &id, &now).await {
        Ok(Some(host)) => {
            let res = Json(host.clone()).into_response();
            broadcast_host_changed(&state, host, "toggle");
            res
        }
        Ok(None) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

fn validate_host(
    name: &str,
    url: &str,
    framework: &str,
    remote_entry: &str,
    exposed_module: &str,
) -> Option<String> {
    if !is_valid_entity_name(name) {
        return Some("name must match [a-zA-Z][a-zA-Z0-9]*".into());
    }
    if !is_valid_host_url(url) {
        return Some("url must be a valid http(s) URL with no trailing slash".into());
    }
    if !is_valid_framework(framework) {
        return Some("framework must be one of: angular, vue, react".into());
    }
    if !is_valid_remote_entry(remote_entry) {
        return Some("remoteEntry must start with / or be a full https URL".into());
    }
    if !exposed_module.starts_with("./") {
        return Some("exposedModule must start with \"./\"".into());
    }
    None
}

fn not_found(id: &str, cid: &CorrelationId) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        format!("host \"{}\" not found", id),
        cid.as_str(),
    )
}

fn validation_err(message: &str, cid: &CorrelationId) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        "validation_failed",
        message.to_string(),
        cid.as_str(),
    )
}

fn server_error(err: StoreError, cid: &CorrelationId) -> Response {
    error!("[hosts] [{}] store error: {}", cid.as_str(), err);
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_server_error",
        err.to_string(),
        cid.as_str(),
    )
}
