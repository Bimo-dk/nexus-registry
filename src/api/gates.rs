use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use tracing::error;
use ulid::Ulid;

use crate::correlation::CorrelationId;
use crate::http_error::error_response;
use crate::state::AppState;
use crate::store::{self, StoreError};
use crate::time::iso_now;
use crate::types::{CreateGateRequest, Gate, UpdateGateRequest};
use crate::validators::{is_valid_domain, is_valid_entity_name};
use crate::ws::broadcast_gate_changed;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list).post(create))
        .route("/by-domain/{domain}", get(by_domain))
        .route("/{id}", get(detail).put(update).delete(remove))
        .route("/{id}/toggle", post(toggle))
}

async fn list(State(state): State<AppState>, Extension(cid): Extension<CorrelationId>) -> Response {
    match store::list_gates(&state.db).await {
        Ok(gates) => Json(gates).into_response(),
        Err(e) => server_error(e, &cid),
    }
}

async fn detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::get_gate(&state.db, &id).await {
        Ok(Some(gate)) => Json(gate).into_response(),
        Ok(None) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

async fn by_domain(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    match store::get_gate_by_domain(&state.db, &domain).await {
        Ok(Some(gate)) => Json(gate).into_response(),
        Ok(None) => error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("no gate registered for domain \"{}\"", domain),
            cid.as_str(),
        ),
        Err(e) => server_error(e, &cid),
    }
}

async fn create(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<CreateGateRequest>>,
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
    let domain = body.domain.unwrap_or_default();
    let host_id = body.host_id;
    let enabled = body.enabled.unwrap_or(true);

    if !is_valid_entity_name(&name) {
        return validation_err("name must match [a-zA-Z][a-zA-Z0-9]*", &cid);
    }
    if !is_valid_domain(&domain) {
        return validation_err(
            "domain must be a valid hostname with optional port and no protocol prefix",
            &cid,
        );
    }
    if let Some(ref hid) = host_id {
        match store::host_exists(&state.db, hid).await {
            Ok(true) => {}
            Ok(false) => {
                return validation_err(
                    &format!("hostId \"{}\" does not reference an existing host", hid),
                    &cid,
                );
            }
            Err(e) => return server_error(e, &cid),
        }
    }

    let now = iso_now();
    let gate = Gate {
        id: Ulid::new().to_string(),
        name,
        domain,
        host_id,
        enabled,
        created_at: now.clone(),
        updated_at: now,
    };

    match store::insert_gate(&state.db, &gate).await {
        Ok(()) => {
            // Re-fetch with embedded host so the broadcast carries the full picture.
            if let Ok(Some(with_host)) = store::get_gate(&state.db, &gate.id).await {
                let res = (StatusCode::CREATED, Json(with_host.clone())).into_response();
                broadcast_gate_changed(&state, with_host, "created", None, None);
                res
            } else {
                (StatusCode::CREATED, Json(gate)).into_response()
            }
        }
        Err(StoreError::Conflict(name)) => error_response(
            StatusCode::CONFLICT,
            "conflict",
            format!("gate \"{}\" already exists", name),
            cid.as_str(),
        ),
        Err(e) => server_error(e, &cid),
    }
}

async fn update(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<UpdateGateRequest>>,
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
    if let Some(ref d) = body.domain {
        if !is_valid_domain(d) {
            return validation_err("domain must be a valid hostname with optional port", &cid);
        }
    }
    if let Some(ref h) = body.host_id {
        match store::host_exists(&state.db, h).await {
            Ok(true) => {}
            Ok(false) => {
                return validation_err(
                    &format!("hostId \"{}\" does not reference an existing host", h),
                    &cid,
                );
            }
            Err(e) => return server_error(e, &cid),
        }
    }

    let now = iso_now();
    match store::update_gate(&state.db, &id, &body, &now).await {
        Ok(Some((gate, host_changed))) => {
            // Re-fetch with embedded host so the broadcast reflects the new mapping.
            let with_host = match store::get_gate(&state.db, &gate.id).await {
                Ok(Some(g)) => g,
                _ => return server_error(StoreError::Conflict("missing after update".into()), &cid),
            };
            let (trigger, old_host_id, new_host_id) = match host_changed {
                Some(old) => ("host_reassigned".to_string(), old, gate.host_id.clone()),
                None => ("updated".to_string(), None, None),
            };
            let res = Json(with_host.clone()).into_response();
            broadcast_gate_changed(&state, with_host, trigger, old_host_id, new_host_id);
            res
        }
        Ok(None) => not_found(&id, &cid),
        Err(StoreError::Conflict(name)) => error_response(
            StatusCode::CONFLICT,
            "conflict",
            format!("gate name or domain \"{}\" already in use", name),
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
    // Capture full gate-with-host BEFORE delete so we can broadcast.
    let with_host = match store::get_gate(&state.db, &id).await {
        Ok(Some(g)) => Some(g),
        Ok(None) => None,
        Err(e) => return server_error(e, &cid),
    };
    match store::delete_gate(&state.db, &id).await {
        Ok(Some(_)) => {
            if let Some(g) = with_host {
                broadcast_gate_changed(&state, g, "deleted", None, None);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

async fn toggle(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    let now = iso_now();
    match store::toggle_gate(&state.db, &id, &now).await {
        Ok(Some(gate)) => {
            // Re-fetch with embedded host
            let with_host = match store::get_gate(&state.db, &gate.id).await {
                Ok(Some(g)) => g,
                _ => return server_error(StoreError::Conflict("missing after toggle".into()), &cid),
            };
            let res = Json(with_host.clone()).into_response();
            broadcast_gate_changed(&state, with_host, "toggle", None, None);
            res
        }
        Ok(None) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

fn not_found(id: &str, cid: &CorrelationId) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        format!("gate \"{}\" not found", id),
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
    error!("[gates] [{}] store error: {}", cid.as_str(), err);
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_server_error",
        err.to_string(),
        cid.as_str(),
    )
}
