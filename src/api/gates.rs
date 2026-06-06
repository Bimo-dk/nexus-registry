use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use serde::Deserialize;
use tracing::error;
use ulid::Ulid;

use crate::correlation::CorrelationId;
use crate::http_error::error_response;
use crate::state::AppState;
use crate::store::{self, audit, ListPage, StoreError};
use crate::time::iso_now;
use crate::types::{BulkIdsToggleRequest, CreateGateRequest, Gate, GatesListResponse, UpdateGateRequest};
use crate::validators::{is_valid_domain, is_valid_entity_name};
use crate::ws::broadcast_gate_changed;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list).post(create))
        .route("/bulk-toggle", post(bulk_toggle))
        .route("/by-domain/{domain}", get(by_domain))
        .route("/{id}", get(detail).put(update).delete(remove))
        .route("/{id}/toggle", post(toggle))
}

const MAX_PAGE_SIZE: u32 = 200;
const MAX_BULK: usize = 100;

#[derive(Deserialize)]
struct ListQuery {
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

async fn list(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
    Extension(cid): Extension<CorrelationId>,
) -> Response {
    let lp = parse_page(q.page, q.limit);
    match store::list_gates(&state.db, lp.as_ref()).await {
        Ok((gates, total)) => {
            let (page, page_size, page_count) = match &lp {
                None => (None, None, None),
                Some(p) => {
                    let page_num = (p.offset / p.limit + 1) as u32;
                    let pc = if p.limit > 0 { ((total + p.limit - 1) / p.limit) as u32 } else { 0 };
                    (Some(page_num), Some(p.limit as u32), Some(pc))
                }
            };
            Json(GatesListResponse { gates, total: total as usize, page, page_size, page_count })
                .into_response()
        }
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
            audit::append(state.db.clone(), "gate", &gate.id, "created", cid.as_str(), None);
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
            let with_host = match store::get_gate(&state.db, &gate.id).await {
                Ok(Some(g)) => g,
                _ => return server_error(StoreError::Conflict("missing after update".into()), &cid),
            };
            let (trigger, old_host_id, new_host_id) = match host_changed {
                Some(old) => ("host_reassigned".to_string(), old, gate.host_id.clone()),
                None => ("updated".to_string(), None, None),
            };
            audit::append(state.db.clone(), "gate", &gate.id, "updated", cid.as_str(), None);
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
    let with_host = match store::get_gate(&state.db, &id).await {
        Ok(Some(g)) => Some(g),
        Ok(None) => None,
        Err(e) => return server_error(e, &cid),
    };
    match store::delete_gate(&state.db, &id).await {
        Ok(Some(_)) => {
            audit::append(state.db.clone(), "gate", &id, "deleted", cid.as_str(), None);
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
            let with_host = match store::get_gate(&state.db, &gate.id).await {
                Ok(Some(g)) => g,
                _ => return server_error(StoreError::Conflict("missing after toggle".into()), &cid),
            };
            audit::append(state.db.clone(), "gate", &gate.id, "toggled", cid.as_str(), None);
            let res = Json(with_host.clone()).into_response();
            broadcast_gate_changed(&state, with_host, "toggle", None, None);
            res
        }
        Ok(None) => not_found(&id, &cid),
        Err(e) => server_error(e, &cid),
    }
}

async fn bulk_toggle(
    State(state): State<AppState>,
    Extension(cid): Extension<CorrelationId>,
    body: Option<Json<BulkIdsToggleRequest>>,
) -> Response {
    let Some(Json(body)) = body else {
        return error_response(StatusCode::BAD_REQUEST, "invalid_body", "JSON body required", cid.as_str());
    };
    if body.ids.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "ids must be a non-empty array",
            cid.as_str(),
        );
    }
    if body.ids.len() > MAX_BULK {
        return error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("bulk operations are limited to {} items", MAX_BULK),
            cid.as_str(),
        );
    }
    let now = iso_now();
    match store::toggle_gates_many(&state.db, &body.ids, body.enabled, &now).await {
        Ok(affected) => {
            audit::append(
                state.db.clone(),
                "gate",
                "bulk",
                "bulk_toggle",
                cid.as_str(),
                Some(serde_json::json!({ "ids": body.ids, "enabled": body.enabled })),
            );
            for id in &body.ids {
                if let Ok(Some(g)) = store::get_gate(&state.db, id).await {
                    broadcast_gate_changed(&state, g, "bulk_toggle", None, None);
                }
            }
            use serde_json::json;
            Json(json!({ "affected": affected, "enabled": body.enabled })).into_response()
        }
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
