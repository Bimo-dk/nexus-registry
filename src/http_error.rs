use axum::{http::StatusCode, response::IntoResponse, response::Response, Json};
use serde_json::json;

pub fn error_response(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
    correlation_id: &str,
) -> Response {
    (
        status,
        Json(json!({
            "error": code,
            "message": message.into(),
            "correlationId": correlation_id,
        })),
    )
        .into_response()
}
