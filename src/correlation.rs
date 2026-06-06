use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};

pub const CORRELATION_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone, Debug)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn generate_id() -> String {
    let buf: [u8; 6] = rand::random();
    format!("reg-{}", hex::encode(buf))
}

pub async fn middleware(mut req: Request, next: Next) -> Response {
    let incoming = req
        .headers()
        .get(&CORRELATION_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let id = incoming.unwrap_or_else(generate_id);
    req.extensions_mut().insert(CorrelationId(id.clone()));

    let mut res = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        res.headers_mut().insert(&CORRELATION_HEADER, value);
    }
    res
}
