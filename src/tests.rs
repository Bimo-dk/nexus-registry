#![cfg(test)]

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use parking_lot::RwLock;
use serde_json::{json, Value};
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::broadcast;
use tower::ServiceExt;
use tower_http::cors::CorsLayer;

use crate::config::env::EnvConfig;
use crate::config::store::ConfigStore;
use crate::features::circuit::CircuitBreakerRegistry;
use crate::features::rate_limit::RateLimitState;
use crate::features::shutdown::ShutdownController;
use crate::observability::log_buffer::LogBuffer;
use crate::observability::metrics::Metrics;
use crate::state::AppState;
use crate::ws::messages::ServerMessage;

const TOKEN: &str = "test-token";
const PEPPER: &str = "test-pepper";

async fn build_test_state() -> AppState {
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect("sqlite::memory:")
        .await
        .expect("in-memory sqlite");

    // Mirror the schema normally applied by store::init (idempotent).
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    let schema = [
        "CREATE TABLE IF NOT EXISTS remotes (
            name TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            exposed_module TEXT NOT NULL,
            route_path TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            added_at TEXT NOT NULL,
            upstream_url TEXT,
            health_status TEXT,
            last_health_check TEXT,
            visibility TEXT NOT NULL DEFAULT 'global'
        )",
        "CREATE TABLE IF NOT EXISTS hosts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
            framework TEXT NOT NULL,
            remote_entry TEXT NOT NULL,
            exposed_module TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS gates (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            domain TEXT NOT NULL UNIQUE,
            host_id TEXT NOT NULL REFERENCES hosts(id) ON DELETE RESTRICT,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )",
    ];
    for stmt in schema {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }

    let config_store = ConfigStore::hydrate(pool.clone()).await.expect("hydrate");
    crate::features::token::init_from_env(&config_store, TOKEN, PEPPER)
        .await
        .unwrap();
    crate::features::metrics::init();

    let env = EnvConfig {
        bind_address: "127.0.0.1".to_string(),
        port: 0,
        node_env: "test".to_string(),
        nexus_token: TOKEN.to_string(),
        nexus_token_pepper: PEPPER.to_string(),
        allowed_origins: vec![],
        system_services: vec![],
        health_interval_ms: 30_000,
        log_buffer_capacity: 100,
        data_dir: std::path::PathBuf::from("."),
        database_url: "sqlite::memory:".to_string(),
    };

    let circuit_breaker = CircuitBreakerRegistry::new(config_store.clone());
    let rate_limit = RateLimitState::new(&config_store.rate_limiting());
    let shutdown = ShutdownController::new();
    let (broadcast_tx, _) = broadcast::channel(64);

    AppState {
        db: pool,
        env: Arc::new(env),
        config_store,
        circuit_breaker,
        rate_limit,
        shutdown,
        metrics: Metrics::new(),
        log_buffer: LogBuffer::new(100),
        broadcast_tx,
        health_cache: Arc::new(RwLock::new(None)),
        started_at: Arc::new(Instant::now()),
    }
}

fn build_app(state: AppState) -> Router {
    crate::build_router(state, CorsLayer::permissive())
}

fn auth_get(path: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("x-nexus-token", TOKEN)
        .body(Body::empty())
        .unwrap()
}

fn auth_put(path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(Method::PUT)
        .uri(path)
        .header("x-nexus-token", TOKEN)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn auth_post(path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("x-nexus-token", TOKEN)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn json_body(res: axum::response::Response) -> Value {
    let bytes = to_bytes(res.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

// ---------- Defaults loaded at hydrate ----------

#[tokio::test]
async fn defaults_loaded_for_all_sections() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app.oneshot(auth_get("/api/config")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = json_body(res).await;

    assert_eq!(body["rateLimiting"]["requestsPerSecond"], 10);
    assert_eq!(body["rateLimiting"]["burstSize"], 20);
    assert_eq!(body["rateLimiting"]["by"], "ip");
    assert_eq!(body["wsReconnect"]["initialDelayMs"], 1000);
    assert_eq!(body["wsReconnect"]["maxDelayMs"], 30000);
    assert_eq!(body["circuitBreaker"]["failureThreshold"], 3);
    assert_eq!(body["gracefulShutdown"]["timeoutMs"], 10000);
    assert_eq!(body["metrics"]["prometheusPath"], "/metrics");
    assert!(body["token"]["activeTokenHash"].as_str().unwrap().len() == 64);
}

// ---------- Unauth ----------

#[tokio::test]
async fn unauthenticated_request_returns_401() {
    let state = build_test_state().await;
    let app = build_app(state);

    let req = Request::builder().uri("/api/config").body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

// ---------- Rate limiting ----------

#[tokio::test]
async fn rate_limiting_put_then_get_reflects_change() {
    let state = build_test_state().await;
    let mut rx = state.broadcast_tx.subscribe();
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_put(
            "/api/config/rate-limiting",
            json!({"enabled": true, "requestsPerSecond": 42, "burstSize": 100, "by": "token"}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let got = json_body(app.oneshot(auth_get("/api/config/rate-limiting")).await.unwrap()).await;
    assert_eq!(got["requestsPerSecond"], 42);
    assert_eq!(got["burstSize"], 100);
    assert_eq!(got["by"], "token");

    let msg = rx.recv().await.expect("broadcast received");
    match msg {
        ServerMessage::ConfigChanged { section, value, .. } => {
            assert_eq!(section, "rate_limiting");
            assert_eq!(value["requestsPerSecond"], 42);
        }
        other => panic!("expected ConfigChanged, got {:?}", other),
    }
}

#[tokio::test]
async fn rate_limiting_validation_rejects_bad_input() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_put(
            "/api/config/rate-limiting",
            json!({"enabled": true, "requestsPerSecond": 5000, "burstSize": 100, "by": "ip"}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = json_body(res).await;
    assert_eq!(body["error"], "validation_failed");
    assert!(body["message"].as_str().unwrap().contains("requestsPerSecond"));
}

// ---------- WS reconnect ----------

#[tokio::test]
async fn ws_reconnect_put_then_get() {
    let state = build_test_state().await;
    let mut rx = state.broadcast_tx.subscribe();
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_put(
            "/api/config/ws-reconnect",
            json!({
                "initialDelayMs": 500,
                "maxDelayMs": 60000,
                "backoffMultiplier": 1.5,
                "jitterMs": 250,
                "maxAttempts": 10
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let got = json_body(app.oneshot(auth_get("/api/config/ws-reconnect")).await.unwrap()).await;
    assert_eq!(got["initialDelayMs"], 500);
    assert_eq!(got["maxAttempts"], 10);

    // Two messages broadcast: ConfigChanged + ReconnectPolicyChanged
    let mut saw_config_changed = false;
    let mut saw_policy_changed = false;
    for _ in 0..2 {
        match rx.recv().await.unwrap() {
            ServerMessage::ConfigChanged { section, .. } if section == "ws_reconnect" => {
                saw_config_changed = true;
            }
            ServerMessage::ReconnectPolicyChanged { policy, .. } => {
                saw_policy_changed = true;
                assert_eq!(policy.max_attempts, 10);
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }
    assert!(saw_config_changed && saw_policy_changed);
}

#[tokio::test]
async fn ws_reconnect_validation_rejects_inverted_delays() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_put(
            "/api/config/ws-reconnect",
            json!({
                "initialDelayMs": 5000,
                "maxDelayMs": 1000,
                "backoffMultiplier": 2.0,
                "jitterMs": 0,
                "maxAttempts": 0
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ---------- Circuit breaker ----------

#[tokio::test]
async fn circuit_breaker_put_then_get_and_state() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_put(
            "/api/config/circuit-breaker",
            json!({
                "enabled": true,
                "failureThreshold": 5,
                "successThreshold": 2,
                "openDurationMs": 60000,
                "halfOpenMaxCalls": 1
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let got = json_body(
        app.clone()
            .oneshot(auth_get("/api/config/circuit-breaker"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(got["failureThreshold"], 5);

    // State endpoint returns empty object initially
    let state_res = app
        .oneshot(auth_get("/api/config/circuit-breaker/state"))
        .await
        .unwrap();
    assert_eq!(state_res.status(), StatusCode::OK);
    let state_body = json_body(state_res).await;
    assert!(state_body.is_object());
}

#[tokio::test]
async fn circuit_breaker_validation_rejects_bad_threshold() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_put(
            "/api/config/circuit-breaker",
            json!({
                "enabled": true,
                "failureThreshold": 999,
                "successThreshold": 1,
                "openDurationMs": 5000,
                "halfOpenMaxCalls": 1
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ---------- Graceful shutdown ----------

#[tokio::test]
async fn graceful_shutdown_put_then_get() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_put(
            "/api/config/graceful-shutdown",
            json!({"timeoutMs": 20000, "wsNoticeMs": 5000}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let got = json_body(
        app.oneshot(auth_get("/api/config/graceful-shutdown"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(got["timeoutMs"], 20000);
    assert_eq!(got["wsNoticeMs"], 5000);
}

#[tokio::test]
async fn graceful_shutdown_validation_rejects_notice_gte_timeout() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_put(
            "/api/config/graceful-shutdown",
            json!({"timeoutMs": 5000, "wsNoticeMs": 5000}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ---------- Metrics config ----------

#[tokio::test]
async fn metrics_put_changes_dynamic_path() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_put(
            "/api/config/metrics",
            json!({
                "prometheusEnabled": true,
                "prometheusPath": "/prom",
                "requireAuth": false,
                "customLabels": {"env": "test"}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Old path returns 404 now
    let res_old = app
        .clone()
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res_old.status(), StatusCode::NOT_FOUND);

    // New path serves Prometheus text
    let res_new = app
        .oneshot(Request::builder().uri("/prom").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res_new.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_validation_rejects_bad_path() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_put(
            "/api/config/metrics",
            json!({
                "prometheusEnabled": true,
                "prometheusPath": "no-leading-slash",
                "requireAuth": false,
                "customLabels": {}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ---------- Token rotation ----------

#[tokio::test]
async fn token_rotation_old_and_new_both_work_within_ttl() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_post(
            "/api/config/token/rotate",
            json!({"newToken": "fresh-token-1234567890", "previousTokenTtlSeconds": 600}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Old token (bootstrapped) still valid within TTL
    let res_old = app.clone().oneshot(auth_get("/api/config")).await.unwrap();
    assert_eq!(res_old.status(), StatusCode::OK);

    // New token works
    let req = Request::builder()
        .uri("/api/config")
        .header("x-nexus-token", "fresh-token-1234567890")
        .body(Body::empty())
        .unwrap();
    let res_new = app.oneshot(req).await.unwrap();
    assert_eq!(res_new.status(), StatusCode::OK);
}

#[tokio::test]
async fn token_rotation_rejects_short_token() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_post(
            "/api/config/token/rotate",
            json!({"newToken": "short", "previousTokenTtlSeconds": 600}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn delete_previous_token_clears_it_immediately() {
    let state = build_test_state().await;
    let app = build_app(state);

    // Rotate first so there IS a previous token
    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/config/token/rotate",
            json!({"newToken": "fresh-token-1234567890", "previousTokenTtlSeconds": 600}),
        ))
        .await
        .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/config/token/previous")
                .header("x-nexus-token", "fresh-token-1234567890")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // Old token no longer valid
    let res_old = app.oneshot(auth_get("/api/config")).await.unwrap();
    assert_eq!(res_old.status(), StatusCode::UNAUTHORIZED);
}

// ---------- Unified PUT/GET ----------

// ---------- Hosts + Gates + visibility ----------

async fn create_host(app: Router, name: &str, framework: &str) -> Value {
    let res = app
        .oneshot(auth_post(
            "/api/hosts",
            json!({
                "name": name,
                "url": "http://host.local:80",
                "framework": framework,
                "remoteEntry": "/host/remoteEntry.json",
                "exposedModule": "./AppShell"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    json_body(res).await
}

#[tokio::test]
async fn host_create_returns_gate_count_zero() {
    let state = build_test_state().await;
    let app = build_app(state);

    let host = create_host(app.clone(), "shellA", "angular").await;
    let id = host["id"].as_str().unwrap().to_string();
    assert_eq!(host["name"], "shellA");
    assert_eq!(host["framework"], "angular");

    let listed = json_body(app.oneshot(auth_get("/api/hosts")).await.unwrap()).await;
    let arr = listed.as_array().unwrap();
    let found = arr.iter().find(|h| h["id"] == id).unwrap();
    assert_eq!(found["gateCount"], 0);
}

#[tokio::test]
async fn host_delete_blocked_by_referencing_gate() {
    let state = build_test_state().await;
    let app = build_app(state);

    let host = create_host(app.clone(), "shellB", "react").await;
    let host_id = host["id"].as_str().unwrap().to_string();

    // Create gate referencing this host
    let gate_res = app
        .clone()
        .oneshot(auth_post(
            "/api/gates",
            json!({"name": "gateOne", "domain": "shop.example.com", "hostId": host_id.clone()}),
        ))
        .await
        .unwrap();
    assert_eq!(gate_res.status(), StatusCode::CREATED);

    // Try to delete host → 409 with blockingGates
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/api/hosts/{}", host_id))
        .header("x-nexus-token", TOKEN)
        .body(Body::empty())
        .unwrap();
    let del = app.oneshot(req).await.unwrap();
    assert_eq!(del.status(), StatusCode::CONFLICT);
    let body = json_body(del).await;
    let blocking = body["blockingGates"].as_array().unwrap();
    assert_eq!(blocking.len(), 1);
    assert_eq!(blocking[0], "gateOne");
}

#[tokio::test]
async fn host_remotes_endpoint_filters_global_and_host_specific() {
    let state = build_test_state().await;
    let app = build_app(state);

    let host_a = create_host(app.clone(), "shellA", "angular").await;
    let host_a_id = host_a["id"].as_str().unwrap().to_string();
    let host_b = create_host(app.clone(), "shellB", "angular").await;
    let host_b_id = host_b["id"].as_str().unwrap().to_string();

    // Global remote — visible to everyone
    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/remotes",
            json!({"name": "globalOne", "url": "/x", "routePath": "global-one"}),
        ))
        .await
        .unwrap();
    // Host-A-only
    let res_a = app
        .clone()
        .oneshot(auth_post(
            "/api/remotes",
            json!({
                "name": "aOnly", "url": "/x", "routePath": "a-only",
                "visibility": format!("host:{}", host_a_id)
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res_a.status(), StatusCode::CREATED);
    // Host-B-only
    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/remotes",
            json!({
                "name": "bOnly", "url": "/x", "routePath": "b-only",
                "visibility": format!("host:{}", host_b_id)
            }),
        ))
        .await
        .unwrap();

    // /api/hosts/<id>/remotes for A should return globalOne + aOnly, NOT bOnly
    let res = app
        .oneshot(auth_get(&format!("/api/hosts/{}/remotes", host_a_id)))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = json_body(res).await;
    let names: Vec<String> = body["remotes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"globalOne".to_string()));
    assert!(names.contains(&"aOnly".to_string()));
    assert!(!names.contains(&"bOnly".to_string()));
}

#[tokio::test]
async fn remote_post_with_nonexistent_host_id_returns_400() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .oneshot(auth_post(
            "/api/remotes",
            json!({
                "name": "ghost", "url": "/x", "routePath": "ghost",
                "visibility": "host:01H000000000000000000NOPE"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = json_body(res).await;
    assert!(body["message"].as_str().unwrap().contains("does not exist"));
}

#[tokio::test]
async fn remotes_get_filters_by_host_id_query_param() {
    let state = build_test_state().await;
    let app = build_app(state);

    let host = create_host(app.clone(), "shellQ", "vue").await;
    let host_id = host["id"].as_str().unwrap().to_string();

    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/remotes",
            json!({"name": "globalQ", "url": "/x", "routePath": "global-q"}),
        ))
        .await
        .unwrap();
    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/remotes",
            json!({
                "name": "qOnly", "url": "/x", "routePath": "q-only",
                "visibility": format!("host:{}", host_id)
            }),
        ))
        .await
        .unwrap();
    // Also a host-X-only remote (created against a different host)
    let host_x = create_host(app.clone(), "shellX", "react").await;
    let host_x_id = host_x["id"].as_str().unwrap().to_string();
    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/remotes",
            json!({
                "name": "xOnly", "url": "/x", "routePath": "x-only",
                "visibility": format!("host:{}", host_x_id)
            }),
        ))
        .await
        .unwrap();

    let res = app
        .oneshot(auth_get(&format!("/api/remotes?host_id={}", host_id)))
        .await
        .unwrap();
    let body = json_body(res).await;
    let names: Vec<String> = body["remotes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"globalQ".to_string()));
    assert!(names.contains(&"qOnly".to_string()));
    assert!(!names.contains(&"xOnly".to_string()));
}

#[tokio::test]
async fn gate_by_domain_returns_embedded_host() {
    let state = build_test_state().await;
    let app = build_app(state);

    let host = create_host(app.clone(), "shellD", "angular").await;
    let host_id = host["id"].as_str().unwrap().to_string();
    let _ = app
        .clone()
        .oneshot(auth_post(
            "/api/gates",
            json!({"name": "gateD", "domain": "domain-d.example.com", "hostId": host_id.clone()}),
        ))
        .await
        .unwrap();

    let res = app
        .oneshot(auth_get("/api/gates/by-domain/domain-d.example.com"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = json_body(res).await;
    assert_eq!(body["name"], "gateD");
    assert_eq!(body["domain"], "domain-d.example.com");
    assert_eq!(body["host"]["id"], host_id);
    assert_eq!(body["host"]["framework"], "angular");
}

#[tokio::test]
async fn gate_put_with_new_host_id_broadcasts_host_reassigned() {
    let state = build_test_state().await;
    let app = build_app(state.clone());

    let host_a = create_host(app.clone(), "shellRA", "angular").await;
    let host_b = create_host(app.clone(), "shellRB", "vue").await;
    let host_a_id = host_a["id"].as_str().unwrap().to_string();
    let host_b_id = host_b["id"].as_str().unwrap().to_string();

    let create = app
        .clone()
        .oneshot(auth_post(
            "/api/gates",
            json!({"name": "gateMove", "domain": "move.example.com", "hostId": host_a_id.clone()}),
        ))
        .await
        .unwrap();
    let created = json_body(create).await;
    let gate_id = created["id"].as_str().unwrap().to_string();

    // Subscribe AFTER setup so we only see the PUT broadcast.
    let mut rx = state.broadcast_tx.subscribe();

    let res = app
        .oneshot(auth_put(
            &format!("/api/gates/{}", gate_id),
            json!({"hostId": host_b_id.clone()}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let msg = rx.recv().await.unwrap();
    match msg {
        ServerMessage::GateChanged {
            trigger,
            old_host_id,
            new_host_id,
            ..
        } => {
            assert_eq!(trigger, "host_reassigned");
            assert_eq!(old_host_id.as_deref(), Some(host_a_id.as_str()));
            assert_eq!(new_host_id.as_deref(), Some(host_b_id.as_str()));
        }
        other => panic!("expected GateChanged, got {:?}", other),
    }
}

#[tokio::test]
async fn host_update_broadcasts_host_changed() {
    let state = build_test_state().await;
    let app = build_app(state.clone());

    let host = create_host(app.clone(), "shellU", "react").await;
    let host_id = host["id"].as_str().unwrap().to_string();

    // Subscribe AFTER setup so we only see the PUT broadcast.
    let mut rx = state.broadcast_tx.subscribe();

    let res = app
        .oneshot(auth_put(
            &format!("/api/hosts/{}", host_id),
            json!({"framework": "vue"}),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let msg = rx.recv().await.unwrap();
    match msg {
        ServerMessage::HostChanged { host, trigger, .. } => {
            assert_eq!(trigger, "updated");
            assert_eq!(host.framework, "vue");
        }
        other => panic!("expected HostChanged, got {:?}", other),
    }
}

// ---------- Original unified test ----------

#[tokio::test]
async fn unified_put_applies_only_provided_sections() {
    let state = build_test_state().await;
    let app = build_app(state);

    let res = app
        .clone()
        .oneshot(auth_put(
            "/api/config",
            json!({
                "rateLimiting": {"enabled": true, "requestsPerSecond": 99, "burstSize": 200, "by": "ip"}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let got = json_body(app.oneshot(auth_get("/api/config")).await.unwrap()).await;
    assert_eq!(got["rateLimiting"]["requestsPerSecond"], 99);
    // wsReconnect untouched
    assert_eq!(got["wsReconnect"]["initialDelayMs"], 1000);
}
