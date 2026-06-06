use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::defaults;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitingConfig {
    pub enabled: bool,
    pub requests_per_second: u32,
    pub burst_size: u32,
    pub by: String,
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::rate_limiting::ENABLED,
            requests_per_second: defaults::rate_limiting::REQUESTS_PER_SECOND,
            burst_size: defaults::rate_limiting::BURST_SIZE,
            by: defaults::rate_limiting::BY.to_string(),
        }
    }
}

impl RateLimitingConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=1000).contains(&self.requests_per_second) {
            return Err("requestsPerSecond must be between 1 and 1000".into());
        }
        if !(1..=500).contains(&self.burst_size) {
            return Err("burstSize must be between 1 and 500".into());
        }
        if self.burst_size < self.requests_per_second {
            return Err("burstSize must be greater than or equal to requestsPerSecond".into());
        }
        if self.by != "ip" && self.by != "token" {
            return Err("by must be either \"ip\" or \"token\"".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsReconnectConfig {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
    pub jitter_ms: u64,
    pub max_attempts: u32,
}

impl Default for WsReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: defaults::ws_reconnect::INITIAL_DELAY_MS,
            max_delay_ms: defaults::ws_reconnect::MAX_DELAY_MS,
            backoff_multiplier: defaults::ws_reconnect::BACKOFF_MULTIPLIER,
            jitter_ms: defaults::ws_reconnect::JITTER_MS,
            max_attempts: defaults::ws_reconnect::MAX_ATTEMPTS,
        }
    }
}

impl WsReconnectConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(100..=10_000).contains(&self.initial_delay_ms) {
            return Err("initialDelayMs must be between 100 and 10000".into());
        }
        if !(1_000..=300_000).contains(&self.max_delay_ms) {
            return Err("maxDelayMs must be between 1000 and 300000".into());
        }
        if self.max_delay_ms < self.initial_delay_ms {
            return Err("maxDelayMs must be greater than or equal to initialDelayMs".into());
        }
        if !(1.0..=10.0).contains(&self.backoff_multiplier) {
            return Err("backoffMultiplier must be between 1.0 and 10.0".into());
        }
        if self.jitter_ms > 5_000 {
            return Err("jitterMs must be between 0 and 5000".into());
        }
        if self.max_attempts > 1_000 {
            return Err("maxAttempts must be between 0 and 1000".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub open_duration_ms: u64,
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::circuit_breaker::ENABLED,
            failure_threshold: defaults::circuit_breaker::FAILURE_THRESHOLD,
            success_threshold: defaults::circuit_breaker::SUCCESS_THRESHOLD,
            open_duration_ms: defaults::circuit_breaker::OPEN_DURATION_MS,
            half_open_max_calls: defaults::circuit_breaker::HALF_OPEN_MAX_CALLS,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=20).contains(&self.failure_threshold) {
            return Err("failureThreshold must be between 1 and 20".into());
        }
        if !(1..=10).contains(&self.success_threshold) {
            return Err("successThreshold must be between 1 and 10".into());
        }
        if !(1_000..=3_600_000).contains(&self.open_duration_ms) {
            return Err("openDurationMs must be between 1000 and 3600000".into());
        }
        if !(1..=5).contains(&self.half_open_max_calls) {
            return Err("halfOpenMaxCalls must be between 1 and 5".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GracefulShutdownConfig {
    pub timeout_ms: u64,
    pub ws_notice_ms: u64,
}

impl Default for GracefulShutdownConfig {
    fn default() -> Self {
        Self {
            timeout_ms: defaults::graceful_shutdown::TIMEOUT_MS,
            ws_notice_ms: defaults::graceful_shutdown::WS_NOTICE_MS,
        }
    }
}

impl GracefulShutdownConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(1_000..=60_000).contains(&self.timeout_ms) {
            return Err("timeoutMs must be between 1000 and 60000".into());
        }
        if !(500..=10_000).contains(&self.ws_notice_ms) {
            return Err("wsNoticeMs must be between 500 and 10000".into());
        }
        if self.ws_notice_ms >= self.timeout_ms {
            return Err("wsNoticeMs must be less than timeoutMs".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    pub prometheus_enabled: bool,
    pub prometheus_path: String,
    pub require_auth: bool,
    pub custom_labels: BTreeMap<String, String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prometheus_enabled: defaults::metrics::PROMETHEUS_ENABLED,
            prometheus_path: defaults::metrics::PROMETHEUS_PATH.to_string(),
            require_auth: defaults::metrics::REQUIRE_AUTH,
            custom_labels: BTreeMap::new(),
        }
    }
}

impl MetricsConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.prometheus_path.starts_with('/') {
            return Err("prometheusPath must start with /".into());
        }
        if self.prometheus_path.contains(' ') {
            return Err("prometheusPath must not contain spaces".into());
        }
        if self.prometheus_path.len() > 64 {
            return Err("prometheusPath max length is 64".into());
        }
        if self.custom_labels.len() > 10 {
            return Err("customLabels max 10 entries".into());
        }
        for k in self.custom_labels.keys() {
            if !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(format!(
                    "customLabels key \"{}\" must be alphanumeric or underscore only",
                    k
                ));
            }
        }
        Ok(())
    }
}

// ---- Gateway protection config ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayProtectionConfig {
    pub rate_limit_enabled: bool,
    pub rate_limit_requests_per_second: u32,
    pub rate_limit_burst: u32,
    pub rate_limit_by: String,
    pub max_connections_per_ip: u32,
    pub max_websocket_connections_per_ip: u32,
    pub request_timeout_ms: u64,
    pub header_read_timeout_ms: u64,
    pub body_read_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_body_bytes: u64,
    pub max_header_bytes: u64,
    pub slowloris_timeout_ms: u64,
    pub ban_duration_seconds: u64,
    pub ban_threshold_violations: u32,
}

impl Default for GatewayProtectionConfig {
    fn default() -> Self {
        use defaults::gateway_protection as d;
        Self {
            rate_limit_enabled: d::RATE_LIMIT_ENABLED,
            rate_limit_requests_per_second: d::RATE_LIMIT_RPS,
            rate_limit_burst: d::RATE_LIMIT_BURST,
            rate_limit_by: d::RATE_LIMIT_BY.to_string(),
            max_connections_per_ip: d::MAX_CONNECTIONS_PER_IP,
            max_websocket_connections_per_ip: d::MAX_WS_CONNECTIONS_PER_IP,
            request_timeout_ms: d::REQUEST_TIMEOUT_MS,
            header_read_timeout_ms: d::HEADER_READ_TIMEOUT_MS,
            body_read_timeout_ms: d::BODY_READ_TIMEOUT_MS,
            idle_timeout_ms: d::IDLE_TIMEOUT_MS,
            max_body_bytes: d::MAX_BODY_BYTES,
            max_header_bytes: d::MAX_HEADER_BYTES,
            slowloris_timeout_ms: d::SLOWLORIS_TIMEOUT_MS,
            ban_duration_seconds: d::BAN_DURATION_SECONDS,
            ban_threshold_violations: d::BAN_THRESHOLD_VIOLATIONS,
        }
    }
}

impl GatewayProtectionConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=10_000).contains(&self.rate_limit_requests_per_second) {
            return Err("rateLimitRequestsPerSecond must be between 1 and 10000".into());
        }
        if self.rate_limit_burst < self.rate_limit_requests_per_second {
            return Err("rateLimitBurst must be >= rateLimitRequestsPerSecond".into());
        }
        if self.rate_limit_by != "ip" && self.rate_limit_by != "token" {
            return Err("rateLimitBy must be \"ip\" or \"token\"".into());
        }
        if !(1..=1_000).contains(&self.max_connections_per_ip) {
            return Err("maxConnectionsPerIp must be between 1 and 1000".into());
        }
        if !(1..=100).contains(&self.max_websocket_connections_per_ip) {
            return Err("maxWebsocketConnectionsPerIp must be between 1 and 100".into());
        }
        for (name, val) in [
            ("requestTimeoutMs", self.request_timeout_ms),
            ("headerReadTimeoutMs", self.header_read_timeout_ms),
            ("bodyReadTimeoutMs", self.body_read_timeout_ms),
            ("idleTimeoutMs", self.idle_timeout_ms),
            ("slowlorisTimeoutMs", self.slowloris_timeout_ms),
        ] {
            if !(100..=300_000).contains(&val) {
                return Err(format!("{} must be between 100 and 300000", name));
            }
        }
        if !(1_024..=104_857_600).contains(&self.max_body_bytes) {
            return Err("maxBodyBytes must be between 1024 and 104857600".into());
        }
        if !(1_024..=65_536).contains(&self.max_header_bytes) {
            return Err("maxHeaderBytes must be between 1024 and 65536".into());
        }
        if !(60..=86_400).contains(&self.ban_duration_seconds) {
            return Err("banDurationSeconds must be between 60 and 86400".into());
        }
        if !(1..=100).contains(&self.ban_threshold_violations) {
            return Err("banThresholdViolations must be between 1 and 100".into());
        }
        Ok(())
    }
}

// Token-rotation storage. Hashes only — never plaintext.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRotationStored {
    pub active_token_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_token_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_token_expires_at: Option<String>,
}

// Aggregate returned by GET /api/config.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllConfig {
    pub rate_limiting: RateLimitingConfig,
    pub ws_reconnect: WsReconnectConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub graceful_shutdown: GracefulShutdownConfig,
    pub metrics: MetricsConfig,
    pub token: TokenRotationStored,
}

// Partial update for PUT /api/config.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PartialConfig {
    pub rate_limiting: Option<RateLimitingConfig>,
    pub ws_reconnect: Option<WsReconnectConfig>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    pub graceful_shutdown: Option<GracefulShutdownConfig>,
    pub metrics: Option<MetricsConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRotateRequest {
    pub new_token: String,
    pub previous_token_ttl_seconds: u64,
}

impl TokenRotateRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.new_token.len() < 16 {
            return Err("newToken must be at least 16 characters".into());
        }
        if !(300..=86_400).contains(&self.previous_token_ttl_seconds) {
            return Err("previousTokenTtlSeconds must be between 300 and 86400".into());
        }
        Ok(())
    }
}
