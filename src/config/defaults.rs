// Default values for every runtime-configurable section. Source of truth — if
// a section table is missing a row at startup, the defaults are inserted.

pub mod rate_limiting {
    pub const ENABLED: bool = true;
    pub const REQUESTS_PER_SECOND: u32 = 10;
    pub const BURST_SIZE: u32 = 20;
    pub const BY: &str = "ip";
}

pub mod ws_reconnect {
    pub const INITIAL_DELAY_MS: u64 = 1000;
    pub const MAX_DELAY_MS: u64 = 30000;
    pub const BACKOFF_MULTIPLIER: f64 = 2.0;
    pub const JITTER_MS: u64 = 1000;
    pub const MAX_ATTEMPTS: u32 = 0;
}

pub mod circuit_breaker {
    pub const ENABLED: bool = true;
    pub const FAILURE_THRESHOLD: u32 = 3;
    pub const SUCCESS_THRESHOLD: u32 = 1;
    pub const OPEN_DURATION_MS: u64 = 300_000;
    pub const HALF_OPEN_MAX_CALLS: u32 = 1;
}

pub mod graceful_shutdown {
    pub const TIMEOUT_MS: u64 = 10_000;
    pub const WS_NOTICE_MS: u64 = 3_000;
}

pub mod metrics {
    pub const PROMETHEUS_ENABLED: bool = true;
    pub const PROMETHEUS_PATH: &str = "/metrics";
    pub const REQUIRE_AUTH: bool = false;
}

pub mod gateway_protection {
    pub const RATE_LIMIT_ENABLED: bool = true;
    pub const RATE_LIMIT_RPS: u32 = 100;
    pub const RATE_LIMIT_BURST: u32 = 200;
    pub const RATE_LIMIT_BY: &str = "ip";
    pub const MAX_CONNECTIONS_PER_IP: u32 = 50;
    pub const MAX_WS_CONNECTIONS_PER_IP: u32 = 5;
    pub const REQUEST_TIMEOUT_MS: u64 = 30_000;
    pub const HEADER_READ_TIMEOUT_MS: u64 = 5_000;
    pub const BODY_READ_TIMEOUT_MS: u64 = 10_000;
    pub const IDLE_TIMEOUT_MS: u64 = 60_000;
    pub const MAX_BODY_BYTES: u64 = 1_048_576;
    pub const MAX_HEADER_BYTES: u64 = 8_192;
    pub const SLOWLORIS_TIMEOUT_MS: u64 = 10_000;
    pub const BAN_DURATION_SECONDS: u64 = 300;
    pub const BAN_THRESHOLD_VIOLATIONS: u32 = 10;
}
