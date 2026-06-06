use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use serde::Serialize;

use crate::config::store::ConfigStore;

#[derive(Debug, Clone)]
pub enum CircuitState {
    Closed { failure_count: u32 },
    Open { since: Instant, remote_name: String },
    HalfOpen { call_count: u32 },
}

pub struct CircuitBreakerRegistry {
    config: Arc<ConfigStore>,
    state: RwLock<HashMap<String, CircuitState>>,
}

impl CircuitBreakerRegistry {
    pub fn new(config: Arc<ConfigStore>) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: RwLock::new(HashMap::new()),
        })
    }

    /// Returns true if a fresh attempt is allowed against the named remote.
    /// Drives Open→HalfOpen transition when open_duration_ms has elapsed.
    pub fn should_attempt(&self, name: &str) -> bool {
        let cfg = self.config.circuit_breaker();
        if !cfg.enabled {
            return true;
        }
        let mut guard = self.state.write();
        let entry = guard
            .entry(name.to_string())
            .or_insert(CircuitState::Closed { failure_count: 0 });
        match entry {
            CircuitState::Closed { .. } => true,
            CircuitState::Open { since, .. } => {
                if since.elapsed().as_millis() as u64 >= cfg.open_duration_ms {
                    *entry = CircuitState::HalfOpen { call_count: 0 };
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen { .. } => true,
        }
    }

    pub fn record_success(&self, name: &str) {
        let cfg = self.config.circuit_breaker();
        let mut guard = self.state.write();
        let entry = guard
            .entry(name.to_string())
            .or_insert(CircuitState::Closed { failure_count: 0 });
        match entry {
            CircuitState::Closed { failure_count } => *failure_count = 0,
            CircuitState::HalfOpen { call_count } => {
                *call_count += 1;
                if *call_count >= cfg.success_threshold {
                    *entry = CircuitState::Closed { failure_count: 0 };
                }
            }
            CircuitState::Open { .. } => {}
        }
    }

    pub fn record_failure(&self, name: &str) {
        let cfg = self.config.circuit_breaker();
        let mut guard = self.state.write();
        let entry = guard
            .entry(name.to_string())
            .or_insert(CircuitState::Closed { failure_count: 0 });
        match entry {
            CircuitState::Closed { failure_count } => {
                *failure_count += 1;
                if *failure_count >= cfg.failure_threshold {
                    *entry = CircuitState::Open {
                        since: Instant::now(),
                        remote_name: name.to_string(),
                    };
                }
            }
            CircuitState::HalfOpen { .. } => {
                *entry = CircuitState::Open {
                    since: Instant::now(),
                    remote_name: name.to_string(),
                };
            }
            CircuitState::Open { .. } => {}
        }
    }

    pub fn snapshot(&self) -> HashMap<String, CircuitStateSnapshot> {
        let guard = self.state.read();
        guard
            .iter()
            .map(|(name, st)| (name.clone(), CircuitStateSnapshot::from(st)))
            .collect()
    }

    pub fn state_of(&self, name: &str) -> &'static str {
        match self.state.read().get(name) {
            Some(CircuitState::Closed { .. }) | None => "closed",
            Some(CircuitState::Open { .. }) => "open",
            Some(CircuitState::HalfOpen { .. }) => "half_open",
        }
    }

    pub fn reset(&self, name: &str) {
        self.state
            .write()
            .insert(name.to_string(), CircuitState::Closed { failure_count: 0 });
    }

    pub fn reset_all(&self) {
        let mut guard = self.state.write();
        for v in guard.values_mut() {
            *v = CircuitState::Closed { failure_count: 0 };
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CircuitStateSnapshot {
    Closed { failure_count: u32 },
    Open { since_ms_ago: u64, remote_name: String },
    HalfOpen { call_count: u32 },
}

impl From<&CircuitState> for CircuitStateSnapshot {
    fn from(s: &CircuitState) -> Self {
        match s {
            CircuitState::Closed { failure_count } => Self::Closed {
                failure_count: *failure_count,
            },
            CircuitState::Open { since, remote_name } => Self::Open {
                since_ms_ago: since.elapsed().as_millis() as u64,
                remote_name: remote_name.clone(),
            },
            CircuitState::HalfOpen { call_count } => Self::HalfOpen {
                call_count: *call_count,
            },
        }
    }
}
