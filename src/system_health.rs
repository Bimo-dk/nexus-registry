use std::time::{Duration, Instant};

use futures_util::future::join_all;
use once_cell::sync::Lazy;
use reqwest::Client;
use serde::Serialize;
use tokio::time::interval;
use tracing::info;

use crate::state::AppState;
use crate::store;
use crate::time::iso_now;
use crate::types::{RemoteConfig, RemoteHealthStatus, UpdateRemoteRequest};
use crate::ws::broadcast_system_health;

static HTTP: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("reqwest client")
});

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceHealth {
    pub name: String,
    pub kind: ServiceKind,
    pub enabled: bool,
    pub status: RemoteHealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub last_checked: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceKind {
    Registry,
    System,
    Remote,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub total: u64,
    pub healthy: u64,
    pub degraded: u64,
    pub down: u64,
    pub unknown: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemHealthSnapshot {
    pub timestamp: String,
    pub services: Vec<ServiceHealth>,
    pub summary: Summary,
}

struct ProbeResult {
    ok: bool,
    latency_ms: u64,
    error: Option<String>,
}

async fn probe(url: &str) -> ProbeResult {
    let start = Instant::now();
    match HTTP.get(url).send().await {
        Ok(resp) => ProbeResult {
            ok: resp.status().is_success(),
            latency_ms: start.elapsed().as_millis() as u64,
            error: None,
        },
        Err(e) => ProbeResult {
            ok: false,
            latency_ms: start.elapsed().as_millis() as u64,
            error: Some(e.to_string()),
        },
    }
}

fn status_from(latency: &ProbeResult) -> RemoteHealthStatus {
    if !latency.ok {
        return RemoteHealthStatus::Down;
    }
    if latency.latency_ms > 1500 {
        return RemoteHealthStatus::Degraded;
    }
    RemoteHealthStatus::Healthy
}

fn camel_to_kebab(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut prev: Option<char> = None;
    for ch in name.chars() {
        if ch.is_ascii_uppercase() {
            if let Some(p) = prev {
                if p.is_ascii_lowercase() || p.is_ascii_digit() {
                    out.push('-');
                }
            }
            for low in ch.to_lowercase() {
                out.push(low);
            }
        } else {
            out.push(ch);
        }
        prev = Some(ch);
    }
    out
}

fn derive_internal_health_url(remote: &RemoteConfig) -> String {
    if let Some(up) = remote.upstream_url.as_deref() {
        let base = up.trim_end_matches('/');
        return format!("{}/health", base);
    }
    let kebab = camel_to_kebab(&remote.name);
    format!("http://{}/health", kebab)
}

pub async fn run_cycle(state: &AppState) -> SystemHealthSnapshot {
    let now = iso_now();

    let registry_health = ServiceHealth {
        name: "registry".to_string(),
        kind: ServiceKind::Registry,
        enabled: true,
        status: RemoteHealthStatus::Healthy,
        latency_ms: Some(0),
        last_checked: now.clone(),
        url: None,
        error: None,
    };

    let sys_futures = state.env.system_services.iter().map(|svc| {
        let now = now.clone();
        async move {
            let p = probe(&svc.health_url).await;
            ServiceHealth {
                name: svc.name.clone(),
                kind: ServiceKind::System,
                enabled: true,
                status: status_from(&p),
                latency_ms: Some(p.latency_ms),
                last_checked: now,
                url: Some(svc.health_url.clone()),
                error: p.error,
            }
        }
    });
    let sys_checks = join_all(sys_futures).await;

    let remotes = store::list(&state.db).await.unwrap_or_default();
    let mut enabled_count = 0usize;
    let mut disabled_count = 0usize;
    for r in &remotes {
        if r.enabled {
            enabled_count += 1;
        } else {
            disabled_count += 1;
        }
    }
    crate::features::metrics::set_remotes_count(enabled_count, disabled_count);

    let remote_futures = remotes.into_iter().map(|remote| {
        let now = now.clone();
        let db = state.db.clone();
        let circuit = state.circuit_breaker.clone();
        async move {
            if !remote.enabled {
                return ServiceHealth {
                    name: remote.name,
                    kind: ServiceKind::Remote,
                    enabled: false,
                    status: RemoteHealthStatus::Unknown,
                    latency_ms: None,
                    last_checked: now,
                    url: None,
                    error: None,
                };
            }
            // Skip probes for remotes whose circuit is open. Reuse the last known status.
            if !circuit.should_attempt(&remote.name) {
                let state_label = circuit.state_of(&remote.name);
                crate::features::metrics::record_circuit_state(&remote.name, state_label);
                return ServiceHealth {
                    name: remote.name.clone(),
                    kind: ServiceKind::Remote,
                    enabled: true,
                    status: remote.health_status.unwrap_or(RemoteHealthStatus::Down),
                    latency_ms: None,
                    last_checked: now,
                    url: None,
                    error: Some(format!("circuit {} — probe skipped", state_label)),
                };
            }

            let url = derive_internal_health_url(&remote);
            let p = probe(&url).await;
            let status = status_from(&p);
            if matches!(status, RemoteHealthStatus::Healthy) {
                circuit.record_success(&remote.name);
            } else {
                circuit.record_failure(&remote.name);
            }
            crate::features::metrics::record_health_check(&remote.name, p.latency_ms as f64, status.as_str());
            crate::features::metrics::record_circuit_state(&remote.name, circuit.state_of(&remote.name));

            let _ = store::update(
                &db,
                &remote.name,
                UpdateRemoteRequest {
                    health_status: Some(status),
                    last_health_check: Some(now.clone()),
                    ..Default::default()
                },
            )
            .await;
            ServiceHealth {
                name: remote.name,
                kind: ServiceKind::Remote,
                enabled: true,
                status,
                latency_ms: Some(p.latency_ms),
                last_checked: now,
                url: Some(url),
                error: p.error,
            }
        }
    });
    let remote_checks = join_all(remote_futures).await;

    let mut services = Vec::with_capacity(1 + sys_checks.len() + remote_checks.len());
    services.push(registry_health);
    services.extend(sys_checks);
    services.extend(remote_checks);

    let mut summary = Summary {
        total: 0,
        healthy: 0,
        degraded: 0,
        down: 0,
        unknown: 0,
    };
    for s in &services {
        summary.total += 1;
        match s.status {
            RemoteHealthStatus::Healthy => summary.healthy += 1,
            RemoteHealthStatus::Degraded => summary.degraded += 1,
            RemoteHealthStatus::Down => summary.down += 1,
            RemoteHealthStatus::Unknown => summary.unknown += 1,
        }
    }

    let snapshot = SystemHealthSnapshot {
        timestamp: now,
        services,
        summary,
    };
    *state.health_cache.write() = Some(snapshot.clone());
    if let Ok(v) = serde_json::to_value(&snapshot) {
        broadcast_system_health(state, v);
    }
    snapshot
}

pub fn start_loop(state: AppState) {
    let interval_ms = state.env.health_interval_ms;
    let system_names: Vec<String> = state.env.system_services.iter().map(|s| s.name.clone()).collect();

    tokio::spawn(async move {
        info!(
            "[system-health] Loop started — interval {}ms — system services: {}",
            interval_ms,
            system_names.join(", ")
        );
        let mut tick = interval(Duration::from_millis(interval_ms.max(1)));
        loop {
            tick.tick().await;
            run_cycle(&state).await;
        }
    });
}
