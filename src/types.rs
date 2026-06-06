use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RemoteHealthStatus {
    Healthy,
    Degraded,
    Down,
    Unknown,
}

impl RemoteHealthStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Down => "down",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "healthy" => Some(Self::Healthy),
            "degraded" => Some(Self::Degraded),
            "down" => Some(Self::Down),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteConfig {
    pub name: String,
    pub url: String,
    pub exposed_module: String,
    pub route_path: String,
    pub enabled: bool,
    pub added_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_health_check: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_status: Option<RemoteHealthStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_url: Option<String>,
    /// `"global"` or `"host:<host_id>"`. Defaults to `"global"` for back-compat
    /// with clients that don't supply a value.
    #[serde(default = "default_visibility")]
    pub visibility: String,
}

fn default_visibility() -> String {
    "global".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddRemoteRequest {
    pub name: Option<String>,
    pub url: Option<String>,
    pub exposed_module: Option<String>,
    pub route_path: Option<String>,
    pub enabled: Option<bool>,
    pub upstream_url: Option<String>,
    pub visibility: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateRemoteRequest {
    pub url: Option<String>,
    pub exposed_module: Option<String>,
    pub route_path: Option<String>,
    pub enabled: Option<bool>,
    pub upstream_url: Option<String>,
    pub health_status: Option<RemoteHealthStatus>,
    pub last_health_check: Option<String>,
    pub visibility: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryResponse {
    pub remotes: Vec<RemoteConfig>,
    pub total: usize,
    pub enabled: usize,
}

// ---------- Hosts ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Host {
    pub id: String,
    pub name: String,
    pub url: String,
    pub framework: String,
    pub remote_entry: String,
    pub exposed_module: String,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostWithGateCount {
    #[serde(flatten)]
    pub host: Host,
    pub gate_count: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHostRequest {
    pub name: Option<String>,
    pub url: Option<String>,
    pub framework: Option<String>,
    pub remote_entry: Option<String>,
    pub exposed_module: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateHostRequest {
    pub name: Option<String>,
    pub url: Option<String>,
    pub framework: Option<String>,
    pub remote_entry: Option<String>,
    pub exposed_module: Option<String>,
    pub enabled: Option<bool>,
}

// ---------- Gates ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Gate {
    pub id: String,
    pub name: String,
    pub domain: String,
    pub host_id: Option<String>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GateWithHost {
    #[serde(flatten)]
    pub gate: Gate,
    pub host: Option<Host>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateGateRequest {
    pub name: Option<String>,
    pub domain: Option<String>,
    pub host_id: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateGateRequest {
    pub name: Option<String>,
    pub domain: Option<String>,
    pub host_id: Option<String>,
    pub enabled: Option<bool>,
}
