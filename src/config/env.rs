use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SystemService {
    pub name: String,
    pub health_url: String,
}

#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub bind_address: String,
    pub port: u16,
    pub node_env: String,
    pub nexus_token: String,
    pub nexus_token_pepper: String,
    pub allowed_origins: Vec<String>,
    pub system_services: Vec<SystemService>,
    pub health_interval_ms: u64,
    pub log_buffer_capacity: usize,
    pub data_dir: PathBuf,
    // Database — either DATABASE_URL OR the DB_* split vars. See
    // `config::database::DatabaseConfig::resolve` for the precedence rules.
    pub database_url: String,
    pub db_driver: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub db_ssl: String,
}

impl EnvConfig {
    pub fn from_env() -> Self {
        let bind_address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8670u16);
        let node_env = env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());
        let nexus_token = env::var("NEXUS_TOKEN").unwrap_or_default();
        let nexus_token_pepper =
            env::var("NEXUS_TOKEN_PEPPER").unwrap_or_else(|_| "nexus-registry-default-pepper".to_string());
        let allowed_origins = env::var("ALLOWED_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let system_services =
            parse_system_services(&env::var("SYSTEM_SERVICES").unwrap_or_else(|_| {
                "gateway=http://gateway:8668/health,host=http://host/health".to_string()
            }));
        let health_interval_ms = env::var("HEALTH_CHECK_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30_000);
        let log_buffer_capacity = env::var("LOG_BUFFER_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data"));
        // DATABASE_URL stays empty by default — the database module resolves
        // it (or assembles one from DB_* split vars) when init runs.
        let database_url = env::var("DATABASE_URL").unwrap_or_default();
        let db_driver = env::var("DB_DRIVER").unwrap_or_default();
        let db_host = env::var("DB_HOST").unwrap_or_default();
        let db_port = env::var("DB_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        let db_user = env::var("DB_USER").unwrap_or_default();
        let db_password = env::var("DB_PASSWORD").unwrap_or_default();
        let db_name = env::var("DB_NAME").unwrap_or_default();
        let db_ssl = env::var("DB_SSL").unwrap_or_default();

        Self {
            bind_address,
            port,
            node_env,
            nexus_token,
            nexus_token_pepper,
            allowed_origins,
            system_services,
            health_interval_ms,
            log_buffer_capacity,
            data_dir,
            database_url,
            db_driver,
            db_host,
            db_port,
            db_user,
            db_password,
            db_name,
            db_ssl,
        }
    }
}

fn parse_system_services(raw: &str) -> Vec<SystemService> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|entry| {
            let (name, url) = entry.split_once('=')?;
            let name = name.trim();
            let url = url.trim();
            if name.is_empty() || url.is_empty() {
                return None;
            }
            Some(SystemService {
                name: name.to_string(),
                health_url: url.to_string(),
            })
        })
        .collect()
}
