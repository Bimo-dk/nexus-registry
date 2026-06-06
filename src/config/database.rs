use std::borrow::Cow;
use std::path::{Path, PathBuf};

use crate::config::env::EnvConfig;

/// Which database engine is in use. Decided once at startup and carried
/// through every query that needs dialect-specific syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
    MySql,
}

impl Dialect {
    pub fn as_str(self) -> &'static str {
        match self {
            Dialect::Sqlite => "sqlite",
            Dialect::Postgres => "postgres",
            Dialect::MySql => "mysql",
        }
    }

    /// sqlx::Any dispatches drivers by URL scheme but does not translate
    /// placeholder syntax. SQLite and MySQL bind with `?`; Postgres binds with
    /// `$1`, `$2`, ... Render rewrites `?` to numbered placeholders for the
    /// Postgres dialect. The naive scan is safe because none of the registry's
    /// SQL contains `?` inside string literals.
    pub fn render<'a>(self, sql: &'a str) -> Cow<'a, str> {
        if !matches!(self, Dialect::Postgres) {
            return Cow::Borrowed(sql);
        }
        let mut out = String::with_capacity(sql.len() + 8);
        let mut n: u32 = 1;
        for c in sql.chars() {
            if c == '?' {
                out.push('$');
                out.push_str(&n.to_string());
                n += 1;
            } else {
                out.push(c);
            }
        }
        Cow::Owned(out)
    }

    /// Render `sql` for the dialect and wrap it with `AssertSqlSafe` so it
    /// satisfies sqlx 0.9's `SqlSafeStr` bound at the call site. Every SQL
    /// string the registry constructs is built from static templates with
    /// only `db.dialect.render`-style rewriting — no user input is ever
    /// concatenated, so the assertion holds at every callsite.
    pub fn prep<'a>(self, sql: &'a str) -> sqlx::AssertSqlSafe<Cow<'a, str>> {
        sqlx::AssertSqlSafe(self.render(sql))
    }
}

/// Resolved database connection settings — what to feed sqlx and which
/// dialect to address it as.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub dialect: Dialect,
}

impl DatabaseConfig {
    /// `DATABASE_URL` wins when set. Otherwise assemble the URL from the
    /// `DB_*` split variables. Returns a descriptive error so the operator
    /// sees the exact missing piece in the startup log.
    pub fn resolve(env: &EnvConfig, data_dir: &Path) -> Result<Self, String> {
        if !env.database_url.is_empty() {
            return Self::from_url(&env.database_url);
        }

        let driver = env.db_driver.to_ascii_lowercase();
        match driver.as_str() {
            "" | "sqlite" => Ok(Self::sqlite_from_parts(env, data_dir)),
            "postgres" | "postgresql" => Self::assemble(env, "postgres", Dialect::Postgres, 5432),
            "mysql" | "mariadb" => Self::assemble(env, "mysql", Dialect::MySql, 3306),
            other => Err(format!(
                "unknown DB_DRIVER \"{other}\": use sqlite, postgres, mysql, or mariadb"
            )),
        }
    }

    fn from_url(raw: &str) -> Result<Self, String> {
        let scheme = raw
            .split_once(':')
            .map(|(s, _)| s)
            .unwrap_or(raw)
            .to_ascii_lowercase();
        match scheme.as_str() {
            "sqlite" => Ok(Self { url: raw.to_string(), dialect: Dialect::Sqlite }),
            "postgres" | "postgresql" => Ok(Self { url: raw.to_string(), dialect: Dialect::Postgres }),
            "mysql" => Ok(Self { url: raw.to_string(), dialect: Dialect::MySql }),
            "mariadb" => {
                let rest = raw.strip_prefix("mariadb://").unwrap_or(raw);
                Ok(Self { url: format!("mysql://{rest}"), dialect: Dialect::MySql })
            }
            other => Err(format!(
                "DATABASE_URL scheme \"{other}\" is not supported: use sqlite://, postgres://, mysql://, or mariadb://"
            )),
        }
    }

    fn sqlite_from_parts(env: &EnvConfig, data_dir: &Path) -> Self {
        let path: PathBuf = if env.db_name.is_empty() {
            data_dir.join("registry.db")
        } else {
            PathBuf::from(&env.db_name)
        };
        Self {
            url: format!("sqlite://{}", path.display()),
            dialect: Dialect::Sqlite,
        }
    }

    fn assemble(env: &EnvConfig, scheme: &str, dialect: Dialect, default_port: u16) -> Result<Self, String> {
        if env.db_host.is_empty() {
            return Err(format!(
                "{scheme}: DB_HOST is required when DATABASE_URL is unset"
            ));
        }
        if env.db_name.is_empty() {
            return Err(format!(
                "{scheme}: DB_NAME is required when DATABASE_URL is unset"
            ));
        }

        let port = if env.db_port == 0 {
            default_port
        } else {
            env.db_port
        };
        let creds = if env.db_user.is_empty() {
            String::new()
        } else {
            format!(
                "{}:{}@",
                percent_encode(&env.db_user),
                percent_encode(&env.db_password)
            )
        };

        let mut url = format!(
            "{scheme}://{creds}{host}:{port}/{name}",
            host = env.db_host,
            name = env.db_name,
        );
        if let Some(qs) = ssl_query(scheme, &env.db_ssl) {
            url.push('?');
            url.push_str(&qs);
        }

        Ok(Self { url, dialect })
    }
}

fn ssl_query(scheme: &str, mode: &str) -> Option<String> {
    let mode = mode.trim().to_ascii_lowercase();
    if mode.is_empty() {
        return None;
    }
    match (scheme, mode.as_str()) {
        ("postgres", "disable") | ("postgres", "prefer") | ("postgres", "require") => {
            Some(format!("sslmode={mode}"))
        }
        ("mysql", "disabled") | ("mysql", "preferred") | ("mysql", "required") => {
            Some(format!("ssl-mode={mode}"))
        }
        // Common aliases the operator might guess
        ("mysql", "disable") => Some("ssl-mode=disabled".into()),
        ("mysql", "prefer") => Some("ssl-mode=preferred".into()),
        ("mysql", "require") => Some("ssl-mode=required".into()),
        _ => None,
    }
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '@' => out.push_str("%40"),
            ':' => out.push_str("%3A"),
            '/' => out.push_str("%2F"),
            '?' => out.push_str("%3F"),
            '#' => out.push_str("%23"),
            '[' => out.push_str("%5B"),
            ']' => out.push_str("%5D"),
            ' ' => out.push_str("%20"),
            c if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') => out.push(c),
            c => {
                for b in c.to_string().as_bytes() {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_env() -> EnvConfig {
        EnvConfig {
            bind_address: String::new(),
            port: 0,
            node_env: String::new(),
            nexus_token: String::new(),
            nexus_token_pepper: String::new(),
            allowed_origins: vec![],
            system_services: vec![],
            health_interval_ms: 0,
            log_buffer_capacity: 0,
            data_dir: PathBuf::from("data"),
            database_url: String::new(),
            db_driver: String::new(),
            db_host: String::new(),
            db_port: 0,
            db_user: String::new(),
            db_password: String::new(),
            db_name: String::new(),
            db_ssl: String::new(),
        }
    }

    #[test]
    fn database_url_wins_over_split_vars() {
        let mut env = empty_env();
        env.database_url = "postgres://x:y@h:5432/db".into();
        env.db_driver = "sqlite".into();
        let cfg = DatabaseConfig::resolve(&env, Path::new(".")).unwrap();
        assert_eq!(cfg.dialect, Dialect::Postgres);
        assert_eq!(cfg.url, "postgres://x:y@h:5432/db");
    }

    #[test]
    fn mariadb_url_rewrites_to_mysql() {
        let mut env = empty_env();
        env.database_url = "mariadb://x:y@h:3306/db".into();
        let cfg = DatabaseConfig::resolve(&env, Path::new(".")).unwrap();
        assert_eq!(cfg.dialect, Dialect::MySql);
        assert_eq!(cfg.url, "mysql://x:y@h:3306/db");
    }

    #[test]
    fn default_driver_is_sqlite_in_data_dir() {
        let env = empty_env();
        let cfg = DatabaseConfig::resolve(&env, Path::new("/srv/data")).unwrap();
        assert_eq!(cfg.dialect, Dialect::Sqlite);
        assert!(cfg.url.starts_with("sqlite://"));
        assert!(cfg.url.ends_with("registry.db"));
    }

    #[test]
    fn split_postgres_assembles_url_with_ssl() {
        let mut env = empty_env();
        env.db_driver = "postgres".into();
        env.db_host = "db".into();
        env.db_port = 0;
        env.db_user = "nexus".into();
        env.db_password = "s e c@ret".into();
        env.db_name = "nexus_registry".into();
        env.db_ssl = "require".into();
        let cfg = DatabaseConfig::resolve(&env, Path::new(".")).unwrap();
        assert_eq!(cfg.dialect, Dialect::Postgres);
        assert_eq!(
            cfg.url,
            "postgres://nexus:s%20e%20c%40ret@db:5432/nexus_registry?sslmode=require"
        );
    }

    #[test]
    fn missing_host_for_postgres_is_a_clear_error() {
        let mut env = empty_env();
        env.db_driver = "postgres".into();
        env.db_name = "x".into();
        let err = DatabaseConfig::resolve(&env, Path::new(".")).unwrap_err();
        assert!(err.contains("DB_HOST"));
    }

    #[test]
    fn unknown_driver_lists_supported_options() {
        let mut env = empty_env();
        env.db_driver = "oracle".into();
        let err = DatabaseConfig::resolve(&env, Path::new(".")).unwrap_err();
        assert!(err.contains("sqlite"));
        assert!(err.contains("postgres"));
        assert!(err.contains("mariadb"));
    }

    #[test]
    fn placeholder_render_passes_sqlite_and_mysql_unchanged() {
        let sql = "INSERT INTO t (a, b) VALUES (?, ?)";
        assert_eq!(Dialect::Sqlite.render(sql), sql);
        assert_eq!(Dialect::MySql.render(sql), sql);
    }

    #[test]
    fn placeholder_render_numbers_postgres_placeholders() {
        let rendered = Dialect::Postgres.render("INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
        assert_eq!(rendered, "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)");
    }
}
