// Patterns kept in lockstep with @bimo-dk/nexus-core/src/validators.ts.
// If you change either side, change both — tenant remotes accepted by one
// service must not be rejected by the other.
use once_cell::sync::Lazy;
use regex::Regex;

static REMOTE_NAME_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-z][a-zA-Z0-9]*$").unwrap());
static ROUTE_PATH_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-z][a-z0-9-]*$").unwrap());
// Hosts/gates names allow upper or lower case start — wider character set than
// remote names because they're human-friendly labels, not module identifiers.
static ENTITY_NAME_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9]*$").unwrap());
static DOMAIN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // hostname[:port] — letters, digits, dots, hyphens, optional :port
    Regex::new(r"^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?(:\d{1,5})?$").unwrap()
});

pub fn is_valid_remote_name(name: &str) -> bool {
    !name.is_empty() && REMOTE_NAME_PATTERN.is_match(name)
}

pub fn is_valid_route_path(path: &str) -> bool {
    !path.is_empty() && ROUTE_PATH_PATTERN.is_match(path)
}

pub fn is_valid_url(url: &str) -> bool {
    if url.is_empty() {
        return false;
    }
    url.starts_with("http://") || url.starts_with("https://")
}

pub fn is_valid_url_or_path(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    if value.starts_with('/') {
        return true;
    }
    is_valid_url(value)
}

pub fn is_valid_entity_name(name: &str) -> bool {
    !name.is_empty() && ENTITY_NAME_PATTERN.is_match(name)
}

pub fn is_valid_framework(s: &str) -> bool {
    matches!(s, "angular" | "vue" | "react")
}

#[allow(dead_code)]
pub fn is_valid_framework_or_auto(s: &str) -> bool {
    s == "auto" || is_valid_framework(s)
}

/// Host URL: valid http(s) URL with no trailing slash.
pub fn is_valid_host_url(s: &str) -> bool {
    is_valid_url(s) && !s.ends_with('/')
}

/// remoteEntry: absolute path starting with `/` OR a full https URL.
pub fn is_valid_remote_entry(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    if s.starts_with('/') {
        return true;
    }
    s.starts_with("https://")
}

pub fn is_valid_domain(s: &str) -> bool {
    !s.is_empty() && DOMAIN_PATTERN.is_match(s)
}

/// Parses a remote `visibility` value. Returns `Ok(None)` for the literal
/// string `"global"`, `Ok(Some(host_id))` for `"host:<id>"`.
pub fn parse_visibility(v: &str) -> Result<Option<&str>, &'static str> {
    if v == "global" {
        return Ok(None);
    }
    if let Some(id) = v.strip_prefix("host:") {
        if id.is_empty() {
            return Err("visibility \"host:\" requires a host id");
        }
        return Ok(Some(id));
    }
    Err("visibility must be \"global\" or \"host:<host_id>\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test corpus mirrors @bimo-dk/nexus-core/src/validators.test.ts —
    // every case there must produce the same verdict here.

    #[test]
    fn remote_name_accepts_camel_case() {
        assert!(is_valid_remote_name("remoteOne"));
        assert!(is_valid_remote_name("checkout"));
        assert!(is_valid_remote_name("paymentFlow123"));
        assert!(is_valid_remote_name("a"));
        assert!(is_valid_remote_name("a1b2"));
    }

    #[test]
    fn remote_name_rejects_bad_input() {
        assert!(!is_valid_remote_name(""));
        assert!(!is_valid_remote_name("Remote"));
        assert!(!is_valid_remote_name("Remote-One"));
        assert!(!is_valid_remote_name("RemoteOne"));
        assert!(!is_valid_remote_name("123remote"));
        assert!(!is_valid_remote_name("1remote"));
        assert!(!is_valid_remote_name("remote one"));
        assert!(!is_valid_remote_name("remote-one"));
        assert!(!is_valid_remote_name("remote_one"));
    }

    #[test]
    fn route_path_accepts_kebab_case() {
        assert!(is_valid_route_path("remote-one"));
        assert!(is_valid_route_path("payment-flow-v2"));
        assert!(is_valid_route_path("checkout"));
        assert!(is_valid_route_path("a"));
        assert!(is_valid_route_path("a1-b2"));
    }

    #[test]
    fn route_path_rejects_bad_input() {
        assert!(!is_valid_route_path(""));
        assert!(!is_valid_route_path("Remote"));
        assert!(!is_valid_route_path("RemoteOne"));
        assert!(!is_valid_route_path("-leading-dash"));
        assert!(!is_valid_route_path("1-numeric-start"));
        assert!(!is_valid_route_path("remoteOne"));
    }

    #[test]
    fn url_accepts_http_and_https() {
        assert!(is_valid_url("http://example.com"));
        assert!(is_valid_url("https://api.nexus.dk/path"));
    }

    #[test]
    fn url_rejects_bad_input() {
        assert!(!is_valid_url(""));
        assert!(!is_valid_url("not a url"));
        assert!(!is_valid_url("ftp://example.com"));
    }

    #[test]
    fn url_or_path_accepts_both() {
        assert!(is_valid_url_or_path("/remotes/x.json"));
        assert!(is_valid_url_or_path("http://x"));
        assert!(is_valid_url_or_path("https://x.com/a"));
    }

    #[test]
    fn url_or_path_rejects_bad_input() {
        assert!(!is_valid_url_or_path(""));
        assert!(!is_valid_url_or_path("ftp://x"));
        assert!(!is_valid_url_or_path("relative/path"));
        assert!(!is_valid_url_or_path("not a url"));
    }
}
