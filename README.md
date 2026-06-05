# nexus-registry

Source of truth for a Nexus deployment. Rust binary (axum + sqlx + tokio) — listens on `:8670`, owns the SQLite catalogue of hosts/gates/remotes/config, broadcasts every change over `/api/ws`.

Tenant-facing overview: [nexus.bimo.dk — Infrastructure: Registry](https://nexus.bimo.dk/infrastructure/infra-registry).
Contributor docs: [Internals — architecture](https://nexus.bimo.dk/internals/nexus-registry/architecture) · [code map](https://nexus.bimo.dk/internals/nexus-registry/code-map).

## Build and run

```bash
# Build
cargo build --release

# Run
NEXUS_TOKEN=dev-token \
NEXUS_TOKEN_PEPPER=replace-me-in-prod \
DATABASE_URL=sqlite:./data/registry.db \
ALLOWED_ORIGINS='*' \
  ./target/release/nexus-registry
```

The full local stack is in `nexus-test/` — `pwsh ./start.ps1` from there brings up the registry, gateway, and a tenant host on Docker.

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `NEXUS_TOKEN` | (none) | Active token. If empty, all auth'd endpoints reject until `/api/config/token/rotate` is called. |
| `NEXUS_TOKEN_PEPPER` | `nexus-registry-default-pepper` | HMAC pepper. **Set this in production.** |
| `DATABASE_URL` | `sqlite:./data/registry.db` | sqlx URL. |
| `DATA_DIR` | `./data` | Created on boot. |
| `BIND_ADDRESS` | `0.0.0.0` | Listen IP. |
| `PORT` | `8670` | Listen port. |
| `ALLOWED_ORIGINS` | (any) | Comma-separated CORS allowlist, or `*`. |
| `SYSTEM_SERVICES` | (none) | `name=url,name=url` pairs probed by the health loop. |
| `HEALTH_CHECK_INTERVAL_MS` | `30000` | Cadence of the periodic probe loop. |

Full list: [reference/environment](https://nexus.bimo.dk/reference/environment).

## API

Endpoint table and request/response shapes: [reference/api-reference](https://nexus.bimo.dk/reference/api-reference).
WebSocket frame schema: [reference/websocket-messages](https://nexus.bimo.dk/reference/websocket-messages).

## Test

```bash
cargo test
```
