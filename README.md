# nexus-registry

Source of truth for a Nexus deployment. Rust binary (axum + sqlx + tokio) — owns the catalogue of hosts, gates, remotes, and all hot-reloadable platform configuration. Every change is broadcast over `/api/ws` so the gateway and connected clients update without restart.

- Image: [`ghcr.io/bimo-dk/nexus-registry`](https://github.com/Bimo-dk/nexus-registry/pkgs/container/nexus-registry)
- Listens on: `:8670` (HTTP + WebSocket)
- Trust boundary: **internal-only.** The registry is never exposed to the public internet — only the gateway and portal bind public ports. See [security — Network trust boundary](https://nexus.bimo.dk/reference/security#network-trust-boundary).
- Persistence: **SQLite, PostgreSQL, MySQL or MariaDB** — see [Choosing a database](#choosing-a-database)
- Docs: [Tenant-facing overview](https://nexus.bimo.dk/infrastructure/infra-registry) · [Internals — architecture](https://nexus.bimo.dk/internals/nexus-registry/architecture) · [code map](https://nexus.bimo.dk/internals/nexus-registry/code-map)

## Quick start (pull and run)

```bash
docker pull ghcr.io/bimo-dk/nexus-registry:latest

docker run -d \
  --name nexus-registry \
  -p 8670:8670 \
  -v nexus-registry-data:/app/data \
  -e NEXUS_TOKEN="$(openssl rand -hex 32)" \
  -e NEXUS_TOKEN_PEPPER="$(openssl rand -hex 32)" \
  -e ALLOWED_ORIGINS='*' \
  ghcr.io/bimo-dk/nexus-registry:latest

# Liveness probe
curl http://localhost:8670/health
```

That is the minimum needed to spin up an empty registry. Replace `ALLOWED_ORIGINS='*'` with a concrete comma-separated list in production.

The `NEXUS_TOKEN` you set here is the shared secret every other Nexus service (gateway, portal, CLI) presents as `X-Nexus-Token`. The `NEXUS_TOKEN_PEPPER` must stay stable across rotations — changing it invalidates every stored token hash.

## Environment variables

`docker inspect ghcr.io/bimo-dk/nexus-registry:latest --format '{{json .Config.Env}}'` lists the live contract. The same set, annotated:

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `NEXUS_TOKEN` | **yes** | (empty) | Shared secret. Empty value rejects every authenticated request. Rotate via `POST /api/config/token/rotate`. |
| `NEXUS_TOKEN_PEPPER` | **yes (prod)** | `nexus-registry-default-pepper` | HMAC pepper for stored token hashes. Logs a warning every boot when left at default. Keep stable across token rotations. |
| `BIND_ADDRESS` | no | `0.0.0.0` | Listen IP. |
| `PORT` | no | `8670` | Listen port. |
| `ALLOWED_ORIGINS` | no | (any) | Comma-separated CORS allowlist, or `*` / empty for any. |
| `DATA_DIR` | no | `/app/data` | Directory for the SQLite file. SQLite only. |
| `DATABASE_URL` | no | derived from `DB_*` or `DATA_DIR` | Full connection URL (`sqlite://`, `postgres://`, `mysql://`, `mariadb://`). Wins over `DB_*` split vars. |
| `DB_DRIVER` | no | (empty → sqlite) | `sqlite`, `postgres`, `mysql`, or `mariadb`. Ignored when `DATABASE_URL` is set. |
| `DB_HOST` | no | (empty) | Required for postgres/mysql/mariadb when `DATABASE_URL` is unset. |
| `DB_PORT` | no | `5432` (pg) / `3306` (mysql) | Override default port. `0` = use default. |
| `DB_USER` | no | (empty) | Required for postgres/mysql/mariadb when `DATABASE_URL` is unset. |
| `DB_PASSWORD` | no | (empty) | URL-encoded internally — pass the raw password. |
| `DB_NAME` | no | `registry` (file path for SQLite) | Database name. For SQLite, the file path; defaults to `${DATA_DIR}/registry.db`. |
| `DB_SSL` | no | (empty) | `disable` / `prefer` / `require` (pg) or `disabled` / `preferred` / `required` (mysql). Short aliases work for mysql too. |
| `SYSTEM_SERVICES` | no | `gateway=http://gateway:8668/health,host=http://host/health` | Comma-separated `name=health_url` pairs probed by the periodic health loop. Surfaced via `/api/system/health`. |
| `HEALTH_CHECK_INTERVAL_MS` | no | `30000` | Cadence of the periodic probe loop. |
| `LOG_BUFFER_CAPACITY` | no | `500` | Ring buffer size for `/api/system/logs` and live `log` WebSocket subscriptions. |
| `NODE_ENV` | no | `production` (Dockerfile), `development` (binary) | Free-form environment label exposed via `/api/system/config`. Common values: `development`, `staging`, `production`. |
| `RUST_LOG` | no | `info` | `tracing-subscriber` filter directive (e.g. `info,nexus_registry=debug`). |

Full reference with valid ranges: [docs — reference/environment](https://nexus.bimo.dk/reference/environment).

## Choosing a database

The registry supports four engines, picked at runtime from the connection URL. Schema is created on first boot for whichever you choose — no separate migration step. SQLite is the default if nothing is set.

### SQLite (default)

Best for single-node deployments and local development. Storage is a single file you mount as a volume.

```bash
docker run -d --name nexus-registry \
  -p 8670:8670 \
  -v nexus-registry-data:/app/data \
  -e NEXUS_TOKEN="$(openssl rand -hex 32)" \
  -e NEXUS_TOKEN_PEPPER="$(openssl rand -hex 32)" \
  ghcr.io/bimo-dk/nexus-registry:latest
```

### PostgreSQL

Best for production HA — multiple registry replicas can share one Postgres cluster.

```bash
docker run -d --name nexus-registry \
  -p 8670:8670 \
  -e DATABASE_URL="postgres://nexus:secret@postgres.internal:5432/nexus_registry" \
  -e NEXUS_TOKEN="..." -e NEXUS_TOKEN_PEPPER="..." \
  ghcr.io/bimo-dk/nexus-registry:latest

# Or split-vars (compose-friendly when the password has special chars):
docker run -d --name nexus-registry \
  -e DB_DRIVER=postgres \
  -e DB_HOST=postgres.internal \
  -e DB_USER=nexus \
  -e DB_PASSWORD='s3cr3t!@#' \
  -e DB_NAME=nexus_registry \
  -e DB_SSL=require \
  -e NEXUS_TOKEN="..." -e NEXUS_TOKEN_PEPPER="..." \
  ghcr.io/bimo-dk/nexus-registry:latest
```

### MySQL

```bash
docker run -d --name nexus-registry \
  -e DATABASE_URL="mysql://nexus:secret@mysql.internal:3306/nexus_registry" \
  -e NEXUS_TOKEN="..." -e NEXUS_TOKEN_PEPPER="..." \
  ghcr.io/bimo-dk/nexus-registry:latest
```

### MariaDB

Identical wire protocol to MySQL — use either scheme; both are accepted.

```bash
docker run -d --name nexus-registry \
  -e DATABASE_URL="mariadb://nexus:secret@mariadb.internal:3306/nexus_registry" \
  -e NEXUS_TOKEN="..." -e NEXUS_TOKEN_PEPPER="..." \
  ghcr.io/bimo-dk/nexus-registry:latest
```

### Picking between `DATABASE_URL` and `DB_*`

| You're using | Use |
|---|---|
| Plain `docker run` / shell scripts | `DATABASE_URL` — single value, sqlx parses the scheme |
| docker compose / Kubernetes Secrets | `DB_*` split vars — no URL-escaping of passwords with `@`, `:`, `/` |
| Both | `DATABASE_URL` wins |

### Persistence and volumes

- **SQLite**: mount `/app/data` (the `${DATA_DIR}` default) as a named volume or bind mount. The single file lives there as `registry.db`.
- **Postgres / MySQL / MariaDB**: the registry doesn't own a volume — your DB server does. No volume mount needed on the registry container.

The image bundles a seed file (`src/data/registry.json`) that initialises an empty SQLite database on first boot. Subsequent boots ignore it. The seed is only honored on SQLite.

## Running with a gateway

The registry on its own does nothing user-visible. The companion gateway proxies traffic for one or more hosts based on what's in the registry. Minimal docker-compose:

```yaml
services:
  registry:
    image: ghcr.io/bimo-dk/nexus-registry:latest
    ports: ["8670:8670"]
    volumes:
      - nexus-registry-data:/app/data
    environment:
      NEXUS_TOKEN: ${NEXUS_TOKEN:?set NEXUS_TOKEN in your .env}
      NEXUS_TOKEN_PEPPER: ${NEXUS_TOKEN_PEPPER:?set NEXUS_TOKEN_PEPPER in your .env}
      ALLOWED_ORIGINS: "*"
      # Default is SQLite at /app/data/registry.db. Override per service to use
      # a Postgres / MySQL / MariaDB instance — examples in "Choosing a database".
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8670/health"]
      interval: 10s
      timeout: 5s
      retries: 5

  gateway:
    image: ghcr.io/bimo-dk/nexus-gateway:latest
    ports: ["8668:8668"]
    depends_on:
      registry:
        condition: service_healthy
    environment:
      NEXUS_TOKEN: ${NEXUS_TOKEN}
      REGISTRY_URL: http://registry:8670
      NEXUS_GATE_NAME: localhost:8668

volumes:
  nexus-registry-data:
```

Put `NEXUS_TOKEN` and `NEXUS_TOKEN_PEPPER` in a sibling `.env`. `docker compose up -d` brings the pair online; the gateway auto-registers its gate against the registry on first boot if `NEXUS_HOST_NAME` is set (see [nexus-gateway README](https://github.com/Bimo-dk/nexus-gateway#readme)).

## API surface

- HTTP: every endpoint under `/api/*` requires `X-Nexus-Token`. `/health` is public. Full reference: [docs — reference/api-reference](https://nexus.bimo.dk/reference/api-reference).
- WebSocket: `GET /api/ws` (token via `X-Nexus-Token` header or `?token=` query). Frame schema: [docs — reference/websocket-messages](https://nexus.bimo.dk/reference/websocket-messages).

## Build from source

```bash
cargo build --release
NEXUS_TOKEN=dev-token \
NEXUS_TOKEN_PEPPER=dev-pepper \
ALLOWED_ORIGINS='*' \
  ./target/release/nexus-registry
```

Tests:

```bash
cargo test
```
