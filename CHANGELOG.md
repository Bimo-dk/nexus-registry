# nexus-registry

## 1.0.0

First stable release. The registry is the source of truth for hosts,
gates, remotes, and every hot-configurable platform setting. Schema
and HTTP/WebSocket API are now stable.

### Highlights

- Stateless Rust + axum + sqlx (Any driver) listening on `:8670`.
  Switch the backing store between SQLite, PostgreSQL, MySQL, and
  MariaDB via the `DATABASE_URL` env-var without code changes.
- REST CRUD for hosts, gates, remotes, plus config endpoints for
  rate-limiting, WebSocket reconnect policy, gateway DDoS protection,
  metrics, token rotation, and circuit-breaker thresholds.
- `/api/ws` WebSocket broadcasts `welcome`, `remotes_changed`,
  `host_changed`, `system_health`, and `log` messages so every
  connected gateway and portal updates within milliseconds of a
  configuration change.
- Health-check loop per remote with circuit-breaker and configurable
  back-off. Failures are recorded but do not remove the remote from
  the routing table.
- Per-IP and per-token rate limiting on the registry's own ingress.
- Ring-buffered log accessible over WebSocket for portal live tailing.

### License

Relicensed from MIT to GNU Affero General Public License v3.0 or any
later version (AGPL-3.0-or-later). Commercial license: svp@bimo.dk.
