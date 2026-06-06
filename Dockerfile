# syntax=docker/dockerfile:1.7
# ============================================================================
# nexus-registry — Rust 1.93 + axum + SQLite. Statisk linket musl-binary.
# Ingen pakke-registry auth påkrævet (alle crates kommer fra crates.io).
# ============================================================================

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcc

WORKDIR /app

COPY Cargo.toml ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp /app/target/release/nexus-registry /usr/local/bin/nexus-registry

# ============================================================================
# Production runtime — minimal alpine, kun det binæren skal bruge.
# ============================================================================
FROM alpine:3
RUN apk add --no-cache wget ca-certificates

# ----------------------------------------------------------------------------
# Configuration — override any of these at `docker run -e KEY=value`
# or in docker-compose under `environment:`.
# ----------------------------------------------------------------------------
# Network
ENV BIND_ADDRESS=0.0.0.0
ENV PORT=8670

# Authentication
#   NEXUS_TOKEN        REQUIRED. Empty value rejects every authenticated request.
#   NEXUS_TOKEN_PEPPER REQUIRED in production. Used as HMAC pepper when hashing
#                      stored tokens. Leaving the default pepper logs a warning
#                      on every boot — set this to a strong random string and
#                      keep it stable across rotations.
ENV NEXUS_TOKEN=
ENV NEXUS_TOKEN_PEPPER=

# CORS — comma-separated list of allowed origins, or "*" / empty for any
ENV ALLOWED_ORIGINS=

# Persistence
#   DATABASE_URL takes precedence. If unset, registry uses sqlite at
#   "${DATA_DIR}/registry.db". Mount /app/data as a volume to persist.
ENV DATA_DIR=/app/data
ENV DATABASE_URL=

# Health-check loop
#   SYSTEM_SERVICES is a comma-separated list of "name=health_url" pairs
#   probed every HEALTH_CHECK_INTERVAL_MS. Surfaced via /api/system/health.
ENV HEALTH_CHECK_INTERVAL_MS=30000
ENV SYSTEM_SERVICES=gateway=http://gateway:8668/health,host=http://host/health

# Observability
#   LOG_BUFFER_CAPACITY — ring-buffered log entries surfaced via /api/system/logs
#                         and the live WebSocket "log" subscription.
#   NODE_ENV            — free-form environment label exposed via /api/system/config.
#                         Common values: development, staging, production.
#   RUST_LOG            — tracing-subscriber EnvFilter directive
#                         (e.g. "info", "info,nexus_registry=debug").
ENV LOG_BUFFER_CAPACITY=500
ENV NODE_ENV=production
ENV RUST_LOG=info
# ----------------------------------------------------------------------------

WORKDIR /app

COPY --from=builder /usr/local/bin/nexus-registry /usr/local/bin/nexus-registry
COPY src/data ./data

EXPOSE 8670

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -qO- "http://localhost:${PORT}/health" || exit 1

CMD ["nexus-registry"]
