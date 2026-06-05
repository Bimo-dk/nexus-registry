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

# Authentication (REQUIRED — empty token rejects every authenticated request)
ENV NEXUS_TOKEN=

# CORS — comma-separated list of allowed origins, or "*" / empty for any
ENV ALLOWED_ORIGINS=

# Persistence
#   DATABASE_URL takes precedence. If unset, registry uses sqlite at
#   "${DATA_DIR}/registry.db". Path can point at any mounted volume.
ENV DATA_DIR=/app/data
ENV DATABASE_URL=

# Health-check loop
ENV HEALTH_CHECK_INTERVAL_MS=30000
ENV SYSTEM_SERVICES=gateway=http://gateway/health,host=http://host/health

# Observability
ENV LOG_BUFFER_CAPACITY=500
ENV NODE_ENV=production
# ----------------------------------------------------------------------------

WORKDIR /app

COPY --from=builder /usr/local/bin/nexus-registry /usr/local/bin/nexus-registry
COPY src/data ./data

EXPOSE 8670

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -qO- "http://localhost:${PORT}/health" || exit 1

CMD ["nexus-registry"]
