# syntax=docker/dockerfile:1.7
# ============================================================================
# nexus-registry — Node 22 + Express. Pakker fra GitHub Packages.
# Bruger BuildKit secrets så NODE_AUTH_TOKEN IKKE leakes i build-logs.
# ============================================================================

FROM node:22-alpine AS builder
WORKDIR /app

COPY package*.json .npmrc tsconfig.json ./

RUN --mount=type=secret,id=node_auth_token,required=true \
    NODE_AUTH_TOKEN=$(cat /run/secrets/node_auth_token) \
    npm install --no-audit --no-fund --legacy-peer-deps

COPY src ./src
RUN npm run build

# ============================================================================
# Production runtime
# ============================================================================
FROM node:22-alpine
RUN apk add --no-cache wget
ENV NODE_ENV=production
ENV PORT=3000
WORKDIR /app

COPY package*.json .npmrc ./

RUN --mount=type=secret,id=node_auth_token,required=true \
    NODE_AUTH_TOKEN=$(cat /run/secrets/node_auth_token) \
    npm install --omit=dev --no-audit --no-fund --legacy-peer-deps && \
    rm -f .npmrc && \
    npm cache clean --force

COPY --from=builder /app/dist ./dist
COPY src/data ./data

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3000/health || exit 1

CMD ["node", "dist/index.js"]
