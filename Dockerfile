# ============================================================================
# nexus-registry — Node 22 + Express. Pakker fra GitHub Packages.
# ============================================================================

FROM node:22-alpine AS builder
WORKDIR /app

ARG NODE_AUTH_TOKEN
COPY package*.json .npmrc tsconfig.json ./
RUN if [ -z "$NODE_AUTH_TOKEN" ]; then echo "NODE_AUTH_TOKEN build-arg er paakraevet"; exit 1; fi && \
    NODE_AUTH_TOKEN=${NODE_AUTH_TOKEN} npm install --no-audit --no-fund --legacy-peer-deps

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

ARG NODE_AUTH_TOKEN
COPY package*.json .npmrc ./
RUN NODE_AUTH_TOKEN=${NODE_AUTH_TOKEN} npm install --omit=dev --no-audit --no-fund --legacy-peer-deps && \
    rm -f .npmrc && \
    npm cache clean --force

COPY --from=builder /app/dist ./dist
COPY src/data ./data

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3000/health || exit 1

CMD ["node", "dist/index.js"]
