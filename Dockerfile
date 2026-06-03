# ============================================================================
# nexus-registry — bygges fra projekt-rod-context fordi @bimo-nexus/core via
# file:../nexus-packages/packages/core kun resolves når begge ligger på samme
# relative niveau som på host-fs.
# ============================================================================

FROM node:22-alpine AS builder

# ----- Build @bimo-nexus/core (file: dep) -----
WORKDIR /workspace/nexus-packages/packages/core
COPY nexus-packages/packages/core/package*.json ./
RUN npm install --no-audit --no-fund --legacy-peer-deps
COPY nexus-packages/packages/core/tsconfig.json ./
COPY nexus-packages/packages/core/tsup.config.ts ./
COPY nexus-packages/packages/core/src ./src
RUN npm run build

# ----- Build nexus-registry -----
WORKDIR /workspace/nexus-registry
COPY nexus-registry/package*.json ./
COPY nexus-registry/tsconfig.json ./
RUN npm install --no-audit --no-fund --legacy-peer-deps
COPY nexus-registry/src ./src
RUN npm run build

# ============================================================================
# Production runtime
# ============================================================================
FROM node:22-alpine
RUN apk add --no-cache wget
ENV NODE_ENV=production
ENV PORT=3000

# Behold samme /workspace struktur så file: deps stadig resolves i runtime
WORKDIR /workspace/nexus-packages/packages/core
COPY --from=builder /workspace/nexus-packages/packages/core/package.json ./
COPY --from=builder /workspace/nexus-packages/packages/core/dist ./dist

WORKDIR /workspace/nexus-registry
COPY --from=builder /workspace/nexus-registry/package.json ./
COPY --from=builder /workspace/nexus-registry/dist ./dist
COPY nexus-registry/src/data ./data
RUN npm install --omit=dev --no-audit --no-fund --legacy-peer-deps && npm cache clean --force

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3000/health || exit 1

CMD ["node", "dist/index.js"]
