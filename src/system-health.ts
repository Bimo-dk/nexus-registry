import { getAllRemotes, updateRemote } from './store.js';
import { broadcastSystemHealth } from './websocket.js';
import type { RemoteConfig, RemoteHealthStatus } from '@bimo-dk/nexus-core';

export interface ServiceHealth {
  name: string;
  kind: 'registry' | 'system' | 'remote';
  enabled: boolean;
  status: RemoteHealthStatus;
  latencyMs?: number;
  lastChecked: string;
  url?: string;
  error?: string;
}

export interface SystemHealthSnapshot {
  timestamp: string;
  services: ServiceHealth[];
  summary: {
    total: number;
    healthy: number;
    degraded: number;
    down: number;
    unknown: number;
  };
}

interface SystemService {
  name: string;
  healthUrl: string;
}

let cachedSnapshot: SystemHealthSnapshot | null = null;
let pollTimer: NodeJS.Timeout | null = null;

/**
 * Parse SYSTEM_SERVICES env-var. Format: 'name=url,name=url'
 * Example: 'gateway=http://gateway/health,host=http://host/health'
 */
function parseSystemServices(): SystemService[] {
  const raw = process.env.SYSTEM_SERVICES ?? 'gateway=http://gateway/health,host=http://host/health';
  return raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((entry) => {
      const eqIdx = entry.indexOf('=');
      if (eqIdx === -1) return null;
      const name = entry.substring(0, eqIdx).trim();
      const healthUrl = entry.substring(eqIdx + 1).trim();
      if (!name || !healthUrl) return null;
      return { name, healthUrl };
    })
    .filter((s): s is SystemService => s !== null);
}

const SYSTEM_SERVICES = parseSystemServices();

/**
 * Derive the health URL the registry can reach server-side.
 * Prefers remote.upstreamUrl (the Docker-internal URL) when set.
 * Falls back to camelCase → kebab-case name convention.
 */
function deriveInternalHealthUrl(remote: RemoteConfig): string {
  if (remote.upstreamUrl) {
    const base = remote.upstreamUrl.replace(/\/$/, '');
    return `${base}/health`;
  }
  const kebab = remote.name.replace(/([a-z0-9])([A-Z])/g, '$1-$2').toLowerCase();
  return `http://${kebab}/health`;
}

async function checkHealth(url: string, timeoutMs = 2000): Promise<{ ok: boolean; latencyMs: number; error?: string }> {
  const start = Date.now();
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    const res = await fetch(url, { signal: ctrl.signal, method: 'GET' });
    clearTimeout(t);
    return { ok: res.ok, latencyMs: Date.now() - start };
  } catch (err) {
    return {
      ok: false,
      latencyMs: Date.now() - start,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

function statusFromLatency(ok: boolean, latencyMs: number): RemoteHealthStatus {
  if (!ok) return 'down';
  if (latencyMs > 1500) return 'degraded';
  return 'healthy';
}

/**
 * Run one complete health round for all known services.
 * Returns a snapshot AND updates remote.healthStatus in the store.
 */
export async function runHealthCheckCycle(): Promise<SystemHealthSnapshot> {
  const now = new Date().toISOString();

  // 1. Registry self
  const registryHealth: ServiceHealth = {
    name: 'registry',
    kind: 'registry',
    enabled: true,
    status: 'healthy',
    latencyMs: 0,
    lastChecked: now,
  };

  // 2. System services (gateway, host, ...)
  const sysChecks = await Promise.all(
    SYSTEM_SERVICES.map(async (svc): Promise<ServiceHealth> => {
      const r = await checkHealth(svc.healthUrl);
      return {
        name: svc.name,
        kind: 'system',
        enabled: true,
        status: statusFromLatency(r.ok, r.latencyMs),
        latencyMs: r.latencyMs,
        lastChecked: now,
        url: svc.healthUrl,
        error: r.error,
      };
    }),
  );

  // 3. Remotes (from registry data)
  const remotes = await getAllRemotes();
  const remoteChecks = await Promise.all(
    remotes.map(async (remote): Promise<ServiceHealth> => {
      if (!remote.enabled) {
        return {
          name: remote.name,
          kind: 'remote',
          enabled: false,
          status: 'unknown',
          lastChecked: now,
        };
      }
      const url = deriveInternalHealthUrl(remote);
      const r = await checkHealth(url);
      const status = statusFromLatency(r.ok, r.latencyMs);

      // Update remote in store so /api/remotes also reflects it
      try {
        await updateRemote(remote.name, { healthStatus: status, lastHealthCheck: now });
      } catch {
        /* update may fail if remote was deleted mid-cycle — ignore */
      }

      return {
        name: remote.name,
        kind: 'remote',
        enabled: true,
        status,
        latencyMs: r.latencyMs,
        lastChecked: now,
        url,
        error: r.error,
      };
    }),
  );

  const allServices = [registryHealth, ...sysChecks, ...remoteChecks];
  const summary = allServices.reduce(
    (acc, s) => {
      acc.total++;
      acc[s.status]++;
      return acc;
    },
    { total: 0, healthy: 0, degraded: 0, down: 0, unknown: 0 },
  );

  const snapshot: SystemHealthSnapshot = { timestamp: now, services: allServices, summary };
  cachedSnapshot = snapshot;
  broadcastSystemHealth(snapshot);
  return snapshot;
}

export function getCachedSnapshot(): SystemHealthSnapshot | null {
  return cachedSnapshot;
}

export function startHealthCheckLoop(intervalMs = 30_000): void {
  if (pollTimer) return;
  // Run immediately, then every interval
  runHealthCheckCycle().catch((err) => console.error('[system-health] Initial check failed:', err));
  pollTimer = setInterval(() => {
    runHealthCheckCycle().catch((err) => console.error('[system-health] Cycle failed:', err));
  }, intervalMs);
  console.log(`[system-health] Loop started — interval ${intervalMs}ms — system services: ${SYSTEM_SERVICES.map((s) => s.name).join(', ')}`);
}

export function stopHealthCheckLoop(): void {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}
