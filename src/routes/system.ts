import { Router, type Request, type Response } from 'express';
import { getCachedSnapshot, runHealthCheckCycle } from '../system-health.js';
import { getLogs } from '../observability/log-buffer.js';
import { getMetricsSnapshot } from '../observability/metrics.js';
import { getConnectionCount } from '../websocket.js';

export const systemRouter = Router();

/**
 * GET /api/system/health
 *   Returns cached health snapshot (refreshed every HEALTH_CHECK_INTERVAL_MS
 *   by the background loop). Pass ?fresh=true to force an immediate check.
 */
systemRouter.get('/health', async (req: Request, res: Response) => {
  if (req.query['fresh'] === 'true') {
    const snapshot = await runHealthCheckCycle();
    res.json(snapshot);
    return;
  }
  const cached = getCachedSnapshot();
  if (cached) {
    res.json(cached);
    return;
  }
  const snapshot = await runHealthCheckCycle();
  res.json(snapshot);
});

/**
 * GET /api/system/config
 *   Returns the effective registry configuration (env-derived). Read-only.
 */
systemRouter.get('/config', (_req: Request, res: Response) => {
  res.json({
    nodeEnv: process.env.NODE_ENV ?? 'development',
    port: Number(process.env.PORT ?? 3000),
    healthCheckIntervalMs: Number(process.env.HEALTH_CHECK_INTERVAL_MS ?? 30_000),
    logBufferCapacity: Number(process.env.LOG_BUFFER_CAPACITY ?? 500),
    allowedOrigins: (process.env.ALLOWED_ORIGINS ?? '*')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    systemServices: (process.env.SYSTEM_SERVICES ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    nexusTokenConfigured: Boolean(process.env.NEXUS_TOKEN),
    wsClients: getConnectionCount(),
    nodeVersion: process.version,
    uptimeSec: Math.floor(process.uptime()),
  });
});

/**
 * GET /api/system/logs?since=ISO&limit=N&level=info
 *   Returns recent log entries from the in-memory ring buffer.
 */
systemRouter.get('/logs', (req: Request, res: Response) => {
  const since = typeof req.query['since'] === 'string' ? req.query['since'] : undefined;
  const limitStr = typeof req.query['limit'] === 'string' ? req.query['limit'] : undefined;
  const limit = limitStr ? Number(limitStr) : 100;
  const levelRaw = typeof req.query['level'] === 'string' ? req.query['level'] : undefined;
  const level = ['debug', 'info', 'warn', 'error'].includes(levelRaw ?? '')
    ? (levelRaw as 'debug' | 'info' | 'warn' | 'error')
    : undefined;
  res.json({ entries: getLogs({ since, limit, level }) });
});

/**
 * GET /api/system/metrics
 *   Returns request counters, latency stats, custom counters, process info.
 */
systemRouter.get('/metrics', (_req: Request, res: Response) => {
  res.json(getMetricsSnapshot());
});
