/**
 * Lightweight in-memory metrics: request counters, latency histogram, custom
 * counters. Reset on process restart — meant for live dashboards, not long-term
 * storage. Use Prometheus/OpenTelemetry for that.
 */
import type { Request, Response, NextFunction } from 'express';

interface RouteStats {
  count: number;
  errors: number;
  totalDurationMs: number;
  lastDurationMs: number;
  minDurationMs: number;
  maxDurationMs: number;
  byStatus: Record<string, number>;
}

const routeStats = new Map<string, RouteStats>();
const counters = new Map<string, number>();
const startedAt = Date.now();

function getOrCreate(key: string): RouteStats {
  let s = routeStats.get(key);
  if (!s) {
    s = { count: 0, errors: 0, totalDurationMs: 0, lastDurationMs: 0, minDurationMs: Infinity, maxDurationMs: 0, byStatus: {} };
    routeStats.set(key, s);
  }
  return s;
}

export function metricsMiddleware(req: Request, res: Response, next: NextFunction): void {
  const start = Date.now();
  res.on('finish', () => {
    const durMs = Date.now() - start;
    const routeKey = `${req.method} ${req.route?.path ?? req.path}`;
    const s = getOrCreate(routeKey);
    s.count++;
    s.totalDurationMs += durMs;
    s.lastDurationMs = durMs;
    s.minDurationMs = Math.min(s.minDurationMs, durMs);
    s.maxDurationMs = Math.max(s.maxDurationMs, durMs);
    s.byStatus[String(res.statusCode)] = (s.byStatus[String(res.statusCode)] ?? 0) + 1;
    if (res.statusCode >= 400) s.errors++;
  });
  next();
}

export function incrementCounter(name: string, by = 1): void {
  counters.set(name, (counters.get(name) ?? 0) + by);
}

export interface MetricsSnapshot {
  timestamp: string;
  uptimeSec: number;
  routes: Array<RouteStats & { route: string; avgDurationMs: number }>;
  counters: Record<string, number>;
  process: {
    memMb: number;
    rssMb: number;
    nodeVersion: string;
  };
}

export function getMetricsSnapshot(): MetricsSnapshot {
  const mem = process.memoryUsage();
  const routes = Array.from(routeStats.entries()).map(([route, s]) => ({
    route,
    ...s,
    minDurationMs: s.minDurationMs === Infinity ? 0 : s.minDurationMs,
    avgDurationMs: s.count > 0 ? s.totalDurationMs / s.count : 0,
  }));
  routes.sort((a, b) => b.count - a.count);
  return {
    timestamp: new Date().toISOString(),
    uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
    routes,
    counters: Object.fromEntries(counters),
    process: {
      memMb: Math.round(mem.heapUsed / 1024 / 1024),
      rssMb: Math.round(mem.rss / 1024 / 1024),
      nodeVersion: process.version,
    },
  };
}
