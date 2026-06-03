import http from 'node:http';
import express, { type ErrorRequestHandler, type Request, type Response } from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { nexusTokenAuth } from './middleware/auth.js';
import { correlationMiddleware, CORRELATION_HEADER, cid } from './correlation.js';
import { remotesRouter } from './routes/remotes.js';
import { systemRouter } from './routes/system.js';
import { loadRegistry } from './store.js';
import { attachWebSocketServer, getConnectionCount } from './websocket.js';
import { startHealthCheckLoop } from './system-health.js';
import { captureConsole } from './observability/log-buffer.js';
import { metricsMiddleware } from './observability/metrics.js';

// Capture all console.* output into the ring buffer for /api/system/logs.
// Must run before any other module logs.
captureConsole();

const PORT = Number(process.env.PORT ?? 3000);
const HEALTH_INTERVAL_MS = Number(process.env.HEALTH_CHECK_INTERVAL_MS ?? 30_000);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS ?? '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const app = express();

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.length === 0 || ALLOWED_ORIGINS.includes('*')) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      return callback(new Error(`Origin ${origin} not allowed by CORS`));
    },
    credentials: false,
    allowedHeaders: ['Content-Type', 'X-Nexus-Token', CORRELATION_HEADER],
    exposedHeaders: [CORRELATION_HEADER],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  }),
);

app.use(correlationMiddleware);
app.use(metricsMiddleware);
app.use(express.json({ limit: '1mb' }));
app.use(morgan(process.env.NODE_ENV === 'production' ? 'combined' : 'dev'));

app.get('/health', (_req: Request, res: Response) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    service: 'nexus-registry',
    wsClients: getConnectionCount(),
  });
});

app.use('/api/remotes', nexusTokenAuth, remotesRouter);
app.use('/api/system', nexusTokenAuth, systemRouter);

app.use((req: Request, res: Response) => {
  res.status(404).json({ error: 'not_found', message: 'Route not found', correlationId: cid(req) });
});

const errorHandler: ErrorRequestHandler = (err, req, res, _next) => {
  console.error(`[registry] [${cid(req)}] Unhandled error:`, err);
  res.status(500).json({
    error: 'internal_server_error',
    message: err instanceof Error ? err.message : 'Unknown error',
    correlationId: cid(req),
  });
};
app.use(errorHandler);

async function start(): Promise<void> {
  try {
    const initial = await loadRegistry();
    console.log(`[registry] Loaded ${initial.remotes.length} remote(s) from disk`);
  } catch (err) {
    console.error('[registry] Failed to load registry.json on startup:', err);
    process.exit(1);
  }

  const server = http.createServer(app);
  attachWebSocketServer(server);

  server.listen(PORT, () => {
    console.log(`[registry] Listening on http://0.0.0.0:${PORT}`);
    console.log(`[registry] WebSocket on ws://0.0.0.0:${PORT}/ws`);
    console.log(`[registry] Allowed CORS origins: ${ALLOWED_ORIGINS.join(', ') || '(any)'}`);
    startHealthCheckLoop(HEALTH_INTERVAL_MS);
  });
}

start();
