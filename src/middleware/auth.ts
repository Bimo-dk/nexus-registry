import type { Request, Response, NextFunction } from 'express';
import { cid } from '../correlation.js';

const NEXUS_TOKEN = process.env.NEXUS_TOKEN ?? '';

if (!NEXUS_TOKEN) {
  console.warn('[auth] WARNING: NEXUS_TOKEN is not set — all authenticated endpoints will reject all requests.');
}

export function nexusTokenAuth(req: Request, res: Response, next: NextFunction): void {
  const presented = req.header('X-Nexus-Token');

  if (!presented) {
    console.error(`[auth] [${cid(req)}] Missing X-Nexus-Token on ${req.method} ${req.path}`);
    res.status(401).json({
      error: 'unauthorized',
      message: 'Missing X-Nexus-Token header',
      correlationId: cid(req),
    });
    return;
  }

  if (presented !== NEXUS_TOKEN) {
    console.error(`[auth] [${cid(req)}] Invalid X-Nexus-Token on ${req.method} ${req.path}`);
    res.status(401).json({
      error: 'unauthorized',
      message: 'Invalid X-Nexus-Token',
      correlationId: cid(req),
    });
    return;
  }

  next();
}
