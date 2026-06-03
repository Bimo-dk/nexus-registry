import { randomBytes } from 'node:crypto';
import type { Request, Response, NextFunction } from 'express';

declare module 'express-serve-static-core' {
  interface Request {
    correlationId?: string;
  }
}

export const CORRELATION_HEADER = 'X-Request-ID';

function generateId(): string {
  return `reg-${randomBytes(6).toString('hex')}`;
}

export function correlationMiddleware(req: Request, res: Response, next: NextFunction): void {
  const incoming = req.header(CORRELATION_HEADER);
  const id = (incoming && incoming.trim()) || generateId();
  req.correlationId = id;
  res.setHeader(CORRELATION_HEADER, id);
  next();
}

export function cid(req: Request): string {
  return req.correlationId ?? '<no-correlation-id>';
}
