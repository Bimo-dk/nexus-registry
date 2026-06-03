/**
 * In-memory ring buffer for recent log entries. Exposed via /api/system/logs
 * and broadcast in real-time via WebSocket. Capacity is bounded so memory
 * usage stays predictable.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogEntry {
  ts: string;
  level: LogLevel;
  source: string;
  message: string;
  correlationId?: string;
  meta?: Record<string, unknown>;
}

const CAPACITY = Number(process.env.LOG_BUFFER_CAPACITY ?? 500);
const buffer: LogEntry[] = [];
let seq = 0;

type Listener = (entry: LogEntry) => void;
const listeners = new Set<Listener>();

export function appendLog(entry: Omit<LogEntry, 'ts'> & { ts?: string }): LogEntry {
  const full: LogEntry = {
    ts: entry.ts ?? new Date().toISOString(),
    level: entry.level,
    source: entry.source,
    message: entry.message,
    correlationId: entry.correlationId,
    meta: entry.meta,
  };
  buffer.push(full);
  seq++;
  while (buffer.length > CAPACITY) buffer.shift();
  for (const fn of listeners) {
    try {
      fn(full);
    } catch (err) {
      // Listener should not block the buffer; swallow but mirror to console
      // eslint-disable-next-line no-console
      console.error('[log-buffer] listener threw:', err);
    }
  }
  return full;
}

export function getLogs(options: { since?: string; limit?: number; level?: LogLevel } = {}): LogEntry[] {
  const limit = Math.max(1, Math.min(options.limit ?? 100, CAPACITY));
  const sinceTs = options.since ? Date.parse(options.since) : 0;
  let filtered = buffer;
  if (sinceTs > 0) {
    filtered = filtered.filter((e) => Date.parse(e.ts) > sinceTs);
  }
  if (options.level) {
    const levels: LogLevel[] = ['debug', 'info', 'warn', 'error'];
    const minIdx = levels.indexOf(options.level);
    filtered = filtered.filter((e) => levels.indexOf(e.level) >= minIdx);
  }
  return filtered.slice(-limit);
}

export function subscribeLogs(fn: Listener): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function getBufferStats(): { capacity: number; size: number; totalAppended: number } {
  return { capacity: CAPACITY, size: buffer.length, totalAppended: seq };
}

/**
 * Patches global console.* so every console call also lands in the ring buffer
 * with the correct level. Original console behavior is preserved (stdout/stderr).
 */
export function captureConsole(): void {
  const original = {
    log: console.log,
    info: console.info,
    warn: console.warn,
    error: console.error,
    debug: console.debug,
  };

  const intercept = (level: LogLevel, fn: (...args: unknown[]) => void) => {
    return (...args: unknown[]): void => {
      fn.apply(console, args);
      try {
        const message = args
          .map((a) => (typeof a === 'string' ? a : safeStringify(a)))
          .join(' ');
        appendLog({ level, source: 'registry', message });
      } catch {
        /* swallow — never let logging break the app */
      }
    };
  };

  console.log = intercept('info', original.log);
  console.info = intercept('info', original.info);
  console.warn = intercept('warn', original.warn);
  console.error = intercept('error', original.error);
  console.debug = intercept('debug', original.debug);
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
