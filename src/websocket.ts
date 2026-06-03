import { WebSocketServer, WebSocket } from 'ws';
import type { Server as HttpServer } from 'node:http';
import { getAllRemotes } from './store.js';
import { subscribeLogs, type LogEntry } from './observability/log-buffer.js';

type ServerMessage =
  | { type: 'welcome'; timestamp: string; clients: number }
  | { type: 'remotes_changed'; timestamp: string; remotes: unknown[]; trigger: string }
  | { type: 'system_health'; timestamp: string; snapshot: unknown }
  | { type: 'log'; entry: LogEntry }
  | { type: 'pong'; timestamp: string };

const WS_PATH = '/ws';

const clients = new Set<WebSocket>();
const logSubscribers = new Set<WebSocket>();
let wss: WebSocketServer | null = null;

export function attachWebSocketServer(server: HttpServer): void {
  wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (req, socket, head) => {
    const url = req.url ?? '';
    if (!url.startsWith(WS_PATH)) {
      socket.destroy();
      return;
    }
    wss!.handleUpgrade(req, socket, head, (ws) => {
      wss!.emit('connection', ws, req);
    });
  });

  // Fan out log entries to subscribed clients
  subscribeLogs((entry) => {
    if (logSubscribers.size === 0) return;
    const data = JSON.stringify({ type: 'log', entry } satisfies ServerMessage);
    for (const ws of logSubscribers) {
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    }
  });

  wss.on('connection', (ws) => {
    clients.add(ws);

    const welcome: ServerMessage = {
      type: 'welcome',
      timestamp: new Date().toISOString(),
      clients: clients.size,
    };
    safeSend(ws, welcome);

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as { type?: string; subscribe?: string };
        if (msg.type === 'ping') {
          safeSend(ws, { type: 'pong', timestamp: new Date().toISOString() });
        } else if (msg.type === 'subscribe' && msg.subscribe === 'logs') {
          logSubscribers.add(ws);
        } else if (msg.type === 'unsubscribe' && msg.subscribe === 'logs') {
          logSubscribers.delete(ws);
        }
      } catch {
        // ignore malformed
      }
    });

    ws.on('close', () => {
      clients.delete(ws);
      logSubscribers.delete(ws);
    });

    ws.on('error', () => {
      clients.delete(ws);
      logSubscribers.delete(ws);
    });
  });
}

export async function broadcastRemotesChanged(trigger: string): Promise<void> {
  if (clients.size === 0) return;
  try {
    const remotes = await getAllRemotes();
    const msg: ServerMessage = {
      type: 'remotes_changed',
      timestamp: new Date().toISOString(),
      remotes,
      trigger,
    };
    const data = JSON.stringify(msg);
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) client.send(data);
    }
  } catch {
    // swallow — broadcasting failure should not break the request that triggered it
  }
}

function safeSend(ws: WebSocket, msg: ServerMessage): void {
  try {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  } catch {
    // ignore
  }
}

export function getConnectionCount(): number {
  return clients.size;
}

export function broadcastSystemHealth(snapshot: unknown): void {
  if (clients.size === 0) return;
  const msg: ServerMessage = {
    type: 'system_health',
    timestamp: new Date().toISOString(),
    snapshot,
  };
  const data = JSON.stringify(msg);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}
