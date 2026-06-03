import { WebSocketServer, WebSocket } from 'ws';
import type { Server as HttpServer } from 'node:http';
import { getAllRemotes } from './store.js';

type ServerMessage =
  | { type: 'welcome'; timestamp: string; clients: number }
  | { type: 'remotes_changed'; timestamp: string; remotes: unknown[]; trigger: string }
  | { type: 'pong'; timestamp: string };

const WS_PATH = '/ws';

const clients = new Set<WebSocket>();
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

  wss.on('connection', (ws) => {
    clients.add(ws);
    console.log(`[ws] Client connected (total: ${clients.size})`);

    const welcome: ServerMessage = {
      type: 'welcome',
      timestamp: new Date().toISOString(),
      clients: clients.size,
    };
    safeSend(ws, welcome);

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as { type?: string };
        if (msg.type === 'ping') {
          safeSend(ws, { type: 'pong', timestamp: new Date().toISOString() });
        }
      } catch {
        // ignore malformed
      }
    });

    ws.on('close', () => {
      clients.delete(ws);
      console.log(`[ws] Client disconnected (total: ${clients.size})`);
    });

    ws.on('error', (err) => {
      console.error('[ws] Client error:', err.message);
      clients.delete(ws);
    });
  });

  console.log(`[ws] WebSocket server attached at ${WS_PATH}`);
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
    let sent = 0;
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data, (err) => {
          if (err) console.error('[ws] Send error:', err.message);
        });
        sent++;
      }
    }
    console.log(`[ws] Broadcast remotes_changed (${trigger}) to ${sent} client(s)`);
  } catch (err) {
    console.error('[ws] Failed to broadcast:', err);
  }
}

function safeSend(ws: WebSocket, msg: ServerMessage): void {
  try {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  } catch (err) {
    console.error('[ws] Failed to send:', err);
  }
}

export function getConnectionCount(): number {
  return clients.size;
}
