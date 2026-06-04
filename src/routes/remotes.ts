import { Router, type Request, type Response } from 'express';
import { isValidRemoteName, isValidRoutePath, isValidUrlOrPath } from '@bimo-dk/nexus-core';
import {
  addRemote,
  deleteRemote,
  getAllRemotes,
  getRemote,
  RegistryConflictError,
  toggleRemote,
  updateRemote,
} from '../store.js';
import { cid } from '../correlation.js';
import { broadcastRemotesChanged } from '../websocket.js';
import type { AddRemoteRequest, RemoteConfig, UpdateRemoteRequest } from '../types.js';

export const remotesRouter = Router();

remotesRouter.get('/', async (_req: Request, res: Response) => {
  const remotes = await getAllRemotes();
  res.json({
    remotes,
    total: remotes.length,
    enabled: remotes.filter((r) => r.enabled).length,
  });
});

remotesRouter.get('/:name', async (req: Request, res: Response) => {
  const name = String(req.params['name']);
  const remote = await getRemote(name);
  if (!remote) {
    res.status(404).json({ error: 'not_found', message: `Remote "${name}" not found`, correlationId: cid(req) });
    return;
  }
  res.json(remote);
});

remotesRouter.post('/', async (req: Request, res: Response) => {
  const body = req.body as Partial<AddRemoteRequest> | undefined;
  if (!body || typeof body !== 'object') {
    res.status(400).json({ error: 'invalid_body', message: 'JSON body required', correlationId: cid(req) });
    return;
  }

  const { name, url, routePath, upstreamUrl } = body;
  const exposedModule = body.exposedModule ?? './RemoteEntry';
  const enabled = body.enabled ?? true;

  const validationError = validateNewRemote({ name, url, routePath, exposedModule });
  if (validationError) {
    console.error(`[remotes] [${cid(req)}] POST validation failed: ${validationError}`);
    res.status(400).json({ error: 'validation_failed', message: validationError, correlationId: cid(req) });
    return;
  }

  const newRemote: RemoteConfig = {
    name: name!,
    url: url!,
    exposedModule,
    routePath: routePath!,
    enabled,
    addedAt: new Date().toISOString(),
    ...(upstreamUrl ? { upstreamUrl } : {}),
  };

  try {
    const created = await addRemote(newRemote);
    res.status(201).json(created);
    broadcastRemotesChanged(`add:${created.name}`);
  } catch (err) {
    if (err instanceof RegistryConflictError) {
      console.error(`[remotes] [${cid(req)}] POST conflict: ${err.message}`);
      res.status(409).json({ error: 'conflict', message: err.message, correlationId: cid(req) });
      return;
    }
    throw err;
  }
});

remotesRouter.put('/:name', async (req: Request, res: Response) => {
  const name = String(req.params['name']);
  const body = req.body as UpdateRemoteRequest | undefined;
  if (!body || typeof body !== 'object') {
    res.status(400).json({ error: 'invalid_body', message: 'JSON body required', correlationId: cid(req) });
    return;
  }

  if (body.routePath !== undefined && !isValidRoutePath(body.routePath)) {
    res.status(400).json({ error: 'validation_failed', message: 'routePath must be kebab-case starting with a lowercase letter', correlationId: cid(req) });
    return;
  }
  if (body.url !== undefined && !isValidUrlOrPath(body.url)) {
    res.status(400).json({ error: 'validation_failed', message: 'url must be a valid http(s) URL or absolute path', correlationId: cid(req) });
    return;
  }

  const updated = await updateRemote(name, body);
  if (!updated) {
    res.status(404).json({ error: 'not_found', message: `Remote "${name}" not found`, correlationId: cid(req) });
    return;
  }
  res.json(updated);
  broadcastRemotesChanged(`update:${updated.name}`);
});

remotesRouter.delete('/:name', async (req: Request, res: Response) => {
  const name = String(req.params['name']);
  const removed = await deleteRemote(name);
  if (!removed) {
    res.status(404).json({ error: 'not_found', message: `Remote "${name}" not found`, correlationId: cid(req) });
    return;
  }
  res.status(204).send();
  broadcastRemotesChanged(`delete:${name}`);
});

remotesRouter.post('/:name/toggle', async (req: Request, res: Response) => {
  const name = String(req.params['name']);
  const updated = await toggleRemote(name);
  if (!updated) {
    res.status(404).json({ error: 'not_found', message: `Remote "${name}" not found`, correlationId: cid(req) });
    return;
  }
  res.json(updated);
  broadcastRemotesChanged(`toggle:${updated.name}`);
});

remotesRouter.post('/:name/redeploy', async (req: Request, res: Response) => {
  const name = String(req.params['name']);
  const remote = await getRemote(name);
  if (!remote) {
    res.status(404).json({ error: 'not_found', message: `Remote "${name}" not found`, correlationId: cid(req) });
    return;
  }
  console.log(`[registry] [${cid(req)}] Redeploy signal for "${remote.name}" at ${new Date().toISOString()}`);
  res.status(202).json({
    accepted: true,
    remote: remote.name,
    timestamp: new Date().toISOString(),
    correlationId: cid(req),
    note: 'Redeploy is logged. Container orchestration (Docker Swarm/K8s) is responsible for actually redeploying.',
  });
});

function validateNewRemote(input: {
  name: string | undefined;
  url: string | undefined;
  routePath: string | undefined;
  exposedModule: string;
}): string | null {
  if (!input.name || !isValidRemoteName(input.name)) {
    return 'name must be camelCase starting with a lowercase letter';
  }
  if (!input.url || !isValidUrlOrPath(input.url)) {
    return 'url must be a valid http(s) URL or absolute path (starting with /)';
  }
  if (!input.routePath || !isValidRoutePath(input.routePath)) {
    return 'routePath must be kebab-case starting with a lowercase letter';
  }
  if (!input.exposedModule.startsWith('./')) {
    return 'exposedModule must start with "./"';
  }
  return null;
}
