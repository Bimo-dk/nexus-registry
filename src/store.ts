import { promises as fs } from 'node:fs';
import path from 'node:path';
import type { RegistryFile, RemoteConfig } from './types.js';

const DATA_DIR = path.resolve(process.cwd(), 'data');
const REGISTRY_FILE = path.join(DATA_DIR, 'registry.json');

let writeLock: Promise<void> = Promise.resolve();

async function ensureDataFile(): Promise<void> {
  try {
    await fs.access(REGISTRY_FILE);
  } catch {
    await fs.mkdir(DATA_DIR, { recursive: true });
    const seed: RegistryFile = { remotes: [] };
    await fs.writeFile(REGISTRY_FILE, JSON.stringify(seed, null, 2), 'utf8');
    console.log(`[store] Initialized empty registry at ${REGISTRY_FILE}`);
  }
}

export async function loadRegistry(): Promise<RegistryFile> {
  await ensureDataFile();
  const raw = await fs.readFile(REGISTRY_FILE, 'utf8');
  const parsed = JSON.parse(raw) as RegistryFile;
  if (!Array.isArray(parsed.remotes)) {
    throw new Error('Corrupted registry.json: missing "remotes" array');
  }
  return parsed;
}

async function persistRegistry(data: RegistryFile): Promise<void> {
  const tmp = `${REGISTRY_FILE}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(data, null, 2), 'utf8');
  await fs.rename(tmp, REGISTRY_FILE);
}

export function withWriteLock<T>(task: () => Promise<T>): Promise<T> {
  const result = writeLock.then(task, task);
  writeLock = result.then(() => undefined, () => undefined);
  return result;
}

export async function getAllRemotes(): Promise<RemoteConfig[]> {
  const data = await loadRegistry();
  return data.remotes;
}

export async function getRemote(name: string): Promise<RemoteConfig | null> {
  const data = await loadRegistry();
  return data.remotes.find((r) => r.name === name) ?? null;
}

export async function addRemote(remote: RemoteConfig): Promise<RemoteConfig> {
  return withWriteLock(async () => {
    const data = await loadRegistry();
    if (data.remotes.some((r) => r.name === remote.name)) {
      throw new RegistryConflictError(`Remote "${remote.name}" already exists`);
    }
    data.remotes.push(remote);
    await persistRegistry(data);
    return remote;
  });
}

export async function updateRemote(
  name: string,
  patch: Partial<RemoteConfig>,
): Promise<RemoteConfig | null> {
  return withWriteLock(async () => {
    const data = await loadRegistry();
    const idx = data.remotes.findIndex((r) => r.name === name);
    if (idx === -1) return null;
    const updated: RemoteConfig = { ...data.remotes[idx], ...patch, name };
    data.remotes[idx] = updated;
    await persistRegistry(data);
    return updated;
  });
}

export async function deleteRemote(name: string): Promise<boolean> {
  return withWriteLock(async () => {
    const data = await loadRegistry();
    const before = data.remotes.length;
    data.remotes = data.remotes.filter((r) => r.name !== name);
    if (data.remotes.length === before) return false;
    await persistRegistry(data);
    return true;
  });
}

export async function toggleRemote(name: string): Promise<RemoteConfig | null> {
  return withWriteLock(async () => {
    const data = await loadRegistry();
    const idx = data.remotes.findIndex((r) => r.name === name);
    if (idx === -1) return null;
    data.remotes[idx].enabled = !data.remotes[idx].enabled;
    await persistRegistry(data);
    return data.remotes[idx];
  });
}

export class RegistryConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RegistryConflictError';
  }
}
