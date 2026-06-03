// MIGRATION: Alle typer kommer nu fra @bimo-dk/nexus-core (single source of truth).
// Lokale registry-interne typer (fx RegistryFile som ikke er en del af det offentlige API) bliver.

export type {
  RemoteConfig,
  AddRemoteRequest,
  UpdateRemoteRequest,
  RegistryResponse,
  HealthStatus,
  RemoteHealthStatus,
} from '@bimo-dk/nexus-core';

import type { RemoteConfig } from '@bimo-dk/nexus-core';

/** Internal disk-layout — ikke i det offentlige API, kun registry-intern. */
export interface RegistryFile {
  remotes: RemoteConfig[];
}
