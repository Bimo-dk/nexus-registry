// MIGRATION: Alle typer kommer nu fra @bimo-nexus/core (single source of truth).
// Lokale registry-interne typer (fx RegistryFile som ikke er en del af det offentlige API) bliver.

export type {
  RemoteConfig,
  AddRemoteRequest,
  UpdateRemoteRequest,
  RegistryResponse,
  HealthStatus,
  RemoteHealthStatus,
} from '@bimo-nexus/core';

import type { RemoteConfig } from '@bimo-nexus/core';

/** Internal disk-layout — ikke i det offentlige API, kun registry-intern. */
export interface RegistryFile {
  remotes: RemoteConfig[];
}
