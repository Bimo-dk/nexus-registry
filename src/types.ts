// MIGRATION: All types now come from @bimo-dk/nexus-core (single source of truth).
// Registry-internal types (e.g. RegistryFile which is not part of the public API) remain here.

export type {
  RemoteConfig,
  AddRemoteRequest,
  UpdateRemoteRequest,
  RegistryResponse,
  HealthStatus,
  RemoteHealthStatus,
} from '@bimo-dk/nexus-core';

import type { RemoteConfig } from '@bimo-dk/nexus-core';

/** Internal disk layout — not part of the public API, registry-internal only. */
export interface RegistryFile {
  remotes: RemoteConfig[];
}
