// Static registry of known modules.
// Adding a module requires code changes (routes, screens), so the registry belongs in code.
// Dynamic modules (from ticket system) are stored in DB and merged at runtime.

import { query, table } from "./db/index.js";

export interface ModuleDefinition {
  id: string;
  label: string;
  defaultRoute: string;
  isDynamic?: boolean;
  isActive?: boolean;
  ticketId?: string;
}

export const MODULE_REGISTRY: ModuleDefinition[] = [
  { id: "planning", label: "Planning", defaultRoute: "/plan/builder" },
  { id: "channel_recommendations", label: "Channel Recommendations", defaultRoute: "/channel-rec" },
];

export const VALID_MODULE_IDS = MODULE_REGISTRY.map((m) => m.id);

export function isValidModuleId(id: string): boolean {
  return VALID_MODULE_IDS.includes(id);
}

/** Fetch dynamic modules from DB and merge with static registry */
export async function getAllModules(): Promise<ModuleDefinition[]> {
  try {
    const dynamicRows = await query<{
      module_id: string;
      label: string;
      default_route: string | null;
      is_active: boolean;
      ticket_id: string | null;
    }>(`SELECT module_id, label, default_route, is_active, ticket_id FROM ${table("dynamic_modules")}`);

    const dynamicModules: ModuleDefinition[] = dynamicRows.map((r) => ({
      id: r.module_id,
      label: r.label,
      defaultRoute: r.default_route ?? "",
      isDynamic: true,
      isActive: r.is_active,
      ticketId: r.ticket_id ?? undefined,
    }));

    // Merge: static first, then dynamic (skip duplicates)
    const staticIds = new Set(MODULE_REGISTRY.map((m) => m.id));
    return [...MODULE_REGISTRY, ...dynamicModules.filter((d) => !staticIds.has(d.id))];
  } catch {
    // If dynamic_modules table doesn't exist yet, return static only
    return [...MODULE_REGISTRY];
  }
}
