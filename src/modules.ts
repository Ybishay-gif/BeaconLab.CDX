// Static registry of known modules.
// Adding a module requires code changes (routes, screens), so the registry belongs in code.

export interface ModuleDefinition {
  id: string;
  label: string;
  defaultRoute: string;
}

export const MODULE_REGISTRY: ModuleDefinition[] = [
  { id: "planning", label: "Planning", defaultRoute: "/plan/builder" },
  { id: "channel_recommendations", label: "Channel Recommendations", defaultRoute: "/channel-rec" },
  { id: "ad_group_levers", label: "Ad Group Levers", defaultRoute: "/ad-group-levers" },
];

export const VALID_MODULE_IDS = MODULE_REGISTRY.map((m) => m.id);

export function isValidModuleId(id: string): boolean {
  return VALID_MODULE_IDS.includes(id);
}
