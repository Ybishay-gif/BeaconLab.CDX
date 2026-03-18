/**
 * Static module/page list — mirrors the frontend config (beacon-lab-v2/src/config/modules.ts).
 * Used by the AI chat ticket tools and system prompt.
 */

export interface ModulePageInfo {
  moduleId: string;
  moduleLabel: string;
  pages: string[];
}

export const MODULE_PAGES: ModulePageInfo[] = [
  {
    moduleId: "planning",
    moduleLabel: "Planning",
    pages: [
      "Plan Builder",
      "Targets",
      "Plan Strategy",
      "Price Exploration",
      "Plan Outcome",
      "Ad Levers",
      "State & Channel Perf",
      "Price Exploration",
      "Strategy Analysis",
      "Plans Comparison",
      "State Analytics",
      "Report Generator",
    ],
  },
  {
    moduleId: "channel_recommendations",
    moduleLabel: "Channel Recommendations",
    pages: ["Channel Params"],
  },
  {
    moduleId: "settings",
    moduleLabel: "Settings",
    pages: [
      "Default Targets",
      "User Management",
      "Tickets",
      "Audit Log",
      "Usage Analytics",
      "SFTP Connections",
    ],
  },
];

/** Format as a readable string for the system prompt */
export function formatModulePagesForPrompt(): string {
  return MODULE_PAGES.map(
    (m) => `- **${m.moduleLabel}** (moduleId: "${m.moduleId}"): ${m.pages.join(", ")}`
  ).join("\n");
}
