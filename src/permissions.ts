// Static registry of all permission keys.
// Permission format: "resource:action"

export const ALL_PERMISSIONS = [
  // Plan pages
  "plan_builder:view",
  "plan_builder:edit",
  "targets:view",
  "targets:edit",
  "price_exploration_plan:view",
  "price_exploration_plan:edit",
  "plan_strategy:view",
  "plan_strategy:edit",
  "plan_outcome:view",
  "plan_outcome:edit",

  // Analytics
  "state_channel_performance:view",
  "price_exploration_analytics:view",
  "strategy_analysis:view",
  "plan_comparison:view",
  "state_analytics:view",

  // Module pages
  "channel_recommendations:view",
  "channel_recommendations:edit",
  "ad_group_levers:view",
  "ad_group_levers:edit",

  // Settings
  "report_generator:edit",
  "default_targets:edit",
  "user_management:view",
  "user_management:edit",
  "roles_permissions:edit",
  "tickets:view",
  "tickets:add",
  "tickets:approve",
  "tickets:deploy_approve",
  "audit_log:view",
  "usage_analytics:view",
  "sftp_connections:edit",
] as const;

export type PermissionKey = (typeof ALL_PERMISSIONS)[number];

/** Mutable copy for runtime use */
export const ALL_PERMISSIONS_LIST: string[] = [...ALL_PERMISSIONS];

/** Grouped for the roles UI permission editor */
export const PERMISSION_GROUPS: Record<string, { label: string; permissions: string[] }> = {
  plan: {
    label: "Plan",
    permissions: [
      "plan_builder:view",
      "plan_builder:edit",
      "targets:view",
      "targets:edit",
      "plan_strategy:view",
      "plan_strategy:edit",
      "price_exploration_plan:view",
      "price_exploration_plan:edit",
      "plan_outcome:view",
      "plan_outcome:edit",
    ],
  },
  analytics: {
    label: "Analytics",
    permissions: [
      "state_channel_performance:view",
      "price_exploration_analytics:view",
      "strategy_analysis:view",
      "plan_comparison:view",
      "state_analytics:view",
    ],
  },
  modules: {
    label: "Modules",
    permissions: [
      "channel_recommendations:view",
      "channel_recommendations:edit",
      "ad_group_levers:view",
      "ad_group_levers:edit",
    ],
  },
  settings: {
    label: "Settings",
    permissions: [
      "report_generator:edit",
      "default_targets:edit",
      "user_management:view",
      "user_management:edit",
      "roles_permissions:edit",
      "tickets:view",
      "tickets:add",
      "tickets:approve",
      "tickets:deploy_approve",
      "audit_log:view",
      "usage_analytics:view",
      "sftp_connections:edit",
    ],
  },
};

/** Seed data for system roles */
export const DEFAULT_ROLE_PERMISSIONS: Record<string, string[]> = {
  Admin: [...ALL_PERMISSIONS],
  Planner: [
    // Plan — full access
    "plan_builder:view",
    "plan_builder:edit",
    "targets:view",
    "targets:edit",
    "plan_strategy:view",
    "plan_strategy:edit",
    "price_exploration_plan:view",
    "price_exploration_plan:edit",
    "plan_outcome:view",
    "plan_outcome:edit",
    // Analytics — view all
    "state_channel_performance:view",
    "price_exploration_analytics:view",
    "strategy_analysis:view",
    "plan_comparison:view",
    "state_analytics:view",
    // Modules — full access
    "channel_recommendations:view",
    "channel_recommendations:edit",
    "ad_group_levers:view",
    "ad_group_levers:edit",
    // Settings — limited
    "report_generator:edit",
    "default_targets:edit",
    "tickets:view",
    "tickets:add",
    "audit_log:view",
  ],
  Viewer: [
    "plan_builder:view",
    "targets:view",
    "plan_strategy:view",
    "price_exploration_plan:view",
    "plan_outcome:view",
    "state_channel_performance:view",
    "price_exploration_analytics:view",
    "strategy_analysis:view",
    "plan_comparison:view",
    "state_analytics:view",
    "channel_recommendations:view",
    "ad_group_levers:view",
    "tickets:view",
    "audit_log:view",
  ],
};
