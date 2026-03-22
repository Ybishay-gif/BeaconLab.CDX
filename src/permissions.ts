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
  "leads:view",
  "leads:export",

  // Module pages
  "channel_recommendations:view",
  "channel_recommendations:edit",
  "ad_group_levers:view",
  "ad_group_levers:edit",

  // Cross Tactic
  "budgets:view",
  "budgets:edit",

  // General
  "ai_chat:view",

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
  "tickets:create_module",
  "audit_log:view",
  "usage_analytics:view",
  "sftp_connections:edit",
  "platform_health:view",
] as const;

export type PermissionKey = (typeof ALL_PERMISSIONS)[number];

/** Mutable copy for runtime use */
export const ALL_PERMISSIONS_LIST: string[] = [...ALL_PERMISSIONS];

/** Grouped for the roles UI permission editor */
export const PERMISSION_GROUPS: Record<string, { label: string; permissions: string[] }> = {
  beacon_lite_tactic: {
    label: "Beacon Lite - Tactic",
    permissions: [
      "state_channel_performance:view",
      "price_exploration_analytics:view",
      "leads:view",
      "leads:export",
    ],
  },
  beacon_lite_cross_tactic: {
    label: "Beacon Lite - Cross Tactic",
    permissions: [
      "report_generator:edit",
      "budgets:view",
      "budgets:edit",
    ],
  },
  lm_tools_plan: {
    label: "LM Tools - Plan",
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
      "ad_group_levers:view",
      "ad_group_levers:edit",
    ],
  },
  lm_tools_channel: {
    label: "LM Tools - Channel Rec",
    permissions: [
      "channel_recommendations:view",
      "channel_recommendations:edit",
    ],
  },
  lm_tools_analytics: {
    label: "LM Tools - Analytics",
    permissions: [
      "strategy_analysis:view",
      "plan_comparison:view",
      "state_analytics:view",
    ],
  },
  general: {
    label: "General",
    permissions: [
      "ai_chat:view",
    ],
  },
  kissterra_settings: {
    label: "Kissterra Tools - Settings",
    permissions: [
      "default_targets:edit",
      "user_management:view",
      "user_management:edit",
      "roles_permissions:edit",
      "tickets:view",
      "tickets:add",
      "tickets:approve",
      "tickets:deploy_approve",
      "tickets:create_module",
      "audit_log:view",
      "usage_analytics:view",
      "sftp_connections:edit",
      "platform_health:view",
    ],
  },
};

/** Seed data for system roles */
export const DEFAULT_ROLE_PERMISSIONS: Record<string, string[]> = {
  Admin: [...ALL_PERMISSIONS],
  Planner: [
    // Cross Tactic
    "budgets:view",
    "budgets:edit",
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
    "leads:view",
    "leads:export",
    // Modules — full access
    "channel_recommendations:view",
    "channel_recommendations:edit",
    "ad_group_levers:view",
    "ad_group_levers:edit",
    // General
    "ai_chat:view",
    // Settings — limited
    "report_generator:edit",
    "default_targets:edit",
    "tickets:view",
    "tickets:add",
    "tickets:create_module",
    "audit_log:view",
  ],
  Viewer: [
    "budgets:view",
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
    "leads:view",
    "channel_recommendations:view",
    "ad_group_levers:view",
    "ai_chat:view",
    "tickets:view",
    "audit_log:view",
  ],
};
