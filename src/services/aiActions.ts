/**
 * AI Chat Actions — Gemini Function Calling tools for platform actions.
 *
 * Defines tool declarations (for Gemini) and execution handlers
 * for actions like generating reports.
 */

import {
  SchemaType,
  type FunctionDeclarationsTool,
} from "@google/generative-ai";
import { createReport, getTableSchema, getFilterValues, type CreateReportInput } from "./reportService.js";
import { getPriceExploration, type PriceExplorationFilters, type PriceExplorationRow } from "./analyticsService.js";
import { getAdLeverData } from "./adLeverService.js";
import { createTicket, listTickets, getTicket, updateTicket, STATUS_TRANSITIONS, type Attachment, type TicketStatus } from "./ticketsService.js";
import { MODULE_PAGES, formatModulePagesForPrompt } from "./modulePages.js";
import { query, table } from "../db/index.js";

/* ------------------------------------------------------------------ */
/*  Tool declarations — passed to Gemini so it knows what it can call  */
/* ------------------------------------------------------------------ */

export const ACTION_TOOLS: FunctionDeclarationsTool = {
  functionDeclarations: [
    {
      name: "list_available_actions",
      description:
        "List all actions the AI assistant can perform on the platform. Call this when the user asks what you can do, what actions are available, or wants help.",
    },
    {
      name: "lookup_filter_values",
      description:
        "Look up valid values for a report filter column. Use this to verify or find correct account names, channel names, states, or any other filterable column before generating a report. Supports fuzzy matching — pass a search term to find close matches. Common columns: account_name, attribution_channel, data_state, transaction_sold.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          column_name: {
            type: SchemaType.STRING,
            description: "The column to look up values for (e.g. 'account_name', 'attribution_channel', 'data_state')",
          },
          search: {
            type: SchemaType.STRING,
            description: "Optional search term to filter results. Returns values containing this term (case-insensitive). E.g. 'QS' to find accounts containing 'QS'.",
          },
        },
        required: ["column_name"],
      },
    },
    {
      name: "list_report_columns",
      description:
        "List all available columns for the Cross Tactic Analysis report. Use this when the user asks what data/columns are available, or wants to select specific columns for their report.",
    },
    {
      name: "generate_report",
      description:
        "Generate a custom data report (CSV) from the Cross Tactic Analysis data. The report will be created asynchronously and the user can download it from the Reports page. Always confirm the parameters with the user before calling this function.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          report_name: {
            type: SchemaType.STRING,
            description: "A descriptive name for the report",
          },
          date_start: {
            type: SchemaType.STRING,
            description: "Start date in YYYY-MM-DD format. Defaults to 30 days ago if not specified.",
          },
          date_end: {
            type: SchemaType.STRING,
            description: "End date in YYYY-MM-DD format. Defaults to today if not specified.",
          },
          states: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by state abbreviations (e.g. ['MA', 'TX']). Leave empty for all states.",
          },
          channels: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by attribution channels. Leave empty for all channels.",
          },
          accounts: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by account names. Leave empty for all accounts.",
          },
          include_unsold: {
            type: SchemaType.BOOLEAN,
            description: "Whether to include unsold transactions. Defaults to true.",
          },
          selected_columns: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Specific columns to include in the report. Use list_report_columns to see available columns. Leave empty for all columns.",
          },
        },
        required: ["report_name"],
      },
    },
    {
      name: "get_ad_lever_data",
      description:
        "Fetch pre-computed Ad Group Lever scores for all state+segment combinations. Each row has a lever score (1–10) plus component scores: COR, Q2B, Win Rate, Retention, QLTV, Strategy, and a final_score average. ALWAYS use this tool when the user asks: what is the lever for a state/segment, which states have a lever of X, how was the lever calculated for a state/segment, show me lever scores, or any question about ad group levers. The data is automatically scoped to the active plan's date range and activity type.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          states: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by state abbreviations (e.g. ['MA', 'TX']). Leave empty for all states.",
          },
          segments: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by segments (e.g. ['MCH', 'SCR']). Leave empty for all segments.",
          },
          min_lever: {
            type: SchemaType.NUMBER,
            description: "Return only rows where lever >= this value. E.g. 8 to find high-lever opportunities.",
          },
          start_date: {
            type: SchemaType.STRING,
            description: "Override start date in YYYY-MM-DD format. If the user mentions a different time period (e.g. 'look at January', 'last month'), use this to override the plan's default date range.",
          },
          end_date: {
            type: SchemaType.STRING,
            description: "Override end date in YYYY-MM-DD format. Use together with start_date to change the analysis period.",
          },
        },
      },
    },
    {
      name: "get_price_exploration_data",
      description:
        "Fetch pre-computed Price Exploration (PE) data with recommended testing points. This tool runs the full PE engine which includes stat-sig classification, blended channel uplifts, funnel rates (quote rate, Q2B), expected bind changes, additional budget calculations, CPB projections, and weighted scoring for testing point selection based on strategy rules. ALWAYS use this tool instead of writing SQL when the user asks about: price exploration, recommended testing points, additional binds/clicks from PE, budget allocation based on PE, CPB projections, win rate uplifts, or any PE-related analysis. The data is automatically scoped to the active plan's date range and activity type.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          states: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by state abbreviations (e.g. ['MA', 'TX']). Leave empty for all states in the plan.",
          },
          channel_groups: {
            type: SchemaType.ARRAY,
            items: { type: SchemaType.STRING },
            description: "Filter by channel group names. Leave empty for all channels.",
          },
          include_all_testing_points: {
            type: SchemaType.BOOLEAN,
            description: "If true, return ALL testing points for each state+channel pair. If false (default), return only the recommended testing point rows. Set to true when the user wants to compare different testing points or see the full PE data.",
          },
          top_pairs: {
            type: SchemaType.NUMBER,
            description: "Return only the top N state+channel pairs ranked by expected bind change. Useful for budget allocation questions. Leave empty for all pairs.",
          },
        },
      },
    },
    /* ---- Ticket tools ---- */
    {
      name: "create_ticket",
      description:
        "Create a bug report or feature request ticket. ONLY call this after you have gathered ALL required information from the user through conversation: type (bug/feature), title, description, module, and page. Attachments (screenshots, files) are handled separately by the frontend — tell the user to use the camera or paperclip buttons in the input area before confirming. Always show a summary and get explicit confirmation before calling this tool.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          type: {
            type: SchemaType.STRING,
            description: "Ticket type: 'bug' or 'feature'",
          },
          title: {
            type: SchemaType.STRING,
            description: "Brief summary of the issue or request (max 200 characters)",
          },
          description: {
            type: SchemaType.STRING,
            description: "Detailed description of the bug or feature request (max 5000 characters)",
          },
          module: {
            type: SchemaType.STRING,
            description: "The moduleId where the issue occurs (e.g. 'planning', 'channel_recommendations', 'settings'). Use list_modules_and_pages if unsure.",
          },
          page: {
            type: SchemaType.STRING,
            description: "The page name where the issue occurs (e.g. 'Plan Builder', 'Targets'). Use list_modules_and_pages if unsure.",
          },
        },
        required: ["type", "title", "description", "module", "page"],
      },
    },
    {
      name: "list_my_tickets",
      description:
        "List tickets created by the current user. Use when the user asks about their tickets, open bugs, feature requests, or ticket history.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          status: {
            type: SchemaType.STRING,
            description: "Filter by status (e.g. 'todo', 'pending_spec', 'deployed', 'done'). Leave empty for all statuses.",
          },
          type: {
            type: SchemaType.STRING,
            description: "Filter by type: 'bug' or 'feature'. Leave empty for both.",
          },
          limit: {
            type: SchemaType.NUMBER,
            description: "Max tickets to return (default 10).",
          },
        },
      },
    },
    {
      name: "get_ticket_status",
      description:
        "Get the full details and current status of a specific ticket by its TKT number. Use when the user asks about a specific ticket like 'What is the status of TKT-42?'.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          ticket_number: {
            type: SchemaType.NUMBER,
            description: "The ticket number (e.g. 42 for TKT-42)",
          },
        },
        required: ["ticket_number"],
      },
    },
    {
      name: "update_ticket_status",
      description:
        "Update the status of a ticket. Validates that the transition is allowed by the workflow. Requires appropriate permissions — ticket owners can advance their own tickets, admins can approve. Always confirm the new status with the user before calling.",
      parameters: {
        type: SchemaType.OBJECT,
        properties: {
          ticket_number: {
            type: SchemaType.NUMBER,
            description: "The ticket number (e.g. 42 for TKT-42)",
          },
          new_status: {
            type: SchemaType.STRING,
            description: "The new status to transition to. Valid statuses: todo, pending_spec, pending_spec_approval, spec_approved, adjusted_spec, pending_deployment, deployment_approved, deployed, done, reopened",
          },
        },
        required: ["ticket_number", "new_status"],
      },
    },
    {
      name: "list_modules_and_pages",
      description:
        "List all available modules and their pages in the platform. Use this when helping a user create a ticket and you need to confirm the correct module and page name.",
    },
  ],
};

/* ------------------------------------------------------------------ */
/*  Action result types                                                */
/* ------------------------------------------------------------------ */

export interface ActionResult {
  /** The data to send back to Gemini as function response */
  response: Record<string, unknown>;
  /** Metadata sent to the frontend for rendering action UI */
  action?: {
    type: "report_created" | "action_list" | "ticket_created" | "ticket_list" | "ticket_detail" | "ticket_updated";
    payload: unknown;
  };
}

/** Plan context passed from the AI chat handler for tools that need plan-scoped data */
export interface ActionPlanContext {
  planId?: string;
  activityLeadType?: string;
  perfStartDate?: string;
  perfEndDate?: string;
  priceStartDate?: string;
  priceEndDate?: string;
  qbcClicks?: number;
  qbcLeadsCalls?: number;
}

/* ------------------------------------------------------------------ */
/*  Execution handlers                                                 */
/* ------------------------------------------------------------------ */

/** User context for tools that need permissions or identity */
export interface ActionUser {
  userId: string;
  email: string;
  permissions: string[];
}

export async function executeAction(
  actionName: string,
  args: Record<string, unknown>,
  user: ActionUser,
  planContext?: ActionPlanContext,
  attachments?: Attachment[],
): Promise<ActionResult> {
  switch (actionName) {
    case "list_available_actions":
      return handleListActions();
    case "lookup_filter_values":
      return await handleLookupFilterValues(args);
    case "list_report_columns":
      return await handleListReportColumns();
    case "generate_report":
      return await handleGenerateReport(args, user.userId);
    case "get_ad_lever_data":
      return await handleGetAdLeverData(args, planContext);
    case "get_price_exploration_data":
      return await handleGetPriceExplorationData(args, planContext);
    case "create_ticket":
      return await handleCreateTicket(args, user, attachments);
    case "list_my_tickets":
      return await handleListMyTickets(args, user);
    case "get_ticket_status":
      return await handleGetTicketStatus(args);
    case "update_ticket_status":
      return await handleUpdateTicketStatus(args, user);
    case "list_modules_and_pages":
      return handleListModulesAndPages();
    default:
      return {
        response: { error: `Unknown action: ${actionName}` },
      };
  }
}

function handleListActions(): ActionResult {
  const actions = [
    {
      name: "Generate Report",
      description: "Create a custom CSV report from the Cross Tactic Analysis data. You can filter by date range, states, channels, and accounts.",
      example: "Generate a report for MA and TX for the last 30 days",
    },
    {
      name: "Report a Bug",
      description: "Report a bug or request a feature. I'll guide you through the process step by step.",
      example: "I want to report a bug",
    },
    {
      name: "Check My Tickets",
      description: "View your submitted tickets and their current status.",
      example: "Show my open tickets",
    },
  ];

  return {
    response: { actions },
    action: {
      type: "action_list",
      payload: actions,
    },
  };
}

async function handleLookupFilterValues(
  args: Record<string, unknown>,
): Promise<ActionResult> {
  const columnName = args.column_name as string;
  if (!columnName) {
    return { response: { error: "column_name is required" } };
  }

  try {
    const allValues = await getFilterValues(columnName);
    const search = (args.search as string)?.toLowerCase();

    let values = allValues;
    if (search) {
      values = allValues.filter((v) => v.toLowerCase().includes(search));
    }

    // Cap at 50 to keep response manageable
    const truncated = values.length > 50;
    const shown = values.slice(0, 50);

    return {
      response: {
        column: columnName,
        total_values: allValues.length,
        matching_values: values.length,
        values: shown,
        truncated,
        hint: search && values.length === 0
          ? `No values containing "${search}" found. Try a shorter search term or call without search to see all values.`
          : undefined,
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { response: { error: `Failed to look up filter values: ${msg}` } };
  }
}

async function handleListReportColumns(): Promise<ActionResult> {
  try {
    const schema = await getTableSchema();
    const columns = schema.map((c) => ({
      name: c.column_name,
      type: c.data_type,
    }));
    return {
      response: {
        total_columns: columns.length,
        columns,
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { response: { error: `Failed to list columns: ${msg}` } };
  }
}

async function handleGenerateReport(
  args: Record<string, unknown>,
  userId: string,
): Promise<ActionResult> {
  // Defaults
  const now = new Date();
  const thirtyDaysAgo = new Date(now);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  const reportName = (args.report_name as string) || "AI Generated Report";
  const dateStart = (args.date_start as string) || thirtyDaysAgo.toISOString().slice(0, 10);
  const dateEnd = (args.date_end as string) || now.toISOString().slice(0, 10);
  const states = (args.states as string[]) || [];
  const channels = (args.channels as string[]) || [];
  const accounts = (args.accounts as string[]) || [];
  const includeUnsold = args.include_unsold !== false; // default true

  // Get columns — use user-specified columns or all
  const schema = await getTableSchema();
  const allColumnNames = schema.map((c) => c.column_name);
  const userColumns = (args.selected_columns as string[]) || [];
  const allColumns = userColumns.length > 0
    ? userColumns.filter((c) => allColumnNames.includes(c))
    : allColumnNames;

  // Build fixed filters
  const fixedFilters: CreateReportInput["fixedFilters"] = {
    transaction_sold: includeUnsold ? "all" : "1",
  };
  if (states.length > 0) fixedFilters.data_state = states;
  if (channels.length > 0) fixedFilters.attribution_channel = channels;
  if (accounts.length > 0) fixedFilters.account_name = accounts;

  const input: CreateReportInput = {
    reportName,
    dateStart,
    dateEnd,
    fixedFilters,
    dynamicFilters: [],
    selectedColumns: allColumns,
  };

  try {
    const { reportId } = await createReport(userId, input);
    return {
      response: {
        success: true,
        reportId,
        reportName,
        dateRange: `${dateStart} to ${dateEnd}`,
        filters: {
          states: states.length > 0 ? states : "all",
          channels: channels.length > 0 ? channels : "all",
          accounts: accounts.length > 0 ? accounts : "all",
          includeUnsold,
        },
        message: "Report created successfully. It will be ready for download shortly on the Reports page.",
      },
      action: {
        type: "report_created",
        payload: { reportId, reportName },
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return {
      response: {
        success: false,
        error: `Failed to create report: ${msg}`,
      },
    };
  }
}

/* ------------------------------------------------------------------ */
/*  Ad Lever data handler                                              */
/* ------------------------------------------------------------------ */

async function handleGetAdLeverData(
  args: Record<string, unknown>,
  planContext?: ActionPlanContext,
): Promise<ActionResult> {
  if (!planContext?.planId) {
    return {
      response: {
        error: "No active plan selected. Please select a plan first so I can fetch lever data scoped to the plan's date range and activity type.",
      },
    };
  }

  const filterStates = (args.states as string[]) || [];
  const filterSegments = (args.segments as string[]) || [];
  const minLever = typeof args.min_lever === "number" ? args.min_lever : 0;
  const startDate = (args.start_date as string) || planContext.perfStartDate;
  const endDate = (args.end_date as string) || planContext.perfEndDate;

  try {
    const rows = await getAdLeverData({
      planId: planContext.planId,
      startDate,
      endDate,
      activityLeadType: planContext.activityLeadType,
      qbc: 0,
    });

    // Filter and slim rows for AI
    let result = rows.map((r) => ({
      state: r.state,
      segment: r.segment,
      lever: r.lever_override ?? r.lever,
      cor_score: r.cor_score,
      q2b_score: r.q2b_score,
      wr_score: r.wr_score,
      retention_score: r.retention_score,
      qltv_score: (r as any).qltv_score,
      strategy_score: (r as any).strategy_score,
      final_score: r.final_score,
      combined_ratio: r.combined_ratio != null ? Math.round(r.combined_ratio * 1000) / 10 + "%" : null,
      q2b: r.q2b != null ? Math.round(r.q2b * 1000) / 10 + "%" : null,
      win_rate: r.win_rate != null ? Math.round(r.win_rate * 1000) / 10 + "%" : null,
      binds: r.binds,
      is_low_volume: r.is_low_volume,
    }));

    // Apply optional filters
    if (filterStates.length > 0) {
      result = result.filter((r) => filterStates.map((s) => s.toUpperCase()).includes(r.state));
    }
    if (filterSegments.length > 0) {
      result = result.filter((r) => filterSegments.map((s) => s.toUpperCase()).includes(r.segment));
    }
    if (minLever > 0) {
      result = result.filter((r) => r.lever != null && Number(r.lever) >= minLever);
    }

    return {
      response: {
        plan_id: planContext.planId,
        date_range: `${startDate} to ${endDate}`,
        activity_lead_type: planContext.activityLeadType,
        total_rows: result.length,
        scoring_note: "Lever 1–10 (higher = better opportunity). Scores: COR (lower COR = better), Q2B (higher = better), Win Rate (higher = better), Retention (higher NB% of LT Prem = better), QLTV (Q2B × MRLTV, higher = better), Strategy (from plan's strategy rule for this state+segment). final_score = average of available component scores. Rows with is_low_volume=true have insufficient data (binds < 2) and receive no computed lever.",
        rows: result,
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return {
      response: { error: `Failed to fetch Ad Lever data: ${msg}` },
    };
  }
}

/* ------------------------------------------------------------------ */
/*  Price Exploration data handler                                     */
/* ------------------------------------------------------------------ */

/** Slim row shape sent to Gemini — only the fields it needs for analysis */
interface PeRowForAi {
  state: string;
  channel_group_name: string;
  testing_point: number;
  bids: number;
  sold: number;
  win_rate: number;
  cpc: number;
  win_rate_uplift: number | null;
  cpc_uplift: number | null;
  additional_clicks: number | null;
  expected_bind_change: number | null;
  additional_budget_needed: number | null;
  current_cpb: number | null;
  expected_cpb: number | null;
  cpb_uplift: number | null;
  stat_sig: string;
  recommended_testing_point: number | null;
  is_recommended: boolean;
  is_override: boolean;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
}

function toAiRow(row: PriceExplorationRow): PeRowForAi {
  return {
    state: row.state,
    channel_group_name: row.channel_group_name,
    testing_point: row.testing_point,
    bids: row.bids,
    sold: row.sold,
    win_rate: row.win_rate,
    cpc: row.cpc,
    win_rate_uplift: row.win_rate_uplift,
    cpc_uplift: row.cpc_uplift,
    additional_clicks: row.additional_clicks,
    expected_bind_change: row.expected_bind_change,
    additional_budget_needed: row.additional_budget_needed,
    current_cpb: row.current_cpb,
    expected_cpb: row.expected_cpb,
    cpb_uplift: row.cpb_uplift,
    stat_sig: row.stat_sig,
    recommended_testing_point: row.recommended_testing_point,
    is_recommended:
      row.testing_point === row.recommended_testing_point && row.testing_point !== 0,
    is_override: row.is_override,
    performance: row.performance,
    roe: row.roe,
    combined_ratio: row.combined_ratio,
  };
}

async function handleGetPriceExplorationData(
  args: Record<string, unknown>,
  planContext?: ActionPlanContext,
): Promise<ActionResult> {
  if (!planContext?.planId) {
    return {
      response: {
        error: "No active plan selected. Please select a plan first so I can fetch the correct Price Exploration data scoped to the plan's date range and strategy rules.",
      },
    };
  }

  const states = (args.states as string[]) || [];
  const channelGroups = (args.channel_groups as string[]) || [];
  const includeAllTps = args.include_all_testing_points === true;
  const topPairs = typeof args.top_pairs === "number" ? args.top_pairs : 0;

  // Determine QBC based on activity type
  const activity = planContext.activityLeadType?.split("_")[0];
  const qbc = activity === "clicks"
    ? (planContext.qbcClicks ?? 0)
    : (planContext.qbcLeadsCalls ?? 0);

  // Use price exploration dates (preferred), fall back to performance dates
  const peStartDate = planContext.priceStartDate || planContext.perfStartDate;
  const peEndDate = planContext.priceEndDate || planContext.perfEndDate;

  const filters: PriceExplorationFilters = {
    planId: planContext.planId,
    startDate: peStartDate,
    endDate: peEndDate,
    activityLeadType: planContext.activityLeadType,
    qbc,
    states: states.length > 0 ? states : undefined,
    channelGroups: channelGroups.length > 0 ? channelGroups : undefined,
    topPairs,
    limit: 50000,
  };

  try {
    const allRows = await getPriceExploration(filters);

    let resultRows: PeRowForAi[];
    if (includeAllTps) {
      resultRows = allRows.map(toAiRow);
    } else {
      // Return only the recommended TP row for each state+channel pair
      resultRows = allRows
        .filter(
          (r) =>
            r.testing_point === r.recommended_testing_point &&
            r.testing_point !== 0
        )
        .map(toAiRow);
    }

    // Sort by expected_bind_change descending
    resultRows.sort(
      (a, b) => (b.expected_bind_change ?? 0) - (a.expected_bind_change ?? 0),
    );

    // Cap output to prevent context explosion
    const MAX_ROWS = 200;
    const truncated = resultRows.length > MAX_ROWS;
    const shown = resultRows.slice(0, MAX_ROWS);

    // Compute summary stats
    const positive = shown.filter((r) => (r.expected_bind_change ?? 0) > 0);
    const totalAdditionalBinds = positive.reduce(
      (sum, r) => sum + (r.expected_bind_change ?? 0),
      0,
    );
    const totalAdditionalBudget = positive.reduce(
      (sum, r) => sum + (r.additional_budget_needed ?? 0),
      0,
    );

    return {
      response: {
        plan_id: planContext.planId,
        date_range: `${peStartDate} to ${peEndDate}`,
        activity_lead_type: planContext.activityLeadType,
        qbc,
        total_state_channel_pairs: resultRows.length,
        pairs_with_positive_bind_change: positive.length,
        total_additional_binds: Math.round(totalAdditionalBinds * 100) / 100,
        total_additional_budget: Math.round(totalAdditionalBudget * 100) / 100,
        rows: shown,
        truncated,
        note: includeAllTps
          ? "Showing ALL testing points. Rows with is_recommended=true are the system-recommended TPs."
          : "Showing only the recommended testing point for each state+channel pair. Use include_all_testing_points=true to see all TPs.",
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return {
      response: {
        error: `Failed to fetch Price Exploration data: ${msg}`,
      },
    };
  }
}

/* ------------------------------------------------------------------ */
/*  Ticket tool handlers                                               */
/* ------------------------------------------------------------------ */

async function handleCreateTicket(
  args: Record<string, unknown>,
  user: ActionUser,
  attachments?: Attachment[],
): Promise<ActionResult> {
  if (!user.permissions.includes("tickets:add")) {
    return {
      response: { error: "You don't have permission to create tickets. Contact an admin to get the 'tickets:add' permission." },
    };
  }

  const ticketType = args.type as "bug" | "feature";
  const title = args.title as string;
  const description = (args.description as string) || "";
  const module = args.module as string;
  const page = args.page as string;

  if (!ticketType || !title || !module || !page) {
    return { response: { error: "Missing required fields: type, title, module, and page are all required." } };
  }

  try {
    const result = await createTicket(
      { userId: user.userId, email: user.email },
      { type: ticketType, title, description, module, page, attachments },
    );
    return {
      response: {
        success: true,
        ticket_number: result.ticketNumber,
        ticket_id: result.ticketId,
        message: `Ticket TKT-${result.ticketNumber} created successfully!`,
      },
      action: {
        type: "ticket_created",
        payload: { ticketNumber: result.ticketNumber, ticketId: result.ticketId, title },
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { response: { error: `Failed to create ticket: ${msg}` } };
  }
}

async function handleListMyTickets(
  args: Record<string, unknown>,
  user: ActionUser,
): Promise<ActionResult> {
  const status = args.status as string | undefined;
  const type = args.type as string | undefined;
  const limit = typeof args.limit === "number" ? args.limit : 10;

  try {
    const tickets = await listTickets({
      createdBy: user.userId,
      status,
      type,
      limit,
    });

    const slim = tickets.map((t) => ({
      ticket_number: t.ticket_number,
      title: t.title,
      type: t.type,
      status: t.status,
      module: t.module,
      page: t.page,
      created_at: t.created_at,
    }));

    return {
      response: {
        total: slim.length,
        tickets: slim,
      },
      action: {
        type: "ticket_list",
        payload: slim,
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { response: { error: `Failed to list tickets: ${msg}` } };
  }
}

async function handleGetTicketStatus(
  args: Record<string, unknown>,
): Promise<ActionResult> {
  const ticketNumber = args.ticket_number as number;
  if (!ticketNumber) {
    return { response: { error: "ticket_number is required" } };
  }

  try {
    // Query by ticket_number
    const rows = await query<{ ticket_id: string }>(
      `SELECT ticket_id FROM ${table("tickets")} WHERE ticket_number = @ticketNumber`,
      { ticketNumber },
    );
    if (rows.length === 0) {
      return { response: { error: `Ticket TKT-${ticketNumber} not found.` } };
    }

    const ticket = await getTicket(rows[0].ticket_id);
    if (!ticket) {
      return { response: { error: `Ticket TKT-${ticketNumber} not found.` } };
    }

    const detail = {
      ticket_number: ticket.ticket_number,
      title: ticket.title,
      type: ticket.type,
      status: ticket.status,
      module: ticket.module,
      page: ticket.page,
      description: ticket.description,
      created_by_email: ticket.created_by_email,
      assigned_to: ticket.assigned_to,
      complexity: ticket.complexity,
      created_at: ticket.created_at,
      updated_at: ticket.updated_at,
      resolved_at: ticket.resolved_at,
      resolution_notes: ticket.resolution_notes,
      valid_next_statuses: STATUS_TRANSITIONS[ticket.status as TicketStatus] || [],
    };

    return {
      response: detail,
      action: {
        type: "ticket_detail",
        payload: detail,
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { response: { error: `Failed to get ticket: ${msg}` } };
  }
}

async function handleUpdateTicketStatus(
  args: Record<string, unknown>,
  user: ActionUser,
): Promise<ActionResult> {
  const ticketNumber = args.ticket_number as number;
  const newStatus = args.new_status as string;

  if (!ticketNumber || !newStatus) {
    return { response: { error: "ticket_number and new_status are required." } };
  }

  try {
    // Look up ticket by number
    const rows = await query<{ ticket_id: string; status: string; created_by: string }>(
      `SELECT ticket_id, status, created_by FROM ${table("tickets")} WHERE ticket_number = @ticketNumber`,
      { ticketNumber },
    );
    if (rows.length === 0) {
      return { response: { error: `Ticket TKT-${ticketNumber} not found.` } };
    }

    const { ticket_id, status: currentStatus, created_by } = rows[0];

    // Validate transition
    const allowed = STATUS_TRANSITIONS[currentStatus as TicketStatus] || [];
    if (!allowed.includes(newStatus as TicketStatus)) {
      return {
        response: {
          error: `Cannot transition from '${currentStatus}' to '${newStatus}'. Valid transitions: ${allowed.join(", ") || "none (terminal status)"}`,
        },
      };
    }

    // Permission check: approval statuses need tickets:approve
    const approvalStatuses = ["spec_approved", "adjusted_spec", "deployment_approved"];
    if (approvalStatuses.includes(newStatus) && !user.permissions.includes("tickets:approve")) {
      return { response: { error: `Status '${newStatus}' requires 'tickets:approve' permission.` } };
    }

    // Owner or admin can update
    const isOwner = created_by === user.userId;
    const isAdmin = user.permissions.includes("tickets:approve");
    if (!isOwner && !isAdmin) {
      return { response: { error: "You can only update tickets you created, or you need 'tickets:approve' permission." } };
    }

    await updateTicket(ticket_id, { status: newStatus as TicketStatus }, { userId: user.userId, email: user.email });

    return {
      response: {
        success: true,
        ticket_number: ticketNumber,
        old_status: currentStatus,
        new_status: newStatus,
        message: `TKT-${ticketNumber} status updated from '${currentStatus}' to '${newStatus}'.`,
      },
      action: {
        type: "ticket_updated",
        payload: { ticketNumber, oldStatus: currentStatus, newStatus },
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { response: { error: `Failed to update ticket: ${msg}` } };
  }
}

function handleListModulesAndPages(): ActionResult {
  return {
    response: {
      modules: MODULE_PAGES.map((m) => ({
        moduleId: m.moduleId,
        moduleLabel: m.moduleLabel,
        pages: m.pages,
      })),
    },
  };
}
