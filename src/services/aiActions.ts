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
    type: "report_created" | "action_list";
    payload: unknown;
  };
}

/* ------------------------------------------------------------------ */
/*  Execution handlers                                                 */
/* ------------------------------------------------------------------ */

export async function executeAction(
  actionName: string,
  args: Record<string, unknown>,
  userId: string,
): Promise<ActionResult> {
  switch (actionName) {
    case "list_available_actions":
      return handleListActions();
    case "lookup_filter_values":
      return await handleLookupFilterValues(args);
    case "list_report_columns":
      return await handleListReportColumns();
    case "generate_report":
      return await handleGenerateReport(args, userId);
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
