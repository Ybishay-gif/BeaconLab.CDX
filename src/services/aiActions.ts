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
import { createReport, getTableSchema, type CreateReportInput } from "./reportService.js";

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

  // Get all available columns for the report
  const schema = await getTableSchema();
  const allColumns = schema.map((c) => c.column_name);

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
