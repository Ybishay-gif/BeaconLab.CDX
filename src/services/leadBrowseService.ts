/**
 * Lead Browse Service — paginated browsing, filtering, and related-lead lookup
 * for the Lead List and Lead Detail pages.
 *
 * Uses the same BQ Cross Tactic table as leadLookupService.
 */

import { query as bqQuery } from "../db/bigquery.js";
import { config } from "../config.js";
import { buildCacheKey, cacheGet, cacheSet } from "../cache.js";
import { getFilterValues } from "./reportService.js";

// ── Types ────────────────────────────────────────────────────────────

export interface DynamicFilter {
  column: string;
  operator: string;
  value: string | number | (string | number)[];
}

export interface LeadBrowseParams {
  startDate: string;
  endDate: string;
  activityTypes?: string[];
  leadTypes?: string[];
  accountNames?: string[];
  campaigns?: string[];
  segments?: string[];
  states?: string[];
  channels?: string[];
  rc1Statuses?: string[];
  rejectReasons?: string[];
  rc1Reasons?: string[];
  statuses?: string[]; // 'Sold' | 'Unsold'
  dynamicFilters?: DynamicFilter[];
  sortColumn?: string;
  sortDir?: "ASC" | "DESC";
  offset?: number;
  limit?: number;
}

export interface LeadListRow {
  Lead_LeadID: string;
  status: "Sold" | "Unsold";
  bid_price: number | null;
  Account_Name: string;
  Campaign_Name: string;
  Data_State: string;
  Segments: string;
  StrategyGroupName: string;
  ChannelGroupName: string;
  Data_DateCreated: string;
}

export interface LeadBrowseResult {
  rows: LeadListRow[];
  hasMore: boolean;
  nextOffset: number;
}

export interface RelatedLeadRow {
  Lead_LeadID: string;
  Data_DateCreated: string;
  Data_State: string;
  Campaign_Name: string;
  Account_Name: string;
  status: "Sold" | "Unsold";
  match_type: string;
}

// ── Whitelist for sort columns ───────────────────────────────────────

const SORT_COLUMN_WHITELIST: Record<string, string> = {
  Lead_LeadID: "Lead_LeadID",
  bid_price: "bid_price",
  Account_Name: "Account_Name",
  Campaign_Name: "Campaign_Name",
  Data_State: "Data_State",
  Segments: "Segments",
  StrategyGroupName: "StrategyGroupName",
  ChannelGroupName: "ChannelGroupName",
  Data_DateCreated: "Data_DateCreated",
  Transaction_sold: "Transaction_sold",
};

// ── Fixed filter column mapping ──────────────────────────────────────

const FIXED_FILTER_MAP: Record<string, string> = {
  activityTypes: "Origin_ActivityType",
  leadTypes: "LeadType",
  accountNames: "Account_Name",
  campaigns: "Campaign_Name",
  segments: "Segments",
  states: "Data_State",
  channels: "Attribution_Channel",
  rc1Statuses: "RateCall1Data_UnderwritingStatus",
  rejectReasons: "TrackingVariables_reject_reason",
  rc1Reasons: "RC1_Reson_Description",
};

// ── Dynamic filter operators ─────────────────────────────────────────

const ALLOWED_OPERATORS = new Set(["=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN"]);

// ── Build WHERE clause ───────────────────────────────────────────────

function buildWhereClause(params: LeadBrowseParams): {
  clause: string;
  sqlParams: Record<string, unknown>;
} {
  const conditions: string[] = [];
  const sqlParams: Record<string, unknown> = {};
  let paramIdx = 0;

  // Date range (required)
  conditions.push("CAST(Data_DateCreated AS DATE) >= @startDate");
  conditions.push("CAST(Data_DateCreated AS DATE) <= @endDate");
  sqlParams.startDate = params.startDate;
  sqlParams.endDate = params.endDate;

  // Fixed filters
  for (const [paramKey, bqColumn] of Object.entries(FIXED_FILTER_MAP)) {
    const values = (params as unknown as Record<string, unknown>)[paramKey] as string[] | undefined;
    if (values && values.length > 0) {
      const key = `ff${paramIdx++}`;
      sqlParams[key] = values;
      conditions.push(`\`${bqColumn}\` IN UNNEST(@${key})`);
    }
  }

  // Status filter (computed from Transaction_sold)
  if (params.statuses && params.statuses.length > 0) {
    const statusConds: string[] = [];
    if (params.statuses.includes("Sold")) {
      statusConds.push("Transaction_sold > 0");
    }
    if (params.statuses.includes("Unsold")) {
      statusConds.push("(Transaction_sold = 0 OR Transaction_sold IS NULL)");
    }
    if (statusConds.length === 1) {
      conditions.push(statusConds[0]);
    }
    // If both Sold + Unsold selected, no filtering needed
  }

  // Dynamic filters (same pattern as crossTacticService)
  for (const f of params.dynamicFilters ?? []) {
    if (!ALLOWED_OPERATORS.has(f.operator)) continue;
    const colRef = `\`${f.column}\``;

    if (f.operator === "BETWEEN" && Array.isArray(f.value) && f.value.length === 2) {
      const k1 = `dp${paramIdx++}`;
      const k2 = `dp${paramIdx++}`;
      sqlParams[k1] = f.value[0];
      sqlParams[k2] = f.value[1];
      conditions.push(`${colRef} BETWEEN @${k1} AND @${k2}`);
    } else if (f.operator === "IN") {
      const vals = Array.isArray(f.value) ? f.value : String(f.value).split(",").map((s) => s.trim());
      const key = `dp${paramIdx++}`;
      sqlParams[key] = vals;
      conditions.push(`${colRef} IN UNNEST(@${key})`);
    } else if (f.operator === "LIKE") {
      const key = `dp${paramIdx++}`;
      sqlParams[key] = `%${f.value}%`;
      conditions.push(`${colRef} LIKE @${key}`);
    } else {
      const key = `dp${paramIdx++}`;
      sqlParams[key] = f.value;
      conditions.push(`${colRef} ${f.operator} @${key}`);
    }
  }

  return {
    clause: conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "",
    sqlParams,
  };
}

// ── Browse Leads ─────────────────────────────────────────────────────

export async function browseLeads(params: LeadBrowseParams): Promise<LeadBrowseResult> {
  const limit = Math.min(params.limit ?? 100, 200);
  const offset = params.offset ?? 0;
  const sortCol = SORT_COLUMN_WHITELIST[params.sortColumn ?? "Data_DateCreated"] ?? "Data_DateCreated";
  const sortDir = params.sortDir === "ASC" ? "ASC" : "DESC";

  const { clause, sqlParams } = buildWhereClause(params);

  // Fetch limit+1 to determine hasMore
  const sql = `
    SELECT
      Lead_LeadID,
      Transaction_sold,
      bid_price,
      Account_Name,
      Campaign_Name,
      Data_State,
      Segments,
      StrategyGroupName,
      ChannelGroupName,
      CAST(Data_DateCreated AS STRING) AS Data_DateCreated
    FROM ${config.rawCrossTacticTable}
    ${clause}
    ORDER BY \`${sortCol}\` ${sortDir}
    LIMIT ${limit + 1}
    OFFSET ${offset}
  `;

  const rawRows = await bqQuery<Record<string, unknown>>(sql, sqlParams);

  const hasMore = rawRows.length > limit;
  const resultRows = hasMore ? rawRows.slice(0, limit) : rawRows;

  const rows: LeadListRow[] = resultRows.map((r) => ({
    Lead_LeadID: String(r.Lead_LeadID ?? ""),
    status: (Number(r.Transaction_sold) || 0) > 0 ? "Sold" : "Unsold",
    bid_price: r.bid_price != null ? Number(r.bid_price) : null,
    Account_Name: String(r.Account_Name ?? ""),
    Campaign_Name: String(r.Campaign_Name ?? ""),
    Data_State: String(r.Data_State ?? ""),
    Segments: String(r.Segments ?? ""),
    StrategyGroupName: String(r.StrategyGroupName ?? ""),
    ChannelGroupName: String(r.ChannelGroupName ?? ""),
    Data_DateCreated: String(r.Data_DateCreated ?? ""),
  }));

  return {
    rows,
    hasMore,
    nextOffset: offset + rows.length,
  };
}

// ── Lead Filter Values ───────────────────────────────────────────────

/** Columns allowed for the filter-values endpoint */
const FILTER_COLUMNS = new Set([
  "Origin_ActivityType",
  "LeadType",
  "Account_Name",
  "Campaign_Name",
  "Segments",
  "Data_State",
  "Attribution_Channel",
  "RateCall1Data_UnderwritingStatus",
  "TrackingVariables_reject_reason",
  "RC1_Reson_Description",
]);

export async function getLeadFilterValues(column: string): Promise<string[]> {
  if (!FILTER_COLUMNS.has(column)) {
    throw new Error(`Invalid filter column: ${column}`);
  }
  // Delegate to reportService (already cached 24h)
  return getFilterValues(column);
}

// ── Related Leads ────────────────────────────────────────────────────

export async function getRelatedLeads(leadId: string): Promise<RelatedLeadRow[]> {
  // Step 1: fetch the source lead's identifier keys
  const keysSql = `
    SELECT
      JornayaLeadId,
      Data_Sha256Phone,
      Sha256Email,
      SoldClickKey
    FROM ${config.rawCrossTacticTable}
    WHERE Lead_LeadID = @leadId
    LIMIT 1
  `;
  const keyRows = await bqQuery<Record<string, unknown>>(keysSql, { leadId });
  if (keyRows.length === 0) return [];

  const src = keyRows[0];
  const jornayaRaw = src.JornayaLeadId ? String(src.JornayaLeadId) : "";
  // Filter out dummy Jornaya IDs (all zeros, e.g. "0000000000", "00000000-0000-...")
  const jornaya = /^0[\-0]*$/.test(jornayaRaw) ? "" : jornayaRaw;
  const phone = src.Data_Sha256Phone ? String(src.Data_Sha256Phone) : "";
  const email = src.Sha256Email ? String(src.Sha256Email) : "";
  const soldClick = src.SoldClickKey ? String(src.SoldClickKey) : "";

  // Build OR conditions only for non-empty identifiers
  const orConditions: string[] = [];
  const sqlParams: Record<string, unknown> = { leadId };

  if (jornaya) {
    orConditions.push("(JornayaLeadId = @jornaya AND JornayaLeadId IS NOT NULL AND JornayaLeadId != '')");
    sqlParams.jornaya = jornaya;
  }
  if (phone) {
    orConditions.push("(Data_Sha256Phone = @phone AND Data_Sha256Phone IS NOT NULL AND Data_Sha256Phone != '')");
    sqlParams.phone = phone;
  }
  if (email) {
    orConditions.push("(Sha256Email = @email AND Sha256Email IS NOT NULL AND Sha256Email != '')");
    sqlParams.email = email;
  }
  if (soldClick) {
    orConditions.push("(SoldClickKey = @soldClick AND SoldClickKey IS NOT NULL AND SoldClickKey != '')");
    sqlParams.soldClick = soldClick;
  }

  if (orConditions.length === 0) return [];

  // Step 2: query related leads
  const matchTypeCase = `CASE
    ${jornaya ? "WHEN JornayaLeadId = @jornaya AND JornayaLeadId IS NOT NULL AND JornayaLeadId != '' THEN 'Jornaya ID'" : ""}
    ${phone ? "WHEN Data_Sha256Phone = @phone AND Data_Sha256Phone IS NOT NULL AND Data_Sha256Phone != '' THEN 'Sha256 Phone'" : ""}
    ${email ? "WHEN Sha256Email = @email AND Sha256Email IS NOT NULL AND Sha256Email != '' THEN 'Sha256 Email'" : ""}
    ${soldClick ? "WHEN SoldClickKey = @soldClick AND SoldClickKey IS NOT NULL AND SoldClickKey != '' THEN 'UA_IP_Zip_Year_Make'" : ""}
    ELSE 'Unknown'
  END`;

  const relatedSql = `
    SELECT
      Lead_LeadID,
      CAST(Data_DateCreated AS STRING) AS Data_DateCreated,
      Data_State,
      Campaign_Name,
      Account_Name,
      Transaction_sold,
      ${matchTypeCase} AS match_type
    FROM ${config.rawCrossTacticTable}
    WHERE Lead_LeadID != @leadId
      AND (${orConditions.join(" OR ")})
    ORDER BY Data_DateCreated DESC
    LIMIT 100
  `;

  const rawRows = await bqQuery<Record<string, unknown>>(relatedSql, sqlParams);

  return rawRows.map((r) => ({
    Lead_LeadID: String(r.Lead_LeadID ?? ""),
    Data_DateCreated: String(r.Data_DateCreated ?? ""),
    Data_State: String(r.Data_State ?? ""),
    Campaign_Name: String(r.Campaign_Name ?? ""),
    Account_Name: String(r.Account_Name ?? ""),
    status: (Number(r.Transaction_sold) || 0) > 0 ? ("Sold" as const) : ("Unsold" as const),
    match_type: String(r.match_type ?? ""),
  }));
}
