/**
 * PG-native analytics query functions (shadow-mode).
 *
 * These mirror the 6 cached BQ functions in analyticsService.ts
 * but query PostgreSQL tables directly (no in-memory cache).
 *
 * Tables used:
 *   - state_segment_daily      (existing PG table, synced daily)
 *   - pe_computed_daily         (new: full PE CTE output from BQ)
 *   - plan_merged_daily         (new: fn_plan_merged_agg output from BQ)
 *
 * ROE/COR: stored with qbc=0, recomputed here at query time with runtime qbc.
 */

import { pgQuery } from "../db/postgres.js";
import type {
  StateSegmentFilters,
  StateSegmentPerformanceRow,
  PriceExplorationRow,
  PlanMergedRow,
} from "./analyticsService.js";
import { splitCombinedFilter } from "./shared/activityScope.js";

const ALL_US_STATE_CODES = [
  "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
  "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
  "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE",
  "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI",
  "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
];

function withAllStateCodes(states: string[]): string[] {
  const normalized = states.map((v) => String(v || "").trim().toUpperCase()).filter(Boolean);
  return [...new Set([...ALL_US_STATE_CODES, ...normalized])].sort();
}

// ── Normalizers (same logic as analyticsService.ts) ──────────────────

type NormalizedFilters = {
  startDate: string;
  endDate: string;
  states: string[];
  segments: string[];
  channelGroups: string[];
  activityType: string;
  leadType: string;
  stateSegmentActivityType: string;
  stateSegmentLeadType: string;
  qbc: number;
  groupBy: string;
};

function normalizeSSPFilters(filters: StateSegmentFilters): NormalizedFilters {
  const states = (filters.states ?? []).map((v) => v.trim()).filter(Boolean);
  const segments = (filters.segments ?? []).map((v) => v.trim().toUpperCase()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((v) => v.trim()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    states,
    segments,
    channelGroups,
    activityType: combined.activityType,
    leadType: combined.leadType,
    stateSegmentActivityType: combined.stateSegmentActivityType,
    stateSegmentLeadType: combined.stateSegmentLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0,
    groupBy: filters.groupBy || "state_segment_channel",
  };
}

// ── 1. getStateSegmentPerformanceFromDailyPG ─────────────────────────

const VIEW_DIMENSIONS: Record<string, string[]> = {
  state: ["state"],
  segment: ["segment"],
  channel_group: ["channel_group_name"],
  state_segment: ["state", "segment"],
  state_channel_group: ["state", "channel_group_name"],
  state_segment_channel: ["state", "segment", "channel_group_name"],
};

export async function getSSPFromDailyPG(
  filters: StateSegmentFilters,
  _normalized: unknown
): Promise<StateSegmentPerformanceRow[]> {
  const n = normalizeSSPFilters(filters);
  const dims = VIEW_DIMENSIONS[n.groupBy] || VIEW_DIMENSIONS.state_segment_channel;

  const selectDims = [
    dims.includes("state") ? "state" : "'ALL' AS state",
    dims.includes("segment") ? "segment" : "'ALL' AS segment",
    dims.includes("channel_group_name") ? "channel_group_name" : "'ALL' AS channel_group_name",
  ].join(",\n        ");
  const groupByClause = dims.join(", ");

  // Build WHERE conditions
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  if (n.startDate) {
    conditions.push("event_date >= @startDate::date");
    params.startDate = n.startDate;
  }
  if (n.endDate) {
    conditions.push("event_date <= @endDate::date");
    params.endDate = n.endDate;
  }
  if (n.states.length > 0) {
    conditions.push("state = ANY(@states)");
    params.states = n.states;
  }
  if (n.segments.length > 0) {
    conditions.push("segment = ANY(@segments)");
    params.segments = n.segments;
  }
  if (n.channelGroups.length > 0) {
    conditions.push("channel_group_name = ANY(@channelGroups)");
    params.channelGroups = n.channelGroups;
  }
  if (n.stateSegmentActivityType) {
    conditions.push("activity_type = @activityType");
    params.activityType = n.stateSegmentActivityType;
  }
  if (n.stateSegmentLeadType) {
    conditions.push("lead_type = @leadType");
    params.leadType = n.stateSegmentLeadType;
  }

  params.qbc = n.qbc;
  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  const sql = `
    SELECT
      ${selectDims},
      SUM(bids) AS bids,
      SUM(sold) AS sold,
      SUM(total_cost) AS total_cost,
      SUM(quote_started) AS quote_started,
      SUM(quotes) AS quotes,
      SUM(binds) AS binds,
      CASE WHEN NULLIF(SUM(quotes), 0) IS NULL THEN NULL
        ELSE SUM(binds)::double precision / SUM(quotes)
      END AS q2b_score,
      SUM(scored_policies) AS scored_policies,
      CASE WHEN NULLIF(SUM(binds), 0) IS NULL THEN NULL
        ELSE SUM(total_cost)::double precision / SUM(binds)
      END AS cpb,
      CASE WHEN SUM(binds) = 0 THEN 0
        ELSE (SUM(target_cpb_sum)::double precision / SUM(binds))
      END AS target_cpb,
      CASE WHEN SUM(binds) = 0 OR NULLIF(SUM(total_cost), 0) IS NULL THEN 0
        ELSE (SUM(target_cpb_sum)::double precision / SUM(binds))
          / (SUM(total_cost)::double precision / SUM(binds))
      END AS performance,
      CASE
        WHEN SUM(scored_policies) = 0
          OR (SUM(avg_equity_sum)::double precision / NULLIF(SUM(scored_policies), 0)) = 0
          THEN 0
        ELSE (
          (SUM(avg_profit_sum)::double precision / NULLIF(SUM(scored_policies), 0))
          - (0.8 * ((SUM(total_cost)::double precision / NULLIF(SUM(binds), 0)) / 0.81 + @qbc))
        ) / (SUM(avg_equity_sum)::double precision / NULLIF(SUM(scored_policies), 0))
      END AS roe,
      CASE
        WHEN SUM(scored_policies) = 0
          OR (SUM(lifetime_premium_sum)::double precision / NULLIF(SUM(scored_policies), 0)) = 0
          THEN 0
        ELSE (
          (SUM(total_cost)::double precision / NULLIF(SUM(binds), 0)) / 0.81
          + @qbc
          + (SUM(lifetime_cost_sum)::double precision / NULLIF(SUM(scored_policies), 0))
        ) / (SUM(lifetime_premium_sum)::double precision / NULLIF(SUM(scored_policies), 0))
      END AS combined_ratio,
      CASE WHEN NULLIF(SUM(scored_policies), 0) IS NULL THEN NULL
        ELSE SUM(avg_mrltv_sum)::double precision / SUM(scored_policies)
      END AS mrltv,
      CASE WHEN NULLIF(SUM(scored_policies), 0) IS NULL THEN NULL
        ELSE SUM(avg_profit_sum)::double precision / SUM(scored_policies)
      END AS profit,
      CASE WHEN NULLIF(SUM(scored_policies), 0) IS NULL THEN NULL
        ELSE SUM(avg_equity_sum)::double precision / SUM(scored_policies)
      END AS equity,
      SUM(target_cpb_sum) AS target_cpb_sum,
      SUM(avg_profit_sum) AS avg_profit_sum,
      SUM(avg_equity_sum) AS avg_equity_sum,
      SUM(lifetime_cost_sum) AS lifetime_cost_sum,
      SUM(lifetime_premium_sum) AS lifetime_premium_sum,
      SUM(avg_mrltv_sum) AS avg_mrltv_sum
    FROM state_segment_daily
    ${where}
    GROUP BY ${groupByClause}
    ORDER BY ${groupByClause}
  `;

  return pgQuery<StateSegmentPerformanceRow>(sql, params);
}

// ── 2. listStateSegmentFiltersFromDailyPG ────────────────────────────

type FilterOptionsRow = {
  states: string[];
  segments: string[];
  channel_groups: string[];
};

export async function listSSPFiltersPG(
  normalized: { startDate: string; endDate: string; activityType: string; leadType: string; stateSegmentActivityType?: string; stateSegmentLeadType?: string }
): Promise<FilterOptionsRow> {
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  if (normalized.startDate) {
    conditions.push("event_date >= @startDate::date");
    params.startDate = normalized.startDate;
  }
  if (normalized.endDate) {
    conditions.push("event_date <= @endDate::date");
    params.endDate = normalized.endDate;
  }
  if (normalized.stateSegmentActivityType) {
    conditions.push("activity_type = @activityType");
    params.activityType = normalized.stateSegmentActivityType;
  }
  if (normalized.stateSegmentLeadType) {
    conditions.push("lead_type = @leadType");
    params.leadType = normalized.stateSegmentLeadType;
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  const [statesResult, segmentsResult, channelsResult] = await Promise.all([
    pgQuery<{ state: string }>(
      `SELECT DISTINCT state FROM state_segment_daily ${where} ORDER BY state`,
      params
    ),
    pgQuery<{ segment: string }>(
      `SELECT DISTINCT segment FROM state_segment_daily
       ${where ? where + " AND" : "WHERE"} segment IN ('MCH', 'MCR', 'SCH', 'SCR', 'HOME', 'RENT')
       ORDER BY segment`,
      params
    ),
    pgQuery<{ channel_group_name: string }>(
      `SELECT DISTINCT channel_group_name FROM state_segment_daily
       ${where ? where + " AND" : "WHERE"} channel_group_name IS NOT NULL AND channel_group_name != ''
       ORDER BY channel_group_name`,
      params
    ),
  ]);

  return {
    states: withAllStateCodes(statesResult.map((r) => r.state)),
    segments: segmentsResult.map((r) => r.segment),
    channel_groups: channelsResult.map((r) => r.channel_group_name),
  };
}

// ── 3. listPriceExplorationFiltersPG ─────────────────────────────────

export async function listPEFiltersPG(
  normalized: { startDate: string; endDate: string; activityType: string; leadType: string }
): Promise<{ states: string[]; channelGroups: string[] }> {
  const activityLeadType = normalized.activityType && normalized.leadType
    ? `${normalized.activityType}_${normalized.leadType}`
    : "";

  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  if (activityLeadType) {
    conditions.push("activity_lead_type = @scope");
    params.scope = activityLeadType;
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  const [statesResult, channelsResult] = await Promise.all([
    pgQuery<{ state: string }>(
      `SELECT DISTINCT state FROM pe_computed_daily ${where} ORDER BY state`,
      params
    ),
    pgQuery<{ channel_group_name: string }>(
      `SELECT DISTINCT channel_group_name FROM pe_computed_daily
       ${where ? where + " AND" : "WHERE"} channel_group_name IS NOT NULL
       ORDER BY channel_group_name`,
      params
    ),
  ]);

  return {
    states: withAllStateCodes(statesResult.map((r) => r.state)),
    channelGroups: channelsResult.map((r) => r.channel_group_name),
  };
}

// ── 4. getPriceExplorationBQviaPG ────────────────────────────────────

export async function getPEBQviaPG(
  normalized: {
    startDate: string;
    endDate: string;
    q2bStartDate: string;
    q2bEndDate: string;
    states: string[];
    channelGroups: string[];
    activityType: string;
    leadType: string;
    qbc: number;
    limit: number;
    topPairs: number;
  }
): Promise<PriceExplorationRow[]> {
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  // Scope filter
  const activityLeadType = normalized.activityType && normalized.leadType
    ? `${normalized.activityType}_${normalized.leadType}`
    : "";
  if (activityLeadType) {
    conditions.push("activity_lead_type = @scope");
    params.scope = activityLeadType;
  }

  // State filter (skip if __ALL__)
  const isAllStates = normalized.states.length === 1 && normalized.states[0] === "__ALL__";
  if (!isAllStates && normalized.states.length > 0) {
    conditions.push("state = ANY(@states)");
    params.states = normalized.states;
  }

  // Channel filter (skip if __ALL__)
  const isAllChannels = normalized.channelGroups.length === 1 && normalized.channelGroups[0] === "__ALL__";
  if (!isAllChannels && normalized.channelGroups.length > 0) {
    conditions.push("channel_group_name = ANY(@channelGroups)");
    params.channelGroups = normalized.channelGroups;
  }

  params.qbc = normalized.qbc;
  params.limit = normalized.limit;

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // Recompute ROE and combined_ratio with runtime qbc
  // ROE = (avg_profit - 0.8 * (cpb / 0.81 + qbc)) / avg_equity
  // COR = (cpb / 0.81 + qbc + avg_lifetime_cost) / avg_lifetime_premium
  const roeExpr = `
    CASE
      WHEN expected_cpb IS NULL
        OR scf_avg_equity IS NULL
        OR scf_avg_equity = 0
        THEN 0
      ELSE (
        scf_avg_profit
        - (0.8 * (expected_cpb / 0.81 + @qbc))
      ) / scf_avg_equity
    END`;

  const corExpr = `
    CASE
      WHEN expected_cpb IS NULL
        OR scf_avg_lifetime_premium IS NULL
        OR scf_avg_lifetime_premium = 0
        THEN 0
      ELSE (
        expected_cpb / 0.81
        + @qbc
        + scf_avg_lifetime_cost
      ) / scf_avg_lifetime_premium
    END`;

  let sql: string;
  if (normalized.topPairs > 0) {
    params.topPairs = normalized.topPairs;
    sql = `
      WITH base AS (
        SELECT
          channel_group_name, state, testing_point,
          opps, bids, win_rate, sold, binds, quotes,
          click_to_quote, channel_quote, click_to_channel_quote,
          q2b, channel_binds, channel_q2b,
          cpc, avg_bid,
          win_rate_uplift_state, cpc_uplift_state,
          win_rate_uplift_channel, cpc_uplift_channel,
          win_rate_uplift, cpc_uplift,
          additional_clicks, expected_bind_change, additional_budget_needed,
          current_cpb, expected_cpb, cpb_uplift,
          performance,
          ${roeExpr} AS roe,
          ${corExpr} AS combined_ratio,
          recommended_testing_point,
          stat_sig,
          COALESCE(stat_sig_channel_group, '') AS stat_sig_channel_group,
          stat_sig_source
        FROM pe_computed_daily
        ${where}
      ),
      pair_bind_scores AS (
        SELECT state, channel_group_name,
          MAX(CASE WHEN testing_point = recommended_testing_point
              THEN COALESCE(expected_bind_change, 0) ELSE 0 END) AS pair_bind_score
        FROM base
        GROUP BY state, channel_group_name
        ORDER BY pair_bind_score DESC
        LIMIT @topPairs
      )
      SELECT b.*
      FROM base b
      INNER JOIN pair_bind_scores p
        ON b.state = p.state AND b.channel_group_name = p.channel_group_name
      ORDER BY p.pair_bind_score DESC, b.state, b.channel_group_name, b.testing_point
      LIMIT @limit
    `;
  } else {
    sql = `
      SELECT
        channel_group_name, state, testing_point,
        opps, bids, win_rate, sold, binds, quotes,
        click_to_quote, channel_quote, click_to_channel_quote,
        q2b, channel_binds, channel_q2b,
        cpc, avg_bid,
        win_rate_uplift_state, cpc_uplift_state,
        win_rate_uplift_channel, cpc_uplift_channel,
        win_rate_uplift, cpc_uplift,
        additional_clicks, expected_bind_change, additional_budget_needed,
        current_cpb, expected_cpb, cpb_uplift,
        performance,
        ${roeExpr} AS roe,
        ${corExpr} AS combined_ratio,
        recommended_testing_point,
        stat_sig,
        COALESCE(stat_sig_channel_group, '') AS stat_sig_channel_group,
        stat_sig_source
      FROM pe_computed_daily
      ${where}
      ORDER BY channel_group_name, state, testing_point
      LIMIT @limit
    `;
  }

  return pgQuery<PriceExplorationRow>(sql, params);
}

// ── 5. listPlanMergedFiltersPG ───────────────────────────────────────

export async function listPlanMergedFiltersPG(
  _normalized: { startDate: string; endDate: string; activityType: string; leadType: string }
): Promise<{
  states: string[];
  segments: string[];
  channelGroups: string[];
  testingPoints: number[];
  statSig: string[];
}> {
  const [statesR, segmentsR, channelsR, tpR, sigR] = await Promise.all([
    pgQuery<{ state: string }>(
      "SELECT DISTINCT state FROM plan_merged_daily WHERE state IS NOT NULL ORDER BY state"
    ),
    pgQuery<{ segment: string }>(
      "SELECT DISTINCT segment FROM plan_merged_daily WHERE segment IS NOT NULL ORDER BY segment"
    ),
    pgQuery<{ channel_group_name: string }>(
      "SELECT DISTINCT channel_group_name FROM plan_merged_daily WHERE channel_group_name IS NOT NULL ORDER BY channel_group_name"
    ),
    pgQuery<{ price_adjustment_percent: number }>(
      "SELECT DISTINCT price_adjustment_percent FROM plan_merged_daily ORDER BY price_adjustment_percent"
    ),
    pgQuery<{ stat_sig: string }>(
      "SELECT DISTINCT stat_sig FROM plan_merged_daily WHERE stat_sig IS NOT NULL ORDER BY stat_sig"
    ),
  ]);

  return {
    states: withAllStateCodes(statesR.map((r) => r.state)),
    segments: segmentsR.map((r) => r.segment),
    channelGroups: channelsR.map((r) => r.channel_group_name),
    testingPoints: tpR.map((r) => Number(r.price_adjustment_percent)),
    statSig: sigR.map((r) => r.stat_sig),
  };
}

// ── 6. getPlanMergedAnalyticsPG ──────────────────────────────────────

export async function getPlanMergedPG(
  normalized: {
    startDate: string;
    endDate: string;
    states: string[];
    segments: string[];
    channelGroups: string[];
    testingPoints: number[];
    statSig: string[];
    activityType: string;
    leadType: string;
  }
): Promise<PlanMergedRow[]> {
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  const isAll = (arr: unknown[]) => arr.length === 1 && (arr[0] === "__ALL__" || arr[0] === 999999999);

  if (!isAll(normalized.states) && normalized.states.length > 0) {
    conditions.push("state = ANY(@states)");
    params.states = normalized.states;
  }
  if (!isAll(normalized.segments) && normalized.segments.length > 0) {
    conditions.push("segment = ANY(@segments)");
    params.segments = normalized.segments;
  }
  if (!isAll(normalized.channelGroups) && normalized.channelGroups.length > 0) {
    conditions.push("channel_group_name = ANY(@channelGroups)");
    params.channelGroups = normalized.channelGroups;
  }
  if (!isAll(normalized.testingPoints) && normalized.testingPoints.length > 0) {
    conditions.push("price_adjustment_percent = ANY(@testingPoints::double precision[])");
    params.testingPoints = normalized.testingPoints;
  }
  if (!isAll(normalized.statSig) && normalized.statSig.length > 0) {
    conditions.push("stat_sig = ANY(@statSig)");
    params.statSig = normalized.statSig;
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  const sql = `
    SELECT
      start_date,
      end_date,
      channel_group_name,
      state,
      segment,
      price_adjustment_percent,
      stat_sig,
      stat_sig_channel_group,
      cpc_uplift,
      win_rate_uplift,
      additional_clicks,
      expected_total_clicks,
      expected_cpc,
      expected_total_cost,
      expected_total_binds,
      additional_expected_binds,
      expected_cpb,
      ss_performance,
      expected_performance,
      performance_uplift
    FROM plan_merged_daily
    ${where}
    ORDER BY channel_group_name, state, price_adjustment_percent
  `;

  return pgQuery<PlanMergedRow>(sql, params);
}
