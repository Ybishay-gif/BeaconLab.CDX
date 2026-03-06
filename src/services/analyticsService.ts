import { analyticsRoutine, analyticsTable, query, table } from "../db/bigquery.js";
import { config } from "../config.js";
import { normalizeActivityScopeKey, splitCombinedFilter } from "./shared/activityScope.js";
import { buildCombinedRatioSql, buildRoeSql } from "./shared/kpiSql.js";

const RAW_CROSS_TACTIC_TABLE = config.rawCrossTacticTable;

export type StateSegmentFilters = {
  startDate?: string;
  endDate?: string;
  states?: string[];
  segments?: string[];
  channelGroups?: string[];
  activityLeadType?: string;
  qbc?: number;
};

export type PriceExplorationFilters = {
  planId?: string;
  startDate?: string;
  endDate?: string;
  q2bStartDate?: string;
  q2bEndDate?: string;
  states?: string[];
  channelGroups?: string[];
  activityLeadType?: string;
  qbc?: number;
  limit?: number;
};

export type PlanMergedFilters = {
  startDate?: string;
  endDate?: string;
  states?: string[];
  segments?: string[];
  channelGroups?: string[];
  testingPoints?: string[];
  statSig?: string[];
  activityLeadType?: string;
};

export type StateSegmentPerformanceRow = {
  state: string;
  segment: string;
  channel_group_name: string;
  bids: number;
  sold: number;
  total_cost: number;
  quote_started: number;
  quotes: number;
  binds: number;
  q2b_score: number;
  scored_policies: number;
  cpb: number;
  target_cpb: number;
  performance: number;
  roe: number;
  combined_ratio: number;
  mrltv: number;
  profit: number;
  equity: number;
};

export type PriceExplorationRow = {
  channel_group_name: string;
  state: string;
  testing_point: number;
  opps: number;
  bids: number;
  win_rate: number;
  sold: number;
  binds: number;
  quotes: number;
  click_to_quote: number | null;
  channel_quote: number;
  click_to_channel_quote: number | null;
  q2b: number | null;
  channel_binds: number | null;
  channel_q2b: number | null;
  cpc: number;
  avg_bid: number;
  win_rate_uplift_state: number | null;
  cpc_uplift_state: number | null;
  win_rate_uplift_channel: number | null;
  cpc_uplift_channel: number | null;
  win_rate_uplift: number | null;
  cpc_uplift: number | null;
  additional_clicks: number | null;
  expected_bind_change: number | null;
  additional_budget_needed: number | null;
  current_cpb: number | null;
  expected_cpb: number | null;
  cpb_uplift: number | null;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
  recommended_testing_point: number | null;
  stat_sig: string;
  stat_sig_channel_group: string;
  stat_sig_source: string;
};

export type PlanMergedRow = {
  start_date: string;
  end_date: string;
  channel_group_name: string;
  state: string;
  segment: string;
  price_adjustment_percent: number;
  stat_sig: string;
  stat_sig_channel_group: string;
  cpc_uplift: number | null;
  win_rate_uplift: number | null;
  additional_clicks: number | null;
  expected_total_clicks: number | null;
  expected_cpc: number | null;
  expected_total_cost: number | null;
  expected_total_binds: number | null;
  additional_expected_binds: number | null;
  expected_cpb: number | null;
  ss_performance: number | null;
  expected_performance: number | null;
  performance_uplift: number | null;
};

type FilterOptionsRow = {
  states: string[];
  segments: string[];
  channel_groups: string[];
};

type PriceExplorationFilterOptionsRow = {
  states: string[];
  channel_groups: string[];
};

type PlanMergedFilterOptionsRow = {
  states: string[];
  segments: string[];
  channel_groups: string[];
  testing_points: number[];
  stat_sig: string[];
};

type StrategyRule = {
  id: number;
  name: string;
  states: string[];
  segments: string[];
  maxCpcUplift: number;
  maxCpbUplift: number;
  corTarget: number;
  growthStrategy: string;
};

type PriceDecisionOverride = {
  state: string;
  channelGroupName: string;
  segment?: string;
  testingPoint: number;
};

type StrategyAnalysisFilters = {
  planId: string;
  startDate?: string;
  endDate?: string;
  activityLeadType?: string;
  qbc?: number;
};

type StrategyBaselineRow = {
  state: string;
  segment: string;
  bids: number | null;
  sold: number | null;
  total_cost: number | null;
  quotes: number | null;
  binds: number | null;
  scored_policies: number | null;
  q2b: number | null;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
};

type StrategyPlanMergedRow = {
  channel_group_name: string;
  state: string;
  segment: string;
  price_adjustment_percent: number | null;
  pe_bids: number | null;
  pe_sold: number | null;
  pe_number_of_quotes: number | null;
  pe_win_rate: number | null;
  pe_total_spend: number | null;
  ss_cpb: number | null;
  expected_cpb: number | null;
  additional_clicks: number | null;
  additional_expected_binds: number | null;
  expected_total_cost: number | null;
};

export type StrategyAnalysisRow = {
  rule_name: string;
  states: string[];
  segments: string[];
  target_cor: number | null;
  bids: number;
  sold: number;
  total_spend: number;
  cpc: number | null;
  wr: number | null;
  quotes: number;
  binds: number;
  current_cpb: number | null;
  expected_cpb: number | null;
  q2b: number | null;
  performance: number | null;
  roe: number | null;
  cor: number | null;
  additional_clicks: number;
  additional_binds: number;
  wr_uplift: number | null;
  cpc_uplift: number | null;
  cpb_uplift: number | null;
  expected_total_cost: number;
  additional_budget: number;
};

export type StateAnalysisSegmentRow = {
  segment: string;
  bids: number;
  sold: number;
  wr: number | null;
  total_spend: number;
  quotes: number;
  sold_to_quotes: number | null;
  binds: number;
  q2b: number | null;
  cor: number | null;
  roe: number | null;
  cpb: number | null;
  performance: number | null;
};

export type StateAnalysisRuleRow = {
  rule_name: string;
  tier: number;
  strategy_key: "aggressive" | "robustic" | "cautious";
  strategy_label: string;
  states: string[];
  segments: string[];
  kpis: StrategyAnalysisRow & { ltv: number | null };
  segment_rows: StateAnalysisSegmentRow[];
};

export type StateAnalysisStateRow = {
  state: string;
  rule_name: string | null;
  tier: number | null;
  strategy_key: "aggressive" | "robustic" | "cautious" | null;
  strategy_label: string | null;
  total_spend: number;
  cor: number | null;
  roe: number | null;
  binds: number;
  performance: number | null;
  additional_clicks: number;
  additional_binds: number;
  additional_budget: number;
  cpc_uplift: number | null;
  cpb_uplift: number | null;
};

export type StateAnalysisResponse = {
  overall: StrategyAnalysisRow & { ltv: number | null };
  states: StateAnalysisStateRow[];
  state_details: Array<{
    state: string;
    rule_name: string | null;
    tier: number | null;
    strategy_key: "aggressive" | "robustic" | "cautious" | null;
    strategy_label: string | null;
    kpis: StrategyAnalysisRow & { ltv: number | null };
    segment_rows: StateAnalysisSegmentRow[];
  }>;
  rules: StateAnalysisRuleRow[];
};

const ALL_US_STATE_CODES = [
  "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
  "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
  "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE",
  "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI",
  "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
];

function withAllStateCodes(states?: string[]): string[] {
  const normalized = (states ?? []).map((value) => String(value || "").trim().toUpperCase()).filter(Boolean);
  return [...new Set([...ALL_US_STATE_CODES, ...normalized])].sort();
}

function parseStrategyRules(raw: string, activityLeadType?: string): StrategyRule[] {
  try {
    const parsed = JSON.parse(raw);
    const scopeKey = normalizeActivityScopeKey(activityLeadType);
    let scopedRules: unknown[] = [];
    if (parsed?.scopes && typeof parsed.scopes === "object") {
      const selectedScope = parsed.scopes?.[scopeKey] || parsed.scopes?.all;
      scopedRules = Array.isArray(selectedScope?.rules) ? selectedScope.rules : [];
    } else if (Array.isArray(parsed?.rules)) {
      if (scopeKey === "clicks_auto" || scopeKey === "all") {
        scopedRules = parsed.rules;
      }
    }
    if (!Array.isArray(scopedRules)) {
      return [];
    }
    return scopedRules
      .map((rule: any, index: number) => ({
        id: Number(rule?.id) || index + 1,
        name: String(rule?.name || "").trim(),
        states: Array.isArray(rule?.states)
          ? rule.states.map((value: string) => String(value || "").trim().toUpperCase()).filter(Boolean)
          : [],
        segments: Array.isArray(rule?.segments)
          ? rule.segments.map((value: string) => String(value || "").trim().toUpperCase()).filter(Boolean)
          : [],
        maxCpcUplift: Number(rule?.maxCpcUplift),
        maxCpbUplift: Number(rule?.maxCpbUplift),
        corTarget: normalizeCorTargetInput(rule?.corTarget),
        growthStrategy: String(rule?.growthStrategy || "balanced").trim().toLowerCase()
      }))
      .filter((rule: StrategyRule) => rule.name && rule.states.length > 0 && rule.segments.length > 0);
  } catch {
    return [];
  }
}

function normalizeCorTargetInput(value: unknown): number {
  const raw = Number(value);
  if (!Number.isFinite(raw) || raw <= 0) {
    return 0;
  }
  // UI label is "(%)" so values like 103.5 are expected and must be normalized to ratio.
  return raw > 2 ? raw / 100 : raw;
}

function extractSegmentFromChannelGroup(channelGroup: string): string {
  const match = String(channelGroup || "").toUpperCase().match(/\b(MCH|MCR|SCH|SCR)\b/);
  return match ? match[1] : "";
}

function buildPriceDecisionOverrideKey(channelGroupName: string, state: string, segment?: string): string {
  return `${String(channelGroupName || "")}|${String(state || "").toUpperCase()}|${String(segment || "").toUpperCase()}`;
}

function parsePriceDecisionOverrides(raw: string, activityLeadType?: string): Map<string, number> {
  const result = new Map<string, number>();
  if (!String(raw || "").trim()) {
    return result;
  }

  try {
    const parsed = JSON.parse(raw);
    const scopeKey = normalizeActivityScopeKey(activityLeadType);
    let scopedOverrides: unknown[] = [];
    if (parsed?.scopes && typeof parsed.scopes === "object") {
      const selectedScope = parsed.scopes?.[scopeKey] || parsed.scopes?.all;
      scopedOverrides = Array.isArray(selectedScope?.overrides) ? selectedScope.overrides : [];
    } else if (Array.isArray(parsed?.overrides)) {
      if (scopeKey === "clicks_auto" || scopeKey === "all") {
        scopedOverrides = parsed.overrides;
      }
    }

    for (const item of scopedOverrides) {
      const entry = item as PriceDecisionOverride;
      const channelGroupName = String(entry?.channelGroupName || "").trim();
      const state = String(entry?.state || "").trim().toUpperCase();
      const segment = String(entry?.segment || "").trim().toUpperCase();
      const testingPoint = Number(entry?.testingPoint);
      if (!channelGroupName || !state || !Number.isFinite(testingPoint)) {
        continue;
      }
      const key = buildPriceDecisionOverrideKey(channelGroupName, state, segment);
      result.set(key, testingPoint);
      if (!segment) {
        const fallbackKey = buildPriceDecisionOverrideKey(channelGroupName, state, "");
        result.set(fallbackKey, testingPoint);
      }
    }
  } catch {
    return result;
  }

  return result;
}

function normalizeUpliftLimit(raw: number, fallback = 0.1): number {
  if (!Number.isFinite(raw)) {
    return fallback;
  }
  const value = raw > 1 ? raw / 100 : raw;
  if (value <= 0) {
    return fallback;
  }
  return value;
}

function toFiniteNumberOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function chooseRecommendedTestingPoint(
  rows: PriceExplorationRow[],
  rule: StrategyRule | null
): number {
  if (!rows.length) {
    return 0;
  }

  const baselineTestingPoint = Number(
    rows.find((row) => Number(row.testing_point) === 0)?.testing_point ?? 0
  );

  if (!rule) {
    const fallbackByFlag = rows.find(
      (row) =>
        Number.isFinite(Number(row.recommended_testing_point)) &&
        Number(row.testing_point) === Number(row.recommended_testing_point)
    );
    return Number.isFinite(Number(fallbackByFlag?.testing_point))
      ? Number(fallbackByFlag?.testing_point)
      : baselineTestingPoint;
  }

  const maxCpc = normalizeUpliftLimit(rule.maxCpcUplift, 0.1);
  const maxCpb = normalizeUpliftLimit(rule.maxCpbUplift, 0.1);
  const strategy = String(rule.growthStrategy || "balanced").toLowerCase();

  const candidates = rows.filter((row) => {
    const tp = Number(row.testing_point);
    const additionalClicks = Number(row.additional_clicks) || 0;
    const cpcUplift = toFiniteNumberOrNull(row.cpc_uplift);
    const cpbUplift = toFiniteNumberOrNull(row.cpb_uplift);
    return (
      tp !== 0 &&
      additionalClicks > 0 &&
      cpcUplift !== null &&
      cpbUplift !== null &&
      cpcUplift <= maxCpc &&
      cpbUplift <= maxCpb
    );
  });

  if (!candidates.length) {
    return baselineTestingPoint;
  }

  const isHighGrowth =
    strategy === "high" || strategy === "high_growth" || strategy === "aggressive";
  const isLowGrowth =
    strategy === "low" || strategy === "cost_focused" || strategy === "cautious";

  candidates.sort((a, b) => {
    const aBinds = Number(a.expected_bind_change) || 0;
    const bBinds = Number(b.expected_bind_change) || 0;
    const aCpc = toFiniteNumberOrNull(a.cpc_uplift) ?? Number.POSITIVE_INFINITY;
    const bCpc = toFiniteNumberOrNull(b.cpc_uplift) ?? Number.POSITIVE_INFINITY;
    const aCpb = toFiniteNumberOrNull(a.cpb_uplift) ?? Number.POSITIVE_INFINITY;
    const bCpb = toFiniteNumberOrNull(b.cpb_uplift) ?? Number.POSITIVE_INFINITY;
    const aClicks = Number(a.additional_clicks) || 0;
    const bClicks = Number(b.additional_clicks) || 0;
    const aTp = Number(a.testing_point) || 0;
    const bTp = Number(b.testing_point) || 0;

    if (isHighGrowth) {
      if (aBinds !== bBinds) return bBinds - aBinds;
      if (aClicks !== bClicks) return bClicks - aClicks;
      if (aCpc !== bCpc) return aCpc - bCpc;
      if (aCpb !== bCpb) return aCpb - bCpb;
      return aTp - bTp;
    }

    if (isLowGrowth) {
      if (aCpc !== bCpc) return aCpc - bCpc;
      if (aCpb !== bCpb) return aCpb - bCpb;
      if (aBinds !== bBinds) return bBinds - aBinds;
      if (aClicks !== bClicks) return bClicks - aClicks;
      return aTp - bTp;
    }

    if (aBinds !== bBinds) return bBinds - aBinds;
    if (aClicks !== bClicks) return bClicks - aClicks;
    if (aCpc !== bCpc) return aCpc - bCpc;
    if (aCpb !== bCpb) return aCpb - bCpb;
    return aTp - bTp;
  });

  return Number(candidates[0]?.testing_point) || baselineTestingPoint;
}

async function getStrategyRulesForPlan(
  planId: string | undefined,
  activityLeadType?: string
): Promise<StrategyRule[]> {
  const normalizedPlanId = String(planId || "").trim();
  if (!normalizedPlanId) {
    return [];
  }
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'plan_strategy_config'
      LIMIT 1
    `,
    { planId: normalizedPlanId }
  );
  return parseStrategyRules(paramRows[0]?.param_value || "", activityLeadType);
}

async function getPriceDecisionOverridesForPlan(
  planId: string | undefined,
  activityLeadType?: string
): Promise<Map<string, number>> {
  const normalizedPlanId = String(planId || "").trim();
  if (!normalizedPlanId) {
    return new Map();
  }
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'price_exploration_decisions'
      LIMIT 1
    `,
    { planId: normalizedPlanId }
  );
  return parsePriceDecisionOverrides(paramRows[0]?.param_value || "", activityLeadType);
}


function normalizeFilters(filters: StateSegmentFilters) {
  const states = (filters.states ?? []).map((value) => value.trim()).filter(Boolean);
  const segments = (filters.segments ?? []).map((value) => value.trim().toUpperCase()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((value) => value.trim()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    states: states.length > 0 ? states : ["__ALL__"],
    segments: segments.length > 0 ? segments : ["__ALL__"],
    channelGroups: channelGroups.length > 0 ? channelGroups : ["__ALL__"],
    activityType: combined.activityType,
    leadType: combined.leadType,
    activityPattern: combined.activityPattern,
    leadPattern: combined.leadPattern,
    stateSegmentActivityType: combined.stateSegmentActivityType,
    stateSegmentLeadType: combined.stateSegmentLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0
  };
}

function normalizePriceExplorationFilters(filters: PriceExplorationFilters) {
  const states = (filters.states ?? []).map((value) => value.trim()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((value) => value.trim()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    q2bStartDate: filters.q2bStartDate || filters.startDate || "",
    q2bEndDate: filters.q2bEndDate || filters.endDate || "",
    states: states.length > 0 ? states : ["__ALL__"],
    channelGroups: channelGroups.length > 0 ? channelGroups : ["__ALL__"],
    activityType: combined.activityType,
    leadType: combined.leadType,
    activityPattern: combined.activityPattern,
    leadPattern: combined.leadPattern,
    stateSegmentActivityType: combined.stateSegmentActivityType,
    stateSegmentLeadType: combined.stateSegmentLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0,
    limit: Number.isFinite(Number(filters.limit)) ? Math.min(Math.max(Number(filters.limit), 1), 200000) : 50000
  };
}

function normalizePlanMergedFilters(filters: PlanMergedFilters) {
  const states = (filters.states ?? []).map((value) => value.trim().toUpperCase()).filter(Boolean);
  const segments = (filters.segments ?? []).map((value) => value.trim().toUpperCase()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((value) => value.trim()).filter(Boolean);
  const testingPoints = (filters.testingPoints ?? [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  const statSig = (filters.statSig ?? []).map((value) => value.trim().toLowerCase()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    states: states.length > 0 ? states : ["__ALL__"],
    segments: segments.length > 0 ? segments : ["__ALL__"],
    channelGroups: channelGroups.length > 0 ? channelGroups : ["__ALL__"],
    testingPoints: testingPoints.length > 0 ? testingPoints : [999999999],
    statSig: statSig.length > 0 ? statSig : ["__ALL__"],
    activityType: combined.activityType,
    leadType: combined.leadType,
    activityPattern: combined.activityPattern,
    leadPattern: combined.leadPattern
  };
}

export async function listStateSegmentFilters(filters: StateSegmentFilters): Promise<FilterOptionsRow> {
  const normalized = normalizeFilters(filters);

  const rows = await query<FilterOptionsRow>(
    `
      WITH scoped AS (
        SELECT
          DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
          Data_State AS state,
          COALESCE(
            NULLIF(TRIM(ChannelGroupName), ''),
            NULLIF(TRIM(CAST(Account_Name AS STRING)), ''),
            ''
          ) AS channel_group_name,
          UPPER(
            COALESCE(
              NULLIF(TRIM(Segments), ''),
              REGEXP_EXTRACT(UPPER(COALESCE(ChannelGroupName, '')), r'(MCH|MCR|SCH|SCR)')
            )
          ) AS segment
        FROM ${RAW_CROSS_TACTIC_TABLE}
        WHERE (@startDate = "" OR DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) >= DATE(@startDate))
          AND (@endDate = "" OR DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) <= DATE(@endDate))
          AND (@stateSegmentActivityType = "" OR LOWER(activitytype) = LOWER(@stateSegmentActivityType))
          AND (@stateSegmentLeadType = "" OR LOWER(Leadtype) = LOWER(@stateSegmentLeadType))
      )
      SELECT
        ARRAY(
          SELECT DISTINCT state
          FROM scoped
          WHERE state IS NOT NULL
          ORDER BY state
        ) AS states,
        ARRAY(
          SELECT DISTINCT segment
          FROM scoped
          WHERE segment IS NOT NULL AND segment IN ('MCH', 'MCR', 'SCH', 'SCR')
          ORDER BY segment
        ) AS segments,
        ARRAY(
          SELECT DISTINCT channel_group_name
          FROM scoped
          WHERE channel_group_name IS NOT NULL AND channel_group_name != ''
          ORDER BY channel_group_name
        ) AS channel_groups
    `,
    normalized
  );

  const first = rows[0];
  return {
    states: withAllStateCodes(first?.states),
    segments: first?.segments ?? [],
    channel_groups: first?.channel_groups ?? []
  };
}

export async function getStateSegmentPerformance(
  filters: StateSegmentFilters
): Promise<StateSegmentPerformanceRow[]> {
  const normalized = normalizeFilters(filters);

  return query<StateSegmentPerformanceRow>(
    `
      WITH base AS (
        SELECT
          DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
          Data_State AS state,
          COALESCE(
            NULLIF(TRIM(ChannelGroupName), ''),
            NULLIF(TRIM(CAST(Account_Name AS STRING)), ''),
            ''
          ) AS channel_group_name,
          UPPER(
            COALESCE(
              NULLIF(TRIM(Segments), ''),
              REGEXP_EXTRACT(
                UPPER(
                  COALESCE(
                    NULLIF(TRIM(ChannelGroupName), ''),
                    NULLIF(TRIM(CAST(Account_Name AS STRING)), ''),
                    ''
                  )
                ),
                r'(MCH|MCR|SCH|SCR)'
              )
            )
          ) AS segment,
          SAFE_CAST(bid_count AS FLOAT64) AS bid_count,
          SAFE_CAST(Transaction_sold AS FLOAT64) AS transaction_sold,
          SAFE_CAST(TransactionSold AS FLOAT64) AS transaction_sold_alt,
          SAFE_CAST(Price AS FLOAT64) AS price,
          SAFE_CAST(TotalQuotes AS FLOAT64) AS total_quote,
          SAFE_CAST(TotalBinds AS FLOAT64) AS total_binds,
          SAFE_CAST(AutoOnlineQuotesStart AS FLOAT64) AS quote_started,
          SAFE_CAST(ScoredPolicies AS FLOAT64) AS scored_policies,
          SAFE_CAST(LifetimePremium AS FLOAT64) AS lifetime_premium,
          SAFE_CAST(LifeTimeCost AS FLOAT64) AS lifetime_cost,
          SAFE_CAST(CustomValues_Mrltv AS FLOAT64) AS avg_mrltv,
          SAFE_CAST(CustomValues_Profit AS FLOAT64) AS avg_profit,
          SAFE_CAST(Equity AS FLOAT64) AS avg_equity,
          SAFE_CAST(Target_TargetCPB AS FLOAT64) AS target_cpb
        FROM ${RAW_CROSS_TACTIC_TABLE}
        WHERE (@stateSegmentActivityType = "" OR LOWER(activitytype) = LOWER(@stateSegmentActivityType))
          AND (@stateSegmentLeadType = "" OR LOWER(Leadtype) = LOWER(@stateSegmentLeadType))
      )
      SELECT
        state,
        segment,
        channel_group_name,
        SUM(COALESCE(bid_count, 0)) AS bids,
        SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
        SUM(COALESCE(price, 0)) AS total_cost,
        SUM(COALESCE(quote_started, 0)) AS quote_started,
        SUM(COALESCE(total_quote, 0)) AS quotes,
        SUM(COALESCE(total_binds, 0)) AS binds,
        SAFE_DIVIDE(
          SUM(COALESCE(total_binds, 0)),
          NULLIF(SUM(COALESCE(total_quote, 0)), 0)
        ) AS q2b_score,
        SUM(COALESCE(scored_policies, 0)) AS scored_policies,
        SAFE_DIVIDE(
          SUM(COALESCE(price, 0)),
          NULLIF(SUM(COALESCE(total_binds, 0)), 0)
        ) AS cpb,
        CASE
          WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
          ELSE SAFE_DIVIDE(
            SUM(COALESCE(target_cpb, 0)),
            SUM(COALESCE(total_binds, 0))
          )
        END AS target_cpb,
        SAFE_DIVIDE(
          CASE
            WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
            ELSE SAFE_DIVIDE(
              SUM(COALESCE(target_cpb, 0)),
              SUM(COALESCE(total_binds, 0))
            )
          END,
          SAFE_DIVIDE(
            SUM(COALESCE(price, 0)),
            NULLIF(SUM(COALESCE(total_binds, 0)), 0)
          )
        ) AS performance,
        ${buildRoeSql({
          zeroConditions: [
            "SUM(COALESCE(scored_policies, 0)) = 0",
            "SAFE_DIVIDE(SUM(COALESCE(avg_equity, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) = 0"
          ],
          avgProfitExpr: "SAFE_DIVIDE(SUM(COALESCE(avg_profit, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))",
          cpbExpr: "SAFE_DIVIDE(SUM(COALESCE(price, 0)), NULLIF(SUM(COALESCE(total_binds, 0)), 0))",
          avgEquityExpr: "SAFE_DIVIDE(SUM(COALESCE(avg_equity, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))"
        })} AS roe,
        ${buildCombinedRatioSql({
          zeroConditions: [
            "SUM(COALESCE(scored_policies, 0)) = 0",
            "SAFE_DIVIDE(SUM(COALESCE(lifetime_premium, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) = 0"
          ],
          cpbExpr: "SAFE_DIVIDE(SUM(COALESCE(price, 0)), NULLIF(SUM(COALESCE(total_binds, 0)), 0))",
          avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(COALESCE(lifetime_cost, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))",
          avgLifetimePremiumExpr:
            "SAFE_DIVIDE(SUM(COALESCE(lifetime_premium, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))"
        })} AS combined_ratio,
        SAFE_DIVIDE(
          SUM(COALESCE(avg_mrltv, 0) * COALESCE(scored_policies, 0)),
          NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
        ) AS mrltv,
        SAFE_DIVIDE(
          SUM(COALESCE(avg_profit, 0)),
          NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
        ) AS profit,
        SAFE_DIVIDE(
          SUM(COALESCE(avg_equity, 0)),
          NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
        ) AS equity
      FROM base
      WHERE (@startDate = "" OR event_date >= DATE(@startDate))
        AND (@endDate = "" OR event_date <= DATE(@endDate))
        AND ("__ALL__" IN UNNEST(@states) OR state IN UNNEST(@states))
        AND ("__ALL__" IN UNNEST(@segments) OR segment IN UNNEST(@segments))
        AND ("__ALL__" IN UNNEST(@channelGroups) OR channel_group_name IN UNNEST(@channelGroups))
        AND segment IN ('MCH', 'MCR', 'SCH', 'SCR')
      GROUP BY state, segment, channel_group_name
      ORDER BY state, segment, channel_group_name
    `,
    normalized
  );
}

export async function listPriceExplorationFilters(
  filters: Pick<PriceExplorationFilters, "startDate" | "endDate" | "activityLeadType">
): Promise<{ states: string[]; channelGroups: string[] }> {
  const normalized = normalizePriceExplorationFilters(filters);

  const rows = await query<PriceExplorationFilterOptionsRow>(
    `
      WITH raw AS (
        SELECT
          Data_State AS state,
          ChannelGroupName AS channel_group_name,
          LOWER(COALESCE(activitytype, '')) AS activity_type_raw,
          LOWER(COALESCE(Leadtype, '')) AS lead_type_raw
        FROM ${RAW_CROSS_TACTIC_TABLE}
        WHERE Data_State IS NOT NULL
          AND ChannelGroupName IS NOT NULL
          AND SAFE_CAST(PriceAdjustmentPercent AS INT64) IS NOT NULL
          AND (@startDate = "" OR DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) >= DATE(@startDate))
          AND (@endDate = "" OR DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) <= DATE(@endDate))
      ),
      scoped AS (
        SELECT
          state,
          channel_group_name,
          CASE
            WHEN activity_type_raw LIKE 'click%' THEN 'clicks'
            WHEN activity_type_raw LIKE 'lead%' THEN 'leads'
            WHEN activity_type_raw LIKE 'call%' THEN 'calls'
            ELSE ''
          END AS activity_group,
          CASE
            WHEN lead_type_raw LIKE '%car%' THEN 'auto'
            WHEN lead_type_raw LIKE '%home%' THEN 'home'
            ELSE ''
          END AS lead_group
        FROM raw
      )
      SELECT
        ARRAY(
          SELECT DISTINCT state
          FROM scoped
          WHERE state IS NOT NULL
            AND (@activityType = "" OR activity_group = @activityType)
            AND (@leadType = "" OR lead_group = @leadType)
          ORDER BY state
        ) AS states,
        ARRAY(
          SELECT DISTINCT channel_group_name
          FROM scoped
          WHERE channel_group_name IS NOT NULL
            AND (@activityType = "" OR activity_group = @activityType)
            AND (@leadType = "" OR lead_group = @leadType)
          ORDER BY channel_group_name
        ) AS channel_groups
    `,
    normalized
  );

  const first = rows[0];
  return {
    states: withAllStateCodes(first?.states),
    channelGroups: first?.channel_groups ?? []
  };
}

export async function getPriceExploration(
  filters: PriceExplorationFilters
): Promise<PriceExplorationRow[]> {
  const normalized = normalizePriceExplorationFilters(filters);

  const rows = await query<PriceExplorationRow>(
    `
      WITH raw_all AS (
        SELECT
          ChannelGroupName AS channel_group_name,
          Data_State AS state,
          SAFE_CAST(PriceAdjustmentPercent AS INT64) AS price_adjustment_percent,
          Lead_LeadID,
          SAFE_CAST(bid_count AS FLOAT64) AS bid_count,
          SAFE_CAST(ExtraBidData_ReturnedAdsCount AS FLOAT64) AS returned_ads_count,
          SAFE_CAST(ExtraBidData_OriginalAdData_Position AS FLOAT64) AS ad_position,
          SAFE_CAST(Transaction_sold AS FLOAT64) AS transaction_sold,
          SAFE_CAST(TransactionSold AS FLOAT64) AS transaction_sold_alt,
          SAFE_CAST(bid_price AS FLOAT64) AS bid_price,
          SAFE_CAST(Price AS FLOAT64) AS price,
          SAFE_CAST(AutoOnlineQuotesStart AS FLOAT64) AS quote_started,
          SAFE_CAST(TotalQuotes AS FLOAT64) AS total_quotes,
          SAFE_CAST(TotalBinds AS FLOAT64) AS total_binds,
          SAFE_CAST(ScoredPolicies AS FLOAT64) AS scored_policies,
          SAFE_CAST(Target_TargetCPB AS FLOAT64) AS target_cpb,
          SAFE_CAST(CustomValues_Profit AS FLOAT64) AS avg_profit,
          SAFE_CAST(Equity AS FLOAT64) AS avg_equity,
          SAFE_CAST(LifetimePremium AS FLOAT64) AS lifetime_premium,
          SAFE_CAST(LifeTimeCost AS FLOAT64) AS lifetime_cost,
          LOWER(COALESCE(activitytype, '')) AS activity_type_raw,
          LOWER(COALESCE(Leadtype, '')) AS lead_type_raw
        FROM ${RAW_CROSS_TACTIC_TABLE}
        WHERE DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) BETWEEN
            IF(@startDate = "", DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY), DATE(@startDate))
            AND IF(@endDate = "", CURRENT_DATE(), DATE(@endDate))
          AND Data_State IS NOT NULL
          AND ChannelGroupName IS NOT NULL
          AND SAFE_CAST(PriceAdjustmentPercent AS INT64) IS NOT NULL
          AND ("__ALL__" IN UNNEST(@channelGroups) OR ChannelGroupName IN UNNEST(@channelGroups))
      ),
      base_all AS (
        SELECT
          channel_group_name,
          state,
          price_adjustment_percent,
          Lead_LeadID,
          bid_count,
          returned_ads_count,
          ad_position,
          transaction_sold,
          transaction_sold_alt,
          bid_price,
          price,
          quote_started,
          total_quotes,
          total_binds,
          scored_policies,
          target_cpb,
          avg_profit,
          avg_equity,
          lifetime_premium,
          lifetime_cost,
          CASE
            WHEN activity_type_raw LIKE 'click%' THEN 'clicks'
            WHEN activity_type_raw LIKE 'lead%' THEN 'leads'
            WHEN activity_type_raw LIKE 'call%' THEN 'calls'
            ELSE ''
          END AS activity_group,
          CASE
            WHEN lead_type_raw LIKE '%car%' THEN 'auto'
            WHEN lead_type_raw LIKE '%home%' THEN 'home'
            ELSE ''
          END AS lead_group
        FROM raw_all
        WHERE (@activityType = "" OR (
          CASE
            WHEN activity_type_raw LIKE 'click%' THEN 'clicks'
            WHEN activity_type_raw LIKE 'lead%' THEN 'leads'
            WHEN activity_type_raw LIKE 'call%' THEN 'calls'
            ELSE ''
          END
        ) = @activityType)
          AND (@leadType = "" OR (
            CASE
              WHEN lead_type_raw LIKE '%car%' THEN 'auto'
              WHEN lead_type_raw LIKE '%home%' THEN 'home'
              ELSE ''
            END
          ) = @leadType)
      ),
      base_filtered AS (
        SELECT *
        FROM base_all
        WHERE ("__ALL__" IN UNNEST(@states) OR state IN UNNEST(@states))
      ),
      state_tp AS (
        SELECT
          channel_group_name,
          state,
          activity_group,
          lead_group,
          price_adjustment_percent,
          COUNT(DISTINCT Lead_LeadID) AS opps,
          SUM(COALESCE(bid_count, 0)) AS bids,
          SUM(COALESCE(returned_ads_count, 0)) AS total_impressions,
          AVG(COALESCE(ad_position, 0)) AS avg_position,
          SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
          SAFE_DIVIDE(
            SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)),
            NULLIF(SUM(COALESCE(bid_count, 0)), 0)
          ) AS win_rate,
          AVG(COALESCE(bid_price, 0)) AS avg_bid,
          SAFE_DIVIDE(
            SUM(COALESCE(price, 0)),
            NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
          ) AS cpc,
          SUM(COALESCE(price, 0)) AS total_spend,
          SAFE_DIVIDE(
            SUM(COALESCE(total_quotes, 0)),
            NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
          ) AS click_to_quote,
          SAFE_DIVIDE(
            SUM(COALESCE(quote_started, 0)),
            NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
          ) AS quote_start_rate,
          SUM(COALESCE(quote_started, 0)) AS number_of_quote_started,
          SUM(COALESCE(total_quotes, 0)) AS number_of_quotes,
          SUM(COALESCE(total_binds, 0)) AS number_of_binds,
          SUM(COALESCE(scored_policies, 0)) AS scored_policies,
          SUM(COALESCE(target_cpb, 0)) AS target_cpb_total,
          SUM(COALESCE(avg_profit, 0)) AS avg_profit_total,
          SUM(COALESCE(avg_equity, 0)) AS avg_equity_total,
          SUM(COALESCE(lifetime_premium, 0)) AS lifetime_premium_total,
          SUM(COALESCE(lifetime_cost, 0)) AS lifetime_cost_total
        FROM base_filtered
        GROUP BY 1, 2, 3, 4, 5
      ),
      channel_tp AS (
        SELECT
          channel_group_name,
          activity_group,
          lead_group,
          price_adjustment_percent,
          SUM(bids) AS channel_bids,
          SUM(sold) AS channel_sold,
          SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS channel_win_rate,
          SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS channel_cpc
        FROM (
          SELECT
            channel_group_name,
            activity_group,
            lead_group,
            price_adjustment_percent,
            SUM(COALESCE(bid_count, 0)) AS bids,
            SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
            SUM(COALESCE(price, 0)) AS total_spend
          FROM base_all
          GROUP BY 1, 2, 3, 4
        )
        GROUP BY 1, 2, 3, 4
      ),
      joined AS (
        SELECT
          s.*,
          b.win_rate AS baseline_win_rate,
          b.cpc AS baseline_cpc,
          b.bids AS baseline_bids,
          b.sold AS baseline_sold,
          c.channel_bids,
          c.channel_sold,
          c.channel_win_rate,
          c.channel_cpc,
          cb.channel_win_rate AS channel_baseline_win_rate,
          cb.channel_cpc AS channel_baseline_cpc,
          cb.channel_bids AS channel_baseline_bids,
          cb.channel_sold AS channel_baseline_sold
        FROM state_tp s
        LEFT JOIN state_tp b
          ON b.channel_group_name = s.channel_group_name
         AND b.state = s.state
         AND b.activity_group = s.activity_group
         AND b.lead_group = s.lead_group
         AND b.price_adjustment_percent = 0
        LEFT JOIN channel_tp c
          ON c.channel_group_name = s.channel_group_name
         AND c.activity_group = s.activity_group
         AND c.lead_group = s.lead_group
         AND c.price_adjustment_percent = s.price_adjustment_percent
        LEFT JOIN channel_tp cb
          ON cb.channel_group_name = s.channel_group_name
         AND cb.activity_group = s.activity_group
         AND cb.lead_group = s.lead_group
         AND cb.price_adjustment_percent = 0
      ),
      scored AS (
        SELECT
          *,
          SAFE_DIVIDE(
            win_rate - baseline_win_rate,
            SQRT(
              SAFE_DIVIDE((sold + baseline_sold), NULLIF((bids + baseline_bids), 0))
              * (1 - SAFE_DIVIDE((sold + baseline_sold), NULLIF((bids + baseline_bids), 0)))
              * (SAFE_DIVIDE(1, NULLIF(bids, 0)) + SAFE_DIVIDE(1, NULLIF(baseline_bids, 0)))
            )
          ) AS z_state,
          SAFE_DIVIDE(
            channel_win_rate - channel_baseline_win_rate,
            SQRT(
              SAFE_DIVIDE((channel_sold + channel_baseline_sold), NULLIF((channel_bids + channel_baseline_bids), 0))
              * (
                1 - SAFE_DIVIDE((channel_sold + channel_baseline_sold), NULLIF((channel_bids + channel_baseline_bids), 0))
              )
              * (SAFE_DIVIDE(1, NULLIF(channel_bids, 0)) + SAFE_DIVIDE(1, NULLIF(channel_baseline_bids, 0)))
            )
          ) AS z_channel
        FROM joined
      ),
      per_group AS (
        SELECT
          channel_group_name,
          state,
          activity_group,
          lead_group,
          price_adjustment_percent AS testing_point,
          opps,
          bids,
          sold,
          number_of_binds,
          scored_policies,
          target_cpb_total,
          avg_profit_total,
          avg_equity_total,
          lifetime_premium_total,
          lifetime_cost_total,
          number_of_quotes,
          avg_bid,
          cpc,
          total_spend,
          CASE
            WHEN price_adjustment_percent = 0 THEN 'baseline'
            WHEN ABS(z_state) >= 2.58 THEN 'high'
            WHEN ABS(z_state) >= 1.96 THEN 'mid'
            ELSE 'low'
          END AS stat_sig,
          CASE
            WHEN price_adjustment_percent = 0 THEN 'baseline'
            WHEN ABS(z_channel) >= 2.58 THEN 'high'
            WHEN ABS(z_channel) >= 1.96 THEN 'mid'
            ELSE 'low'
          END AS stat_sig_channel_group,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(cpc - baseline_cpc, NULLIF(baseline_cpc, 0))
          END AS cpc_uplift_state,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(win_rate - baseline_win_rate, NULLIF(baseline_win_rate, 0))
          END AS win_rate_uplift_state,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(channel_cpc - channel_baseline_cpc, NULLIF(channel_baseline_cpc, 0))
          END AS cpc_uplift_channel,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(channel_win_rate - channel_baseline_win_rate, NULLIF(channel_baseline_win_rate, 0))
          END AS win_rate_uplift_channel,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE ((CASE WHEN ABS(z_state) >= 1.96 THEN win_rate ELSE channel_win_rate END) - baseline_win_rate) * bids
          END AS additional_clicks
        FROM scored
      ),
      final_agg AS (
        SELECT
          channel_group_name,
          state,
          testing_point,
          SUM(opps) AS opps,
          SUM(bids) AS bids,
          SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS win_rate,
          SUM(sold) AS sold,
          SUM(number_of_binds) AS binds,
          SUM(scored_policies) AS scored_policies,
          SUM(target_cpb_total) AS target_cpb_total,
          SUM(avg_profit_total) AS avg_profit_total,
          SUM(avg_equity_total) AS avg_equity_total,
          SUM(lifetime_premium_total) AS lifetime_premium_total,
          SUM(lifetime_cost_total) AS lifetime_cost_total,
          SUM(number_of_quotes) AS quotes,
          SAFE_DIVIDE(SUM(number_of_quotes), NULLIF(SUM(bids), 0)) AS click_to_quote,
          SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS cpc,
          SAFE_DIVIDE(SUM(avg_bid * bids), NULLIF(SUM(bids), 0)) AS avg_bid,
          SUM(total_spend) AS total_spend,
          CASE
            WHEN SUM(number_of_binds) = 0 THEN 0
            ELSE SAFE_DIVIDE(SUM(target_cpb_total), SUM(number_of_binds))
          END AS target_cpb,
          SAFE_DIVIDE(
            CASE
              WHEN SUM(number_of_binds) = 0 THEN 0
              ELSE SAFE_DIVIDE(SUM(target_cpb_total), SUM(number_of_binds))
            END,
            SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(number_of_binds), 0))
          ) AS performance,
          ${buildRoeSql({
            zeroConditions: [
              "SUM(scored_policies) = 0",
              "SAFE_DIVIDE(SUM(avg_equity_total), NULLIF(SUM(scored_policies), 0)) = 0"
            ],
            avgProfitExpr: "SAFE_DIVIDE(SUM(avg_profit_total), NULLIF(SUM(scored_policies), 0))",
            cpbExpr: "SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(number_of_binds), 0))",
            avgEquityExpr: "SAFE_DIVIDE(SUM(avg_equity_total), NULLIF(SUM(scored_policies), 0))"
          })} AS roe,
          ${buildCombinedRatioSql({
            zeroConditions: [
              "SUM(scored_policies) = 0",
              "SAFE_DIVIDE(SUM(lifetime_premium_total), NULLIF(SUM(scored_policies), 0)) = 0"
            ],
            cpbExpr: "SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(number_of_binds), 0))",
            avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(lifetime_cost_total), NULLIF(SUM(scored_policies), 0))",
            avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(lifetime_premium_total), NULLIF(SUM(scored_policies), 0))"
          })} AS combined_ratio,
          SAFE_DIVIDE(
            SUM(IF(win_rate_uplift_state IS NULL, 0, win_rate_uplift_state * bids)),
            NULLIF(SUM(IF(win_rate_uplift_state IS NULL, 0, bids)), 0)
          ) AS win_rate_uplift_state,
          SAFE_DIVIDE(
            SUM(IF(cpc_uplift_state IS NULL, 0, cpc_uplift_state * sold)),
            NULLIF(SUM(IF(cpc_uplift_state IS NULL, 0, sold)), 0)
          ) AS cpc_uplift_state,
          SAFE_DIVIDE(
            SUM(IF(win_rate_uplift_channel IS NULL, 0, win_rate_uplift_channel * bids)),
            NULLIF(SUM(IF(win_rate_uplift_channel IS NULL, 0, bids)), 0)
          ) AS win_rate_uplift_channel,
          SAFE_DIVIDE(
            SUM(IF(cpc_uplift_channel IS NULL, 0, cpc_uplift_channel * sold)),
            NULLIF(SUM(IF(cpc_uplift_channel IS NULL, 0, sold)), 0)
          ) AS cpc_uplift_channel,
          SUM(COALESCE(additional_clicks, 0)) AS additional_clicks,
          CASE
            WHEN testing_point = 0 THEN 'baseline'
            WHEN COUNTIF(stat_sig = 'high') > 0 THEN 'high'
            WHEN COUNTIF(stat_sig = 'mid') > 0 THEN 'mid'
            ELSE 'low'
          END AS stat_sig,
          CASE
            WHEN testing_point = 0 THEN 'baseline'
            WHEN COUNTIF(stat_sig_channel_group = 'high') > 0 THEN 'high'
            WHEN COUNTIF(stat_sig_channel_group = 'mid') > 0 THEN 'mid'
            ELSE 'low'
          END AS stat_sig_channel_group
        FROM per_group
        GROUP BY channel_group_name, state, testing_point
      ),
      with_budget AS (
        SELECT
          *,
          SUM(bids) OVER (PARTITION BY channel_group_name, state) AS total_bids_channel_state,
          SUM(sold) OVER (PARTITION BY channel_group_name, state) AS current_sold_channel_state,
          SUM(quotes) OVER (PARTITION BY channel_group_name, state) AS channel_quote,
          SUM(total_spend) OVER (PARTITION BY channel_group_name, state) AS current_spend_channel_state,
          SUM(quotes) OVER (PARTITION BY state, testing_point) AS state_quotes,
          SUM(sold) OVER (PARTITION BY state, testing_point) AS state_sold
        FROM final_agg
      ),
      with_expected AS (
        SELECT
          *,
          MAX(IF(testing_point = 0, win_rate, NULL)) OVER (PARTITION BY channel_group_name, state)
            AS baseline_win_rate_channel_state,
          MAX(IF(testing_point = 0, cpc, NULL)) OVER (PARTITION BY channel_group_name, state)
            AS baseline_cpc_channel_state,
          (win_rate * total_bids_channel_state) AS expected_clicks,
          (win_rate * total_bids_channel_state * cpc) AS expected_total_cost
        FROM with_budget
      ),
      state_channel_binds AS (
        SELECT
          channel_group_name,
          state,
          SUM(COALESCE(total_binds, 0)) AS binds_state_channel
        FROM base_filtered
        GROUP BY channel_group_name, state
      ),
      state_channel_financials AS (
        SELECT
          channel_group_name,
          state,
          SAFE_DIVIDE(SUM(COALESCE(avg_profit, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_profit,
          SAFE_DIVIDE(SUM(COALESCE(avg_equity, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_equity,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_premium, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_premium,
          SAFE_DIVIDE(SUM(COALESCE(lifetime_cost, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_lifetime_cost
        FROM base_filtered
        GROUP BY channel_group_name, state
      ),
      channel_binds AS (
        SELECT
          channel_group_name,
          SUM(COALESCE(total_binds, 0)) AS channel_binds
        FROM base_all
        GROUP BY channel_group_name
      ),
      q2b_source AS (
        SELECT
          state,
          segment,
          SAFE_DIVIDE(SUM(COALESCE(total_binds, 0)), NULLIF(SUM(COALESCE(total_quote, 0)), 0)) AS q2b
        FROM ${analyticsTable("v_state_segment_performance_daily")}
        WHERE (@q2bStartDate = "" OR event_date >= DATE(@q2bStartDate))
          AND (@q2bEndDate = "" OR event_date <= DATE(@q2bEndDate))
        GROUP BY state, segment
      ),
      q2b_channel AS (
        SELECT
          segment,
          SAFE_DIVIDE(SUM(COALESCE(total_binds, 0)), NULLIF(SUM(COALESCE(total_quote, 0)), 0)) AS channel_q2b
        FROM ${analyticsTable("v_state_segment_performance_daily")}
        WHERE (@q2bStartDate = "" OR event_date >= DATE(@q2bStartDate))
          AND (@q2bEndDate = "" OR event_date <= DATE(@q2bEndDate))
        GROUP BY segment
      ),
      final_rows AS (
        SELECT
          with_expected.channel_group_name,
          with_expected.state,
          testing_point,
          opps,
          bids,
          win_rate,
          sold,
          state_channel_binds.binds_state_channel AS binds,
          quotes,
          click_to_quote,
          channel_quote,
          SAFE_DIVIDE(channel_quote, NULLIF(total_bids_channel_state, 0)) AS click_to_channel_quote,
          q2b_source.q2b,
          channel_binds.channel_binds,
          q2b_channel.channel_q2b,
          cpc,
          avg_bid,
          win_rate_uplift_state,
          cpc_uplift_state,
          win_rate_uplift_channel,
          cpc_uplift_channel,
          performance,
          roe,
          combined_ratio,
          CASE
            WHEN testing_point = 0 THEN NULL
            WHEN stat_sig = 'low' THEN win_rate_uplift_channel
            ELSE win_rate_uplift_state
          END AS win_rate_uplift,
          CASE
            WHEN testing_point = 0 THEN NULL
            WHEN stat_sig = 'low' THEN cpc_uplift_channel
            ELSE cpc_uplift_state
          END AS cpc_uplift,
          CASE
            WHEN testing_point = 0 THEN 0
            ELSE
              (
                (win_rate * total_bids_channel_state)
                - (COALESCE(baseline_win_rate_channel_state, 0) * total_bids_channel_state)
              )
          END AS additional_clicks,
          (
            CASE
              WHEN testing_point = 0 THEN 0
              ELSE
                (
                  (win_rate * total_bids_channel_state)
                  - (COALESCE(baseline_win_rate_channel_state, 0) * total_bids_channel_state)
                )
            END
          )
          * (
            CASE
              WHEN COALESCE(quotes, 0) >= 10 AND SAFE_DIVIDE(quotes, NULLIF(sold, 0)) IS NOT NULL
                THEN SAFE_DIVIDE(quotes, NULLIF(sold, 0))
              WHEN SAFE_DIVIDE(state_quotes, NULLIF(state_sold, 0)) IS NOT NULL
                THEN SAFE_DIVIDE(state_quotes, NULLIF(state_sold, 0))
              ELSE SAFE_DIVIDE(channel_quote, NULLIF(total_bids_channel_state, 0))
            END
          )
          * (
            CASE
              WHEN testing_point = 0 THEN 0
              WHEN COALESCE(state_channel_binds.binds_state_channel, 0) >= 5 AND q2b_source.q2b IS NOT NULL
                THEN q2b_source.q2b
              ELSE COALESCE(q2b_channel.channel_q2b, 0)
            END
          ) AS expected_bind_change,
          (expected_total_cost - current_spend_channel_state) AS additional_budget_needed,
          SAFE_DIVIDE(
            current_spend_channel_state,
            NULLIF(state_channel_binds.binds_state_channel, 0)
          ) AS current_cpb,
          SAFE_DIVIDE(
            expected_total_cost,
            NULLIF(
              state_channel_binds.binds_state_channel
              + (
                (
                  CASE
                    WHEN testing_point = 0 THEN 0
                    ELSE
                      (
                        (win_rate * total_bids_channel_state)
                        - (COALESCE(baseline_win_rate_channel_state, 0) * total_bids_channel_state)
                      )
                  END
                )
                * (
                  CASE
                    WHEN COALESCE(quotes, 0) >= 10 AND SAFE_DIVIDE(quotes, NULLIF(sold, 0)) IS NOT NULL
                      THEN SAFE_DIVIDE(quotes, NULLIF(sold, 0))
                    WHEN SAFE_DIVIDE(state_quotes, NULLIF(state_sold, 0)) IS NOT NULL
                      THEN SAFE_DIVIDE(state_quotes, NULLIF(state_sold, 0))
                    ELSE SAFE_DIVIDE(channel_quote, NULLIF(total_bids_channel_state, 0))
                  END
                )
                * (
                  CASE
                    WHEN testing_point = 0 THEN 0
                    WHEN COALESCE(state_channel_binds.binds_state_channel, 0) >= 5 AND q2b_source.q2b IS NOT NULL
                      THEN q2b_source.q2b
                    ELSE COALESCE(q2b_channel.channel_q2b, 0)
                  END
                )
              ),
              0
            )
          ) AS expected_cpb,
          SAFE_DIVIDE(
            SAFE_DIVIDE(
              expected_total_cost,
              NULLIF(
                state_channel_binds.binds_state_channel
                + (
                  (
                    CASE
                      WHEN testing_point = 0 THEN 0
                      ELSE
                        (
                          (win_rate * total_bids_channel_state)
                          - (COALESCE(baseline_win_rate_channel_state, 0) * total_bids_channel_state)
                        )
                    END
                  )
                  * (
                    CASE
                      WHEN COALESCE(quotes, 0) >= 10 AND SAFE_DIVIDE(quotes, NULLIF(sold, 0)) IS NOT NULL
                        THEN SAFE_DIVIDE(quotes, NULLIF(sold, 0))
                      WHEN SAFE_DIVIDE(state_quotes, NULLIF(state_sold, 0)) IS NOT NULL
                        THEN SAFE_DIVIDE(state_quotes, NULLIF(state_sold, 0))
                      ELSE SAFE_DIVIDE(channel_quote, NULLIF(total_bids_channel_state, 0))
                    END
                  )
                  * (
                    CASE
                      WHEN testing_point = 0 THEN 0
                      WHEN COALESCE(state_channel_binds.binds_state_channel, 0) >= 5 AND q2b_source.q2b IS NOT NULL
                        THEN q2b_source.q2b
                      ELSE COALESCE(q2b_channel.channel_q2b, 0)
                    END
                  )
                ),
                0
              )
            )
            - SAFE_DIVIDE(
                current_spend_channel_state,
                NULLIF(state_channel_binds.binds_state_channel, 0)
              ),
            NULLIF(
              SAFE_DIVIDE(
                current_spend_channel_state,
                NULLIF(state_channel_binds.binds_state_channel, 0)
              ),
              0
            )
          ) AS cpb_uplift,
          stat_sig,
          stat_sig_channel_group,
          CASE
            WHEN testing_point = 0 THEN 'baseline'
            WHEN stat_sig = 'low' THEN 'channel only'
            ELSE 'channel & state'
          END AS stat_sig_source
        FROM with_expected
        LEFT JOIN q2b_source
          ON q2b_source.state = with_expected.state
         AND q2b_source.segment = REGEXP_EXTRACT(UPPER(with_expected.channel_group_name), r'(MCH|MCR|SCH|SCR)')
        LEFT JOIN state_channel_binds
          ON state_channel_binds.channel_group_name = with_expected.channel_group_name
         AND state_channel_binds.state = with_expected.state
        LEFT JOIN channel_binds
          ON channel_binds.channel_group_name = with_expected.channel_group_name
        LEFT JOIN q2b_channel
          ON q2b_channel.segment = REGEXP_EXTRACT(UPPER(with_expected.channel_group_name), r'(MCH|MCR|SCH|SCR)')
      ),
      final_rows_scoped AS (
        SELECT
          final_rows.* EXCEPT (roe, combined_ratio),
          ${buildRoeSql({
            zeroConditions: [
              "final_rows.expected_cpb IS NULL",
              "state_channel_financials.avg_equity IS NULL",
              "state_channel_financials.avg_equity = 0"
            ],
            avgProfitExpr: "state_channel_financials.avg_profit",
            cpbExpr: "final_rows.expected_cpb",
            avgEquityExpr: "state_channel_financials.avg_equity"
          })} AS roe,
          ${buildCombinedRatioSql({
            zeroConditions: [
              "final_rows.expected_cpb IS NULL",
              "state_channel_financials.avg_lifetime_premium IS NULL",
              "state_channel_financials.avg_lifetime_premium = 0"
            ],
            cpbExpr: "final_rows.expected_cpb",
            avgLifetimeCostExpr: "state_channel_financials.avg_lifetime_cost",
            avgLifetimePremiumExpr: "state_channel_financials.avg_lifetime_premium"
          })} AS combined_ratio
        FROM final_rows
        LEFT JOIN state_channel_financials
          ON state_channel_financials.channel_group_name = final_rows.channel_group_name
         AND state_channel_financials.state = final_rows.state
      ),
      ranked_rows AS (
        SELECT
          *,
          FIRST_VALUE(testing_point) OVER (
            PARTITION BY channel_group_name, state
            ORDER BY
              CASE
                WHEN testing_point != 0
                  AND cpb_uplift IS NOT NULL
                  AND cpb_uplift <= 0.10
                  AND additional_clicks > 0
                  THEN 0
                WHEN testing_point = 0 THEN 1
                ELSE 2
              END,
              CASE
                WHEN testing_point != 0
                  AND cpb_uplift IS NOT NULL
                  AND cpb_uplift <= 0.10
                  AND additional_clicks > 0
                  THEN additional_clicks
                ELSE -1e18
              END DESC,
              testing_point
          ) AS recommended_testing_point
        FROM final_rows_scoped
      )
      SELECT *
      FROM ranked_rows
      ORDER BY channel_group_name, state, testing_point
      LIMIT @limit
    `,
    normalized
  );

  const strategyRules = await getStrategyRulesForPlan(filters.planId, filters.activityLeadType);
  const manualOverrides = await getPriceDecisionOverridesForPlan(filters.planId, filters.activityLeadType);
  if (!strategyRules.length) {
    if (!manualOverrides.size) {
      return rows;
    }
    const groupedWithoutRules = new Map<string, PriceExplorationRow[]>();
    for (const row of rows) {
      const state = String(row.state || "").toUpperCase();
      const segment = extractSegmentFromChannelGroup(String(row.channel_group_name || ""));
      const key = `${String(row.channel_group_name || "")}|${state}|${segment || "__UNSEGMENTED__"}`;
      const bucket = groupedWithoutRules.get(key) || [];
      bucket.push(row);
      groupedWithoutRules.set(key, bucket);
    }
    for (const groupRows of groupedWithoutRules.values()) {
      if (!groupRows.length) {
        continue;
      }
      const sample = groupRows[0];
      const state = String(sample.state || "").toUpperCase();
      const segment = extractSegmentFromChannelGroup(String(sample.channel_group_name || ""));
      const overrideKey = buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, segment);
      const overrideTp = manualOverrides.get(overrideKey) ?? manualOverrides.get(
        buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, "")
      );
      if (!Number.isFinite(Number(overrideTp))) {
        continue;
      }
      const normalizedOverride = Number(overrideTp);
      if (!groupRows.some((item) => Number(item.testing_point) === normalizedOverride)) {
        continue;
      }
      for (const row of groupRows) {
        row.recommended_testing_point = normalizedOverride;
      }
    }
    return rows;
  }

  const grouped = new Map<string, PriceExplorationRow[]>();
  for (const row of rows) {
    const state = String(row.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(row.channel_group_name || ""));
    const key = `${String(row.channel_group_name || "")}|${state}|${segment || "__UNSEGMENTED__"}`;
    const bucket = grouped.get(key) || [];
    bucket.push(row);
    grouped.set(key, bucket);
  }

  for (const groupRows of grouped.values()) {
    if (!groupRows.length) {
      continue;
    }
    const sample = groupRows[0];
    const state = String(sample.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(sample.channel_group_name || ""));
    const matchedRule = strategyRules.find(
      (rule) => rule.states.includes(state) && rule.segments.includes(segment)
    ) || null;
    const overrideKey = buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, segment);
    const overrideTp = manualOverrides.get(overrideKey) ?? manualOverrides.get(
      buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, "")
    );
    const hasValidOverride =
      Number.isFinite(Number(overrideTp)) &&
      groupRows.some((row) => Number(row.testing_point) === Number(overrideTp));
    const recommendedTp = hasValidOverride
      ? Number(overrideTp)
      : chooseRecommendedTestingPoint(groupRows, matchedRule);
    for (const row of groupRows) {
      row.recommended_testing_point = recommendedTp;
    }
  }

  return rows;
}

export async function listPlanMergedFilters(
  filters: Pick<PlanMergedFilters, "startDate" | "endDate" | "activityLeadType">
): Promise<{
  states: string[];
  segments: string[];
  channelGroups: string[];
  testingPoints: number[];
  statSig: string[];
}> {
  const normalized = normalizePlanMergedFilters(filters);

  const rows = await query<PlanMergedFilterOptionsRow>(
    `
      WITH scoped AS (
        SELECT state, segment, channel_group_name, price_adjustment_percent, stat_sig
        FROM ${analyticsRoutine("fn_plan_merged_agg")}(
          IF(@startDate = "", DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY), DATE(@startDate)),
          IF(@endDate = "", CURRENT_DATE(), DATE(@endDate))
        )
        WHERE (
            @activityType = ""
            OR REGEXP_CONTAINS(LOWER(channel_group_name), @activityPattern)
          )
          AND (@leadType = "" OR REGEXP_CONTAINS(LOWER(channel_group_name), @leadPattern))
      )
      SELECT
        ARRAY(
          SELECT DISTINCT state
          FROM scoped
          WHERE state IS NOT NULL
          ORDER BY state
        ) AS states,
        ARRAY(
          SELECT DISTINCT segment
          FROM scoped
          WHERE segment IS NOT NULL
          ORDER BY segment
        ) AS segments,
        ARRAY(
          SELECT DISTINCT channel_group_name
          FROM scoped
          WHERE channel_group_name IS NOT NULL
          ORDER BY channel_group_name
        ) AS channel_groups,
        ARRAY(
          SELECT DISTINCT price_adjustment_percent
          FROM scoped
          ORDER BY price_adjustment_percent
        ) AS testing_points,
        ARRAY(
          SELECT DISTINCT stat_sig
          FROM scoped
          WHERE stat_sig IS NOT NULL
          ORDER BY stat_sig
        ) AS stat_sig
    `,
    normalized
  );

  const first = rows[0];
  return {
    states: withAllStateCodes(first?.states),
    segments: first?.segments ?? [],
    channelGroups: first?.channel_groups ?? [],
    testingPoints: first?.testing_points ?? [],
    statSig: first?.stat_sig ?? []
  };
}

export async function getPlanMergedAnalytics(
  filters: PlanMergedFilters
): Promise<PlanMergedRow[]> {
  const normalized = normalizePlanMergedFilters(filters);

  return query<PlanMergedRow>(
    `
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
      FROM ${analyticsRoutine("fn_plan_merged_agg")}(
        IF(@startDate = "", DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY), DATE(@startDate)),
        IF(@endDate = "", CURRENT_DATE(), DATE(@endDate))
      )
      WHERE ("__ALL__" IN UNNEST(@states) OR state IN UNNEST(@states))
        AND ("__ALL__" IN UNNEST(@segments) OR segment IN UNNEST(@segments))
        AND ("__ALL__" IN UNNEST(@channelGroups) OR channel_group_name IN UNNEST(@channelGroups))
        AND (999999999 IN UNNEST(@testingPoints) OR price_adjustment_percent IN UNNEST(@testingPoints))
        AND ("__ALL__" IN UNNEST(@statSig) OR stat_sig IN UNNEST(@statSig))
        AND (
          @activityType = ""
          OR REGEXP_CONTAINS(LOWER(channel_group_name), @activityPattern)
        )
        AND (@leadType = "" OR REGEXP_CONTAINS(LOWER(channel_group_name), @leadPattern))
      ORDER BY channel_group_name, state, price_adjustment_percent
    `,
    normalized
  );
}

export async function getStrategyAnalysis(
  filters: StrategyAnalysisFilters
): Promise<StrategyAnalysisRow[]> {
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'plan_strategy_config'
      LIMIT 1
    `,
    { planId: filters.planId }
  );

  const rules = parseStrategyRules(paramRows[0]?.param_value || "", filters.activityLeadType);
  if (!rules.length) {
    return [];
  }

  const uniqueStates = [...new Set(rules.flatMap((rule) => rule.states))];
  const uniqueSegments = [...new Set(rules.flatMap((rule) => rule.segments))];
  const rawBaselineRows = await getStateSegmentPerformance({
    startDate: filters.startDate,
    endDate: filters.endDate,
    states: uniqueStates,
    segments: uniqueSegments,
    activityLeadType: filters.activityLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : undefined
  });

  const baselineGrouped = new Map<string, StrategyBaselineRow>();
  for (const row of rawBaselineRows) {
    const state = String(row.state || "").toUpperCase();
    const segment = String(row.segment || "").toUpperCase();
    const key = `${state}|${segment}`;
    const current = baselineGrouped.get(key) || {
      state,
      segment,
      bids: 0,
      sold: 0,
      total_cost: 0,
      quotes: 0,
      binds: 0,
      scored_policies: 0,
      q2b: null,
      performance: null,
      roe: null,
      combined_ratio: null
    };

    const bids = Number(row.bids) || 0;
    const sold = Number(row.sold) || 0;
    const totalCost = Number(row.total_cost) || 0;
    const quotes = Number(row.quotes) || 0;
    const binds = Number(row.binds) || 0;
    const scoredPolicies = Number(row.scored_policies) || 0;
    const perf = Number(row.performance);
    const roe = Number(row.roe);
    const cor = Number(row.combined_ratio);

    current.bids = (Number(current.bids) || 0) + bids;
    current.sold = (Number(current.sold) || 0) + sold;
    current.total_cost = (Number(current.total_cost) || 0) + totalCost;
    current.quotes = (Number(current.quotes) || 0) + quotes;
    current.binds = (Number(current.binds) || 0) + binds;
    current.scored_policies = (Number(current.scored_policies) || 0) + scoredPolicies;

    const currentPerfWeighted = (Number(current.performance) || 0) * Math.max((Number(current.binds) || 0) - binds, 0);
    const currentRoeWeighted =
      (Number(current.roe) || 0) * Math.max((Number(current.scored_policies) || 0) - scoredPolicies, 0);
    const currentCorWeighted =
      (Number(current.combined_ratio) || 0) * Math.max((Number(current.scored_policies) || 0) - scoredPolicies, 0);

    const nextPerfWeight = Number(current.binds) || 0;
    const nextRoeWeight = Number(current.scored_policies) || 0;
    const nextCorWeight = Number(current.scored_policies) || 0;

    current.performance =
      nextPerfWeight > 0
        ? (currentPerfWeighted + (Number.isFinite(perf) ? perf * binds : 0)) / nextPerfWeight
        : null;
    current.roe =
      nextRoeWeight > 0
        ? (currentRoeWeighted + (Number.isFinite(roe) ? roe * scoredPolicies : 0)) / nextRoeWeight
        : null;
    current.combined_ratio =
      nextCorWeight > 0
        ? (currentCorWeighted + (Number.isFinite(cor) ? cor * scoredPolicies : 0)) / nextCorWeight
        : null;
    current.q2b = (Number(current.quotes) || 0) > 0 ? (Number(current.binds) || 0) / Number(current.quotes) : null;

    baselineGrouped.set(key, current);
  }
  const baselineRows = [...baselineGrouped.values()];
  const priceRows = await getPriceExploration({
    planId: filters.planId,
    startDate: filters.startDate,
    endDate: filters.endDate,
    q2bStartDate: filters.startDate,
    q2bEndDate: filters.endDate,
    states: uniqueStates,
    activityLeadType: filters.activityLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : undefined,
    limit: 200000
  });

  function extractSegment(channelGroup: string): string {
    const match = String(channelGroup || "").toUpperCase().match(/\b(MCH|MCR|SCH|SCR)\b/);
    return match ? match[1] : "";
  }

  type RecommendedSummary = {
    state: string;
    segment: string;
    additional_clicks: number;
    additional_binds: number;
    additional_budget: number;
    current_wr: number | null;
    expected_wr: number | null;
    wr_weight: number;
    current_cpc: number | null;
    expected_cpc: number | null;
    cpc_weight: number;
    current_cpb: number | null;
    expected_cpb: number | null;
    cpb_weight: number;
  };

  const rowsByChannelStateSegment = new Map<string, PriceExplorationRow[]>();
  for (const row of priceRows) {
    const state = String(row.state || "").toUpperCase();
    const segment = extractSegment(String(row.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }
    const key = `${String(row.channel_group_name || "")}|${state}|${segment}`;
    const group = rowsByChannelStateSegment.get(key) || [];
    group.push(row);
    rowsByChannelStateSegment.set(key, group);
  }

  const recommendedSummaries: RecommendedSummary[] = [];
  for (const groupRows of rowsByChannelStateSegment.values()) {
    const baseline = groupRows.find((row) => Number(row.testing_point) === 0) || null;
    const recommendedByFlag = groupRows.find((row) =>
      Number.isFinite(Number(row.recommended_testing_point)) &&
      Number(row.testing_point) === Number(row.recommended_testing_point)
    );
    const recommended =
      recommendedByFlag ||
      [...groupRows].sort((a, b) => {
        const aTp = Number(a.testing_point) || 0;
        const bTp = Number(b.testing_point) || 0;
        const aCpbUplift = Number(a.cpb_uplift);
        const bCpbUplift = Number(b.cpb_uplift);
        const aAdditionalClicks = Number(a.additional_clicks) || 0;
        const bAdditionalClicks = Number(b.additional_clicks) || 0;
        const aPriority =
          aTp !== 0 && Number.isFinite(aCpbUplift) && aCpbUplift <= 0.10 && aAdditionalClicks > 0
            ? 0
            : aTp === 0
              ? 1
              : 2;
        const bPriority =
          bTp !== 0 && Number.isFinite(bCpbUplift) && bCpbUplift <= 0.10 && bAdditionalClicks > 0
            ? 0
            : bTp === 0
              ? 1
              : 2;
        if (aPriority !== bPriority) {
          return aPriority - bPriority;
        }
        if (aPriority === 0 && aAdditionalClicks !== bAdditionalClicks) {
          return bAdditionalClicks - aAdditionalClicks;
        }
        return aTp - bTp;
      })[0];

    if (!recommended) {
      continue;
    }

    const state = String(recommended.state || "").toUpperCase();
    const segment = extractSegment(String(recommended.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }

    const baselineBids = Number(baseline?.bids);
    const baselineSold = Number(baseline?.sold);
    const baselineBinds = Number(baseline?.binds);
    const baselineWr = Number(baseline?.win_rate);
    const baselineCpc = Number(baseline?.cpc);
    const expectedWr = Number(recommended.win_rate);
    const expectedCpc = Number(recommended.cpc);
    const wrUplift = Number(recommended.win_rate_uplift);
    const cpcUplift = Number(recommended.cpc_uplift);

    const derivedBaselineWr =
      Number.isFinite(expectedWr) && Number.isFinite(wrUplift) && Math.abs(1 + wrUplift) > 1e-9
        ? expectedWr / (1 + wrUplift)
        : null;
    const derivedBaselineCpc =
      Number.isFinite(expectedCpc) && Number.isFinite(cpcUplift) && Math.abs(1 + cpcUplift) > 1e-9
        ? expectedCpc / (1 + cpcUplift)
        : null;

    const currentWr = Number.isFinite(baselineWr) ? baselineWr : derivedBaselineWr;
    const currentCpc = Number.isFinite(baselineCpc) ? baselineCpc : derivedBaselineCpc;
    const currentCpb = Number(recommended.current_cpb);
    const expectedCpb = Number(recommended.expected_cpb);

    recommendedSummaries.push({
      state,
      segment,
      additional_clicks: Number(recommended.additional_clicks) || 0,
      additional_binds: Number(recommended.expected_bind_change) || 0,
      additional_budget: Number(recommended.additional_budget_needed) || 0,
      current_wr: Number.isFinite(currentWr) ? currentWr : null,
      expected_wr: Number.isFinite(expectedWr) ? expectedWr : null,
      wr_weight:
        Number.isFinite(baselineBids) && baselineBids > 0
          ? baselineBids
          : Number(recommended.bids) || 0,
      current_cpc: Number.isFinite(currentCpc) ? currentCpc : null,
      expected_cpc: Number.isFinite(expectedCpc) ? expectedCpc : null,
      cpc_weight:
        Number.isFinite(baselineSold) && baselineSold > 0
          ? baselineSold
          : Number(recommended.sold) || 0,
      current_cpb: Number.isFinite(currentCpb) ? currentCpb : null,
      expected_cpb: Number.isFinite(expectedCpb) ? expectedCpb : null,
      cpb_weight:
        Number.isFinite(baselineBinds) && baselineBinds > 0
          ? baselineBinds
          : Number(recommended.binds) || 0
    });
  }

  return rules.map((rule) => {
    const stateSet = new Set(rule.states);
    const segmentSet = new Set(rule.segments);
    const includesAllCoreSegments =
      segmentSet.has("MCH") && segmentSet.has("MCR") && segmentSet.has("SCH") && segmentSet.has("SCR");

    const matchingBaseline = baselineRows.filter(
      (row) => stateSet.has(String(row.state || "").toUpperCase()) && segmentSet.has(String(row.segment || "").toUpperCase())
    );
    const matchingRecommended = recommendedSummaries.filter(
      (row) =>
        stateSet.has(String(row.state || "").toUpperCase()) &&
        (includesAllCoreSegments || segmentSet.has(String(row.segment || "").toUpperCase()))
    );

    const bids = matchingBaseline.reduce((sum, row) => sum + (Number(row.bids) || 0), 0);
    const sold = matchingBaseline.reduce((sum, row) => sum + (Number(row.sold) || 0), 0);
    const quotes = matchingBaseline.reduce((sum, row) => sum + (Number(row.quotes) || 0), 0);
    const binds = matchingBaseline.reduce((sum, row) => sum + (Number(row.binds) || 0), 0);
    const totalSpend = matchingBaseline.reduce((sum, row) => sum + (Number(row.total_cost) || 0), 0);
    const wr = bids > 0 ? sold / bids : null;
    const cpc = sold > 0 ? totalSpend / sold : null;
    const q2b = quotes > 0 ? binds / quotes : null;

    const performanceWeighted = matchingBaseline.reduce(
      (acc, row) => {
        const weight = Number(row.binds) || 0;
        const value = Number(row.performance);
        if (weight > 0 && Number.isFinite(value)) {
          acc.total += value * weight;
          acc.weight += weight;
        }
        return acc;
      },
      { total: 0, weight: 0 }
    );
    const roeWeighted = matchingBaseline.reduce(
      (acc, row) => {
        const weight = Number(row.scored_policies) || 0;
        const value = Number(row.roe);
        if (weight > 0 && Number.isFinite(value)) {
          acc.total += value * weight;
          acc.weight += weight;
        }
        return acc;
      },
      { total: 0, weight: 0 }
    );
    const corWeighted = matchingBaseline.reduce(
      (acc, row) => {
        const weight = Number(row.scored_policies) || 0;
        const value = Number(row.combined_ratio);
        if (weight > 0 && Number.isFinite(value)) {
          acc.total += value * weight;
          acc.weight += weight;
        }
        return acc;
      },
      { total: 0, weight: 0 }
    );

    const additionalClicks = matchingRecommended.reduce((sum, row) => sum + (Number(row.additional_clicks) || 0), 0);
    const additionalBinds = matchingRecommended.reduce(
      (sum, row) => sum + (Number(row.additional_binds) || 0),
      0
    );
    const additionalBudget = matchingRecommended.reduce((sum, row) => sum + (Number(row.additional_budget) || 0), 0);
    const expectedTotalCost = totalSpend + additionalBudget;
    const currentCpb = binds > 0 ? totalSpend / binds : null;
    const expectedCpb = binds + additionalBinds > 0 ? expectedTotalCost / (binds + additionalBinds) : null;

    const wrRollup = matchingRecommended.reduce(
      (acc, row) => {
        const current = Number(row.current_wr);
        const expected = Number(row.expected_wr);
        const weight = Number(row.wr_weight) || 0;
        if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
          acc.currentWins += current * weight;
          acc.expectedWins += expected * weight;
          acc.weight += weight;
        }
        return acc;
      },
      { currentWins: 0, expectedWins: 0, weight: 0 }
    );
    const cpbRollup = matchingRecommended.reduce(
      (acc, row) => {
        const current = Number(row.current_cpb);
        const expected = Number(row.expected_cpb);
        const weight = Number(row.cpb_weight) || 0;
        if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
          acc.currentCost += current * weight;
          acc.expectedCost += expected * weight;
        }
        return acc;
      },
      { currentCost: 0, expectedCost: 0 }
    );

    const wr_uplift =
      wrRollup.weight > 0 && wrRollup.currentWins > 0
        ? (wrRollup.expectedWins - wrRollup.currentWins) / wrRollup.currentWins
        : null;
    const currentCpc = bids > 0 ? totalSpend / bids : null;
    const expectedCost = expectedTotalCost;
    const expectedClicks = bids + additionalClicks;
    const expectedCpc = expectedClicks > 0 ? expectedCost / expectedClicks : null;
    const cpc_uplift =
      Number.isFinite(currentCpc) && Number.isFinite(expectedCpc) && Number(currentCpc) > 0
        ? (Number(expectedCpc) - Number(currentCpc)) / Number(currentCpc)
        : null;
    const cpb_uplift =
      cpbRollup.currentCost > 0
        ? (cpbRollup.expectedCost - cpbRollup.currentCost) / cpbRollup.currentCost
        : null;

    return {
      rule_name: rule.name,
      states: rule.states,
      segments: rule.segments,
      target_cor: rule.corTarget > 0 ? rule.corTarget : null,
      bids,
      sold,
      total_spend: totalSpend,
      cpc,
      wr,
      quotes,
      binds,
      current_cpb: currentCpb,
      expected_cpb: expectedCpb,
      q2b,
      performance: performanceWeighted.weight > 0 ? performanceWeighted.total / performanceWeighted.weight : null,
      roe: roeWeighted.weight > 0 ? roeWeighted.total / roeWeighted.weight : null,
      cor: corWeighted.weight > 0 ? corWeighted.total / corWeighted.weight : null,
      additional_clicks: additionalClicks,
      additional_binds: additionalBinds,
      wr_uplift,
      cpc_uplift,
      cpb_uplift,
      expected_total_cost: expectedTotalCost,
      additional_budget: additionalBudget
    };
  });
}

function mapGrowthStrategy(
  growthStrategy: string
): { key: "aggressive" | "robustic" | "cautious"; label: string } {
  const value = String(growthStrategy || "").trim().toLowerCase();
  if (value.includes("aggressive") || value.includes("high")) {
    return { key: "aggressive", label: "Aggressive growth" };
  }
  if (value.includes("cautious") || value.includes("cost") || value.includes("low")) {
    return { key: "cautious", label: "Cautious growth" };
  }
  return { key: "robustic", label: "Robustic growth" };
}

type StrategyBaselineExtRow = {
  state: string;
  segment: string;
  bids: number;
  sold: number;
  total_cost: number;
  quotes: number;
  binds: number;
  scored_policies: number;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
  mrltv: number | null;
};

type StrategyRecommendedSummary = {
  state: string;
  segment: string;
  additional_clicks: number;
  additional_binds: number;
  additional_budget: number;
  current_wr: number | null;
  expected_wr: number | null;
  wr_weight: number;
  current_cpb: number | null;
  expected_cpb: number | null;
  cpb_weight: number;
};

function aggregateStrategySlice(
  baselineRows: StrategyBaselineExtRow[],
  recommendedRows: StrategyRecommendedSummary[]
): StrategyAnalysisRow & { ltv: number | null } {
  const bids = baselineRows.reduce((sum, row) => sum + (Number(row.bids) || 0), 0);
  const sold = baselineRows.reduce((sum, row) => sum + (Number(row.sold) || 0), 0);
  const quotes = baselineRows.reduce((sum, row) => sum + (Number(row.quotes) || 0), 0);
  const binds = baselineRows.reduce((sum, row) => sum + (Number(row.binds) || 0), 0);
  const totalSpend = baselineRows.reduce((sum, row) => sum + (Number(row.total_cost) || 0), 0);
  const wr = bids > 0 ? sold / bids : null;
  const cpc = sold > 0 ? totalSpend / sold : null;
  const q2b = quotes > 0 ? binds / quotes : null;

  const performanceWeighted = baselineRows.reduce(
    (acc, row) => {
      const value = Number(row.performance);
      const weight = Number(row.binds) || 0;
      if (weight > 0 && Number.isFinite(value)) {
        acc.total += value * weight;
        acc.weight += weight;
      }
      return acc;
    },
    { total: 0, weight: 0 }
  );
  const roeWeighted = baselineRows.reduce(
    (acc, row) => {
      const value = Number(row.roe);
      const weight = Number(row.scored_policies) || 0;
      if (weight > 0 && Number.isFinite(value)) {
        acc.total += value * weight;
        acc.weight += weight;
      }
      return acc;
    },
    { total: 0, weight: 0 }
  );
  const corWeighted = baselineRows.reduce(
    (acc, row) => {
      const value = Number(row.combined_ratio);
      const weight = Number(row.scored_policies) || 0;
      if (weight > 0 && Number.isFinite(value)) {
        acc.total += value * weight;
        acc.weight += weight;
      }
      return acc;
    },
    { total: 0, weight: 0 }
  );
  const ltvWeighted = baselineRows.reduce(
    (acc, row) => {
      const value = Number(row.mrltv);
      const weight = Number(row.scored_policies) || 0;
      if (weight > 0 && Number.isFinite(value)) {
        acc.total += value * weight;
        acc.weight += weight;
      }
      return acc;
    },
    { total: 0, weight: 0 }
  );

  const additionalClicks = recommendedRows.reduce((sum, row) => sum + (Number(row.additional_clicks) || 0), 0);
  const additionalBinds = recommendedRows.reduce((sum, row) => sum + (Number(row.additional_binds) || 0), 0);
  const additionalBudget = recommendedRows.reduce((sum, row) => sum + (Number(row.additional_budget) || 0), 0);
  const expectedTotalCost = totalSpend + additionalBudget;
  const currentCpb = binds > 0 ? totalSpend / binds : null;
  const expectedCpb = binds + additionalBinds > 0 ? expectedTotalCost / (binds + additionalBinds) : null;

  const wrRollup = recommendedRows.reduce(
    (acc, row) => {
      const current = Number(row.current_wr);
      const expected = Number(row.expected_wr);
      const weight = Number(row.wr_weight) || 0;
      if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
        acc.currentWins += current * weight;
        acc.expectedWins += expected * weight;
      }
      return acc;
    },
    { currentWins: 0, expectedWins: 0 }
  );
  const cpbRollup = recommendedRows.reduce(
    (acc, row) => {
      const current = Number(row.current_cpb);
      const expected = Number(row.expected_cpb);
      const weight = Number(row.cpb_weight) || 0;
      if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
        acc.currentCost += current * weight;
        acc.expectedCost += expected * weight;
      }
      return acc;
    },
    { currentCost: 0, expectedCost: 0 }
  );

  const wrUplift =
    wrRollup.currentWins > 0 ? (wrRollup.expectedWins - wrRollup.currentWins) / wrRollup.currentWins : null;
  const expectedClicks = bids + additionalClicks;
  const expectedCpc = expectedClicks > 0 ? expectedTotalCost / expectedClicks : null;
  const cpcUplift =
    Number.isFinite(Number(cpc)) && Number.isFinite(Number(expectedCpc)) && Number(cpc) > 0
      ? (Number(expectedCpc) - Number(cpc)) / Number(cpc)
      : null;
  const cpbUplift =
    cpbRollup.currentCost > 0 ? (cpbRollup.expectedCost - cpbRollup.currentCost) / cpbRollup.currentCost : null;

  return {
    rule_name: "Aggregate",
    states: [],
    segments: [],
    target_cor: null,
    bids,
    sold,
    total_spend: totalSpend,
    cpc,
    wr,
    quotes,
    binds,
    current_cpb: currentCpb,
    expected_cpb: expectedCpb,
    q2b,
    performance: performanceWeighted.weight > 0 ? performanceWeighted.total / performanceWeighted.weight : null,
    roe: roeWeighted.weight > 0 ? roeWeighted.total / roeWeighted.weight : null,
    cor: corWeighted.weight > 0 ? corWeighted.total / corWeighted.weight : null,
    additional_clicks: additionalClicks,
    additional_binds: additionalBinds,
    wr_uplift: wrUplift,
    cpc_uplift: cpcUplift,
    cpb_uplift: cpbUplift,
    expected_total_cost: expectedTotalCost,
    additional_budget: additionalBudget,
    ltv: ltvWeighted.weight > 0 ? ltvWeighted.total / ltvWeighted.weight : null
  };
}

export async function getStateAnalysis(
  filters: StrategyAnalysisFilters
): Promise<StateAnalysisResponse> {
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'plan_strategy_config'
      LIMIT 1
    `,
    { planId: filters.planId }
  );

  const rules = parseStrategyRules(paramRows[0]?.param_value || "", filters.activityLeadType);
  if (!rules.length) {
    return {
      overall: {
        ...aggregateStrategySlice([], []),
        rule_name: "All rules",
        states: [],
        segments: []
      },
      states: ALL_US_STATE_CODES.map((stateCode) => ({
        state: stateCode,
        rule_name: null,
        tier: null,
        strategy_key: null,
        strategy_label: null,
        total_spend: 0,
        cor: null,
        roe: null,
        binds: 0,
        performance: null,
        additional_clicks: 0,
        additional_binds: 0,
        additional_budget: 0,
        cpc_uplift: null,
        cpb_uplift: null
      })),
      state_details: [],
      rules: []
    };
  }

  const uniqueStates = [...new Set(rules.flatMap((rule) => rule.states))];
  const uniqueSegments = [...new Set(rules.flatMap((rule) => rule.segments))];
  const rawBaselineRows = await getStateSegmentPerformance({
    startDate: filters.startDate,
    endDate: filters.endDate,
    states: uniqueStates,
    segments: uniqueSegments,
    activityLeadType: filters.activityLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : undefined
  });

  const baselineGrouped = new Map<string, StrategyBaselineExtRow>();
  for (const row of rawBaselineRows) {
    const state = String(row.state || "").toUpperCase();
    const segment = String(row.segment || "").toUpperCase();
    const key = `${state}|${segment}`;
    const current = baselineGrouped.get(key) || {
      state,
      segment,
      bids: 0,
      sold: 0,
      total_cost: 0,
      quotes: 0,
      binds: 0,
      scored_policies: 0,
      performance: null,
      roe: null,
      combined_ratio: null,
      mrltv: null
    };

    const bids = Number(row.bids) || 0;
    const sold = Number(row.sold) || 0;
    const totalCost = Number(row.total_cost) || 0;
    const quotes = Number(row.quotes) || 0;
    const binds = Number(row.binds) || 0;
    const scoredPolicies = Number(row.scored_policies) || 0;
    const perf = Number(row.performance);
    const roe = Number(row.roe);
    const cor = Number(row.combined_ratio);
    const ltv = Number(row.mrltv);

    const prevBinds = Number(current.binds) || 0;
    const prevScored = Number(current.scored_policies) || 0;
    const prevPerfWeighted = (Number(current.performance) || 0) * prevBinds;
    const prevRoeWeighted = (Number(current.roe) || 0) * prevScored;
    const prevCorWeighted = (Number(current.combined_ratio) || 0) * prevScored;
    const prevLtvWeighted = (Number(current.mrltv) || 0) * prevScored;

    current.bids = (Number(current.bids) || 0) + bids;
    current.sold = (Number(current.sold) || 0) + sold;
    current.total_cost = (Number(current.total_cost) || 0) + totalCost;
    current.quotes = (Number(current.quotes) || 0) + quotes;
    current.binds = prevBinds + binds;
    current.scored_policies = prevScored + scoredPolicies;

    current.performance =
      current.binds > 0
        ? (prevPerfWeighted + (Number.isFinite(perf) ? perf * binds : 0)) / current.binds
        : null;
    current.roe =
      current.scored_policies > 0
        ? (prevRoeWeighted + (Number.isFinite(roe) ? roe * scoredPolicies : 0)) / current.scored_policies
        : null;
    current.combined_ratio =
      current.scored_policies > 0
        ? (prevCorWeighted + (Number.isFinite(cor) ? cor * scoredPolicies : 0)) / current.scored_policies
        : null;
    current.mrltv =
      current.scored_policies > 0
        ? (prevLtvWeighted + (Number.isFinite(ltv) ? ltv * scoredPolicies : 0)) / current.scored_policies
        : null;

    baselineGrouped.set(key, current);
  }
  const baselineRows = [...baselineGrouped.values()];

  const priceRows = await getPriceExploration({
    planId: filters.planId,
    startDate: filters.startDate,
    endDate: filters.endDate,
    q2bStartDate: filters.startDate,
    q2bEndDate: filters.endDate,
    states: uniqueStates,
    activityLeadType: filters.activityLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : undefined,
    limit: 200000
  });

  const rowsByChannelStateSegment = new Map<string, PriceExplorationRow[]>();
  for (const row of priceRows) {
    const state = String(row.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(row.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }
    const key = `${String(row.channel_group_name || "")}|${state}|${segment}`;
    const bucket = rowsByChannelStateSegment.get(key) || [];
    bucket.push(row);
    rowsByChannelStateSegment.set(key, bucket);
  }

  const recommendedSummaries: StrategyRecommendedSummary[] = [];
  for (const groupRows of rowsByChannelStateSegment.values()) {
    const baseline = groupRows.find((row) => Number(row.testing_point) === 0) || null;
    const recommendedByFlag = groupRows.find(
      (row) =>
        Number.isFinite(Number(row.recommended_testing_point)) &&
        Number(row.testing_point) === Number(row.recommended_testing_point)
    );
    const recommended = recommendedByFlag || groupRows[0];
    if (!recommended) {
      continue;
    }

    const state = String(recommended.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(recommended.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }

    const baselineBids = Number(baseline?.bids);
    const baselineBinds = Number(baseline?.binds);
    const baselineWr = Number(baseline?.win_rate);
    const expectedWr = Number(recommended.win_rate);
    const wrUplift = Number(recommended.win_rate_uplift);
    const derivedBaselineWr =
      Number.isFinite(expectedWr) && Number.isFinite(wrUplift) && Math.abs(1 + wrUplift) > 1e-9
        ? expectedWr / (1 + wrUplift)
        : null;
    const currentWr = Number.isFinite(baselineWr) ? baselineWr : derivedBaselineWr;

    const currentCpb = Number(recommended.current_cpb);
    const expectedCpb = Number(recommended.expected_cpb);

    recommendedSummaries.push({
      state,
      segment,
      additional_clicks: Number(recommended.additional_clicks) || 0,
      additional_binds: Number(recommended.expected_bind_change) || 0,
      additional_budget: Number(recommended.additional_budget_needed) || 0,
      current_wr: Number.isFinite(currentWr) ? currentWr : null,
      expected_wr: Number.isFinite(expectedWr) ? expectedWr : null,
      wr_weight: Number.isFinite(baselineBids) && baselineBids > 0 ? baselineBids : Number(recommended.bids) || 0,
      current_cpb: Number.isFinite(currentCpb) ? currentCpb : null,
      expected_cpb: Number.isFinite(expectedCpb) ? expectedCpb : null,
      cpb_weight:
        Number.isFinite(baselineBinds) && baselineBinds > 0 ? baselineBinds : Number(recommended.binds) || 0
    });
  }

  const overallAgg = aggregateStrategySlice(baselineRows, recommendedSummaries);

  const ruleRows: StateAnalysisRuleRow[] = rules.map((rule) => {
    const stateSet = new Set(rule.states);
    const segmentSet = new Set(rule.segments);
    const includesAllCoreSegments =
      segmentSet.has("MCH") && segmentSet.has("MCR") && segmentSet.has("SCH") && segmentSet.has("SCR");
    const strategy = mapGrowthStrategy(rule.growthStrategy);

    const baselineSlice = baselineRows.filter(
      (row) => stateSet.has(row.state) && segmentSet.has(row.segment)
    );
    const recommendedSlice = recommendedSummaries.filter(
      (row) => stateSet.has(row.state) && (includesAllCoreSegments || segmentSet.has(row.segment))
    );
    const ruleAgg = aggregateStrategySlice(baselineSlice, recommendedSlice);

    const segmentRows: StateAnalysisSegmentRow[] = rule.segments.map((segment) => {
      const baselineSegment = baselineRows.filter((row) => stateSet.has(row.state) && row.segment === segment);
      const recommendedSegment = recommendedSummaries.filter(
        (row) => stateSet.has(row.state) && row.segment === segment
      );
      const agg = aggregateStrategySlice(baselineSegment, recommendedSegment);
      return {
        segment,
        bids: agg.bids,
        sold: agg.sold,
        wr: agg.wr,
        total_spend: agg.total_spend,
        quotes: agg.quotes,
        sold_to_quotes: agg.quotes > 0 ? agg.sold / agg.quotes : null,
        binds: agg.binds,
        q2b: agg.q2b,
        cor: agg.cor,
        roe: agg.roe,
        cpb: agg.current_cpb,
        performance: agg.performance
      };
    });

    return {
      rule_name: rule.name,
      tier: Number(rule.id) || 999,
      strategy_key: strategy.key,
      strategy_label: strategy.label,
      states: rule.states,
      segments: rule.segments,
      kpis: {
        ...ruleAgg,
        rule_name: rule.name,
        states: rule.states,
        segments: rule.segments
      },
      segment_rows: segmentRows
    };
  });

  const stateRows: StateAnalysisStateRow[] = ALL_US_STATE_CODES.map((stateCode) => {
    const matchingRules = ruleRows
      .filter((rule) => rule.states.includes(stateCode))
      .sort((a, b) => (a.tier || 999) - (b.tier || 999));
    const primaryRule = matchingRules[0] || null;
    const segmentSet = primaryRule ? new Set(primaryRule.segments) : null;

    const baselineSlice = baselineRows.filter(
      (row) => row.state === stateCode && (!segmentSet || segmentSet.has(row.segment))
    );
    const recommendedSlice = recommendedSummaries.filter(
      (row) => row.state === stateCode && (!segmentSet || segmentSet.has(row.segment))
    );
    const agg = aggregateStrategySlice(baselineSlice, recommendedSlice);

    return {
      state: stateCode,
      rule_name: primaryRule?.rule_name || null,
      tier: primaryRule?.tier ?? null,
      strategy_key: primaryRule?.strategy_key ?? null,
      strategy_label: primaryRule?.strategy_label ?? null,
      total_spend: agg.total_spend,
      cor: agg.cor,
      roe: agg.roe,
      binds: agg.binds,
      performance: agg.performance,
      additional_clicks: agg.additional_clicks,
      additional_binds: agg.additional_binds,
      additional_budget: agg.additional_budget,
      cpc_uplift: agg.cpc_uplift,
      cpb_uplift: agg.cpb_uplift
    };
  });

  const stateDetails = stateRows.map((stateRow) => {
    const segmentSet = stateRow.rule_name
      ? new Set(
          (ruleRows.find((rule) => rule.rule_name === stateRow.rule_name)?.segments || []).map((value) =>
            String(value || "").toUpperCase()
          )
        )
      : null;
    const baselineSlice = baselineRows.filter(
      (row) => row.state === stateRow.state && (!segmentSet || segmentSet.has(String(row.segment || "").toUpperCase()))
    );
    const recommendedSlice = recommendedSummaries.filter(
      (row) => row.state === stateRow.state && (!segmentSet || segmentSet.has(String(row.segment || "").toUpperCase()))
    );
    const agg = aggregateStrategySlice(baselineSlice, recommendedSlice);

    const segments = segmentSet ? [...segmentSet] : [...new Set(baselineSlice.map((row) => String(row.segment || "").toUpperCase()))];
    segments.sort();
    const segmentRows: StateAnalysisSegmentRow[] = segments.map((segment) => {
      const baselineSegment = baselineSlice.filter((row) => String(row.segment || "").toUpperCase() === segment);
      const recommendedSegment = recommendedSlice.filter((row) => String(row.segment || "").toUpperCase() === segment);
      const segmentAgg = aggregateStrategySlice(baselineSegment, recommendedSegment);
      return {
        segment,
        bids: segmentAgg.bids,
        sold: segmentAgg.sold,
        wr: segmentAgg.wr,
        total_spend: segmentAgg.total_spend,
        quotes: segmentAgg.quotes,
        sold_to_quotes: segmentAgg.quotes > 0 ? segmentAgg.sold / segmentAgg.quotes : null,
        binds: segmentAgg.binds,
        q2b: segmentAgg.q2b,
        cor: segmentAgg.cor,
        roe: segmentAgg.roe,
        cpb: segmentAgg.current_cpb,
        performance: segmentAgg.performance
      };
    });

    return {
      state: stateRow.state,
      rule_name: stateRow.rule_name,
      tier: stateRow.tier,
      strategy_key: stateRow.strategy_key,
      strategy_label: stateRow.strategy_label,
      kpis: {
        ...agg,
        rule_name: stateRow.rule_name || "State",
        states: [stateRow.state],
        segments
      },
      segment_rows: segmentRows
    };
  });

  return {
    overall: {
      ...overallAgg,
      rule_name: "All rules",
      states: uniqueStates,
      segments: uniqueSegments
    },
    states: stateRows,
    state_details: stateDetails,
    rules: ruleRows
  };
}
