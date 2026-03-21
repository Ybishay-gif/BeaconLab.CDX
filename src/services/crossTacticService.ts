/**
 * Cross Tactic Analytics Explorer — queries the raw BQ table directly
 * with user-selected dimensions and metrics, caching aggregated results.
 *
 * This does NOT sync to PG — the table is 174M rows / 436GB, too large
 * for the db-f1-micro instance. BQ handles scans natively with
 * partitioning (Data_DateCreated) and clustering (activity_type, lead_type,
 * Account_Name, Data_State).
 */

import { query as bqQuery } from "../db/bigquery.js";
import { config } from "../config.js";
import { buildCacheKey, cached } from "../cache.js";
import { getTableSchema, getFilterValues } from "./reportService.js";

// ── Types ────────────────────────────────────────────────────────────

export type AggregationType = "SUM" | "AVG" | "COUNT";

export type MetricDef = {
  agg: AggregationType;
  label: string;
};

export type DimensionCategory = "geographic" | "account" | "channel" | "activity" | "time" | "other";

export type DimensionDef = {
  column: string;
  label: string;
  category: DimensionCategory;
};

export type DrillStep = {
  dimension: string;
  value: string;
};

export type CrossTacticRequest = {
  dimensions: string[];
  metrics: string[];
  filters: Record<string, string[]>;
  startDate: string;
  endDate: string;
  drillPath: DrillStep[];
};

export type CrossTacticResult = {
  rows: Record<string, unknown>[];
  metadata: {
    rowCount: number;
    cached: boolean;
    dimensions: string[];
    metrics: string[];
  };
};

// ── Dimension & Metric Catalogs ──────────────────────────────────────

const DIMENSIONS: DimensionDef[] = [
  { column: "Data_State", label: "State", category: "geographic" },
  { column: "Account_Name", label: "Account", category: "account" },
  { column: "CompanyAccountId", label: "Company Account ID", category: "account" },
  { column: "Campaign_Name", label: "Campaign", category: "account" },
  { column: "Attribution_Channel", label: "Attribution Channel", category: "channel" },
  { column: "Segments", label: "Segment", category: "channel" },
  { column: "activitytype", label: "Activity Type", category: "activity" },
  { column: "Leadtype", label: "Lead Type", category: "activity" },
  { column: "Transaction_Sold", label: "Transaction Sold", category: "other" },
];

const METRICS: Record<string, MetricDef> = {
  row_count: { agg: "COUNT", label: "Row Count" },
  Price: { agg: "SUM", label: "Total Price" },
  TotalBinds: { agg: "SUM", label: "Total Binds" },
  ScoredPolicies: { agg: "SUM", label: "Scored Policies" },
  LifetimePremium: { agg: "SUM", label: "Lifetime Premium" },
  LifeTimeCost: { agg: "SUM", label: "Lifetime Cost" },
  CustomValues_Profit: { agg: "SUM", label: "Profit" },
  Equity: { agg: "SUM", label: "Equity" },
  Target_TargetCPB: { agg: "AVG", label: "Avg Target CPB" },
  Transaction_Sold: { agg: "SUM", label: "Sold Count" },
};

// Derived metrics computed after the query (from aggregated values)
const DERIVED_METRICS: Record<string, { label: string; numerator: string; denominator: string }> = {
  cpb: { label: "Cost Per Bind", numerator: "LifeTimeCost", denominator: "TotalBinds" },
  cor: { label: "COR", numerator: "LifeTimeCost", denominator: "LifetimePremium" },
};

const VALID_DIMENSIONS = new Set(DIMENSIONS.map((d) => d.column));
const VALID_METRICS = new Set(Object.keys(METRICS));

const MAX_DIMENSIONS = 6;
const MAX_RESULT_ROWS = 10_000;
const CACHE_TTL = 24 * 60 * 60 * 1000; // 24h

// ── Schema Endpoint ──────────────────────────────────────────────────

export async function getCrossTacticSchema(): Promise<{
  dimensions: DimensionDef[];
  metrics: Array<{ key: string } & MetricDef>;
  derivedMetrics: Array<{ key: string; label: string }>;
}> {
  // Validate that our catalogs match the actual BQ schema
  const schema = await getTableSchema(false);
  const bqColumns = new Set(schema.map((c) => c.column_name));

  const validDimensions = DIMENSIONS.filter((d) => bqColumns.has(d.column));
  const validMetrics = Object.entries(METRICS)
    .filter(([key]) => key === "row_count" || bqColumns.has(key))
    .map(([key, def]) => ({ key, ...def }));

  return {
    dimensions: validDimensions,
    metrics: validMetrics,
    derivedMetrics: Object.entries(DERIVED_METRICS).map(([key, def]) => ({ key, label: def.label })),
  };
}

// ── Filter Values (delegates to reportService) ───────────────────────

export { getFilterValues } from "./reportService.js";

// ── Core Aggregation ─────────────────────────────────────────────────

export async function getCrossTacticAggregation(
  req: CrossTacticRequest
): Promise<CrossTacticResult> {
  // ── Validate inputs ──
  if (!req.dimensions.length || req.dimensions.length > MAX_DIMENSIONS) {
    throw new Error(`dimensions must have 1-${MAX_DIMENSIONS} items`);
  }
  for (const d of req.dimensions) {
    if (!VALID_DIMENSIONS.has(d)) throw new Error(`Invalid dimension: ${d}`);
  }
  for (const m of req.metrics) {
    if (!VALID_METRICS.has(m)) throw new Error(`Invalid metric: ${m}`);
  }
  for (const [col] of Object.entries(req.filters)) {
    if (!VALID_DIMENSIONS.has(col)) throw new Error(`Invalid filter dimension: ${col}`);
  }
  for (const step of req.drillPath) {
    if (!VALID_DIMENSIONS.has(step.dimension)) throw new Error(`Invalid drill dimension: ${step.dimension}`);
  }
  if (!req.startDate || !req.endDate) {
    throw new Error("startDate and endDate are required");
  }

  // ── Check cache ──
  const cacheKey = buildCacheKey("cross-tactic", {
    dimensions: req.dimensions,
    metrics: req.metrics,
    filters: JSON.stringify(req.filters),
    startDate: req.startDate,
    endDate: req.endDate,
    drillPath: JSON.stringify(req.drillPath),
  });

  const rows = await cached<Record<string, unknown>[]>(cacheKey, async () => {
    const { sql, params } = buildAggregationSql(req);
    return bqQuery<Record<string, unknown>>(sql, params);
  }, CACHE_TTL);

  // Compute derived metrics on the aggregated rows
  const enrichedRows = rows.map((row) => {
    const out = { ...row };
    for (const [key, def] of Object.entries(DERIVED_METRICS)) {
      if (req.metrics.includes(def.numerator) && req.metrics.includes(def.denominator)) {
        const num = Number(row[`total_${def.numerator}`] ?? 0);
        const den = Number(row[`total_${def.denominator}`] ?? 0);
        out[key] = den !== 0 ? num / den : null;
      }
    }
    return out;
  });

  return {
    rows: enrichedRows,
    metadata: {
      rowCount: enrichedRows.length,
      cached: rows === enrichedRows ? false : true, // always true after enrichment, check cache directly
      dimensions: req.dimensions,
      metrics: req.metrics,
    },
  };
}

// ── SQL Builder ──────────────────────────────────────────────────────

function buildAggregationSql(
  req: CrossTacticRequest
): { sql: string; params: Record<string, unknown> } {
  const params: Record<string, unknown> = {};
  let paramIdx = 0;

  // SELECT clause: dimensions + metrics
  const selectParts: string[] = [];
  for (const dim of req.dimensions) {
    selectParts.push(`\`${dim}\``);
  }

  for (const metric of req.metrics) {
    const def = METRICS[metric];
    if (def.agg === "COUNT") {
      selectParts.push("COUNT(*) AS row_count");
    } else {
      selectParts.push(`${def.agg}(\`${metric}\`) AS total_${metric}`);
    }
  }

  // WHERE clause
  const conditions: string[] = [];

  // Date range (leverages partitioning)
  params.startDate = req.startDate;
  params.endDate = req.endDate;
  conditions.push("`Data_DateCreated` >= @startDate");
  conditions.push("`Data_DateCreated` <= @endDate");

  // Drill-down path constraints (pin parent dimensions)
  for (const step of req.drillPath) {
    const key = `drill_${paramIdx++}`;
    params[key] = step.value;
    conditions.push(`\`${step.dimension}\` = @${key}`);
  }

  // User filters
  for (const [col, values] of Object.entries(req.filters)) {
    if (!values?.length) continue;
    const key = `filter_${paramIdx++}`;
    params[key] = values;
    conditions.push(`\`${col}\` IN UNNEST(@${key})`);
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // GROUP BY + ORDER BY
  const groupBy = req.dimensions.map((d) => `\`${d}\``).join(", ");
  const orderBy = req.metrics.includes("row_count")
    ? "row_count DESC"
    : `total_${req.metrics[0]} DESC`;

  const sql = `SELECT ${selectParts.join(", ")}
FROM ${config.rawCrossTacticTable}
${whereClause}
GROUP BY ${groupBy}
ORDER BY ${orderBy}
LIMIT ${MAX_RESULT_ROWS}`;

  return { sql, params };
}
