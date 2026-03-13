import { randomUUID } from "node:crypto";
import { query, table } from "../db/index.js";
import { config } from "../config.js";
import { splitCombinedFilter } from "./shared/activityScope.js";
import { buildCombinedRatioSql, buildRoeSql } from "./shared/kpiSql.js";
import { getStrategyRulesForPlan } from "./analyticsService.js";
const RAW_CROSS_TACTIC_TABLE = config.rawCrossTacticTable;
let targetsTableReady = null;
let targetsPerfDailyReady = null;
let targetsPerfDailyCheckedAt = 0;
let targetsPlanIdColumnReady = null;
let targetsPlanIdCheckedAt = 0;
const TARGETS_PERF_DAILY_CHECK_TTL_MS = 5 * 60 * 1000;
const TARGETS_PLAN_ID_CHECK_TTL_MS = 5 * 60 * 1000;
function normalizeFilters(filters) {
    const combined = splitCombinedFilter(filters.activityLeadType);
    return {
        startDate: filters.startDate || "",
        endDate: filters.endDate || "",
        stateSegmentActivityType: combined.stateSegmentActivityType,
        stateSegmentLeadType: combined.stateSegmentLeadType,
        qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0
    };
}
async function hasTargetsPerfDailyTable() {
    // PG always has the table (created via migration)
    if (config.usePg)
        return true;
    const now = Date.now();
    if (!targetsPerfDailyReady || now - targetsPerfDailyCheckedAt > TARGETS_PERF_DAILY_CHECK_TTL_MS) {
        targetsPerfDailyCheckedAt = now;
        targetsPerfDailyReady = query(`
        SELECT 1 AS present
        FROM \`${config.projectId}.${config.dataset}.INFORMATION_SCHEMA.TABLES\`
        WHERE table_name = 'targets_perf_daily'
        LIMIT 1
      `)
            .then((rows) => rows.length > 0)
            .catch(() => false);
    }
    return targetsPerfDailyReady;
}
async function hasTargetsPlanIdColumn() {
    // PG schema always has plan_id column
    if (config.usePg)
        return true;
    const now = Date.now();
    if (!targetsPlanIdColumnReady || now - targetsPlanIdCheckedAt > TARGETS_PLAN_ID_CHECK_TTL_MS) {
        targetsPlanIdCheckedAt = now;
        targetsPlanIdColumnReady = query(`
        SELECT 1 AS present
        FROM \`${config.projectId}.${config.dataset}.INFORMATION_SCHEMA.COLUMNS\`
        WHERE table_name = 'targets'
          AND column_name = 'plan_id'
        LIMIT 1
      `)
            .then((rows) => rows.length > 0)
            .catch(() => false);
    }
    return targetsPlanIdColumnReady;
}
let targetsAltColumnReady = null;
let targetsAltCheckedAt = 0;
const TARGETS_ALT_CHECK_TTL_MS = 5 * 60 * 1000;
async function hasTargetsActivityLeadTypeColumn() {
    if (config.usePg)
        return true;
    const now = Date.now();
    if (!targetsAltColumnReady || now - targetsAltCheckedAt > TARGETS_ALT_CHECK_TTL_MS) {
        targetsAltCheckedAt = now;
        targetsAltColumnReady = query(`
        SELECT 1 AS present
        FROM \`${config.projectId}.${config.dataset}.INFORMATION_SCHEMA.COLUMNS\`
        WHERE table_name = 'targets'
          AND column_name = 'activity_lead_type'
        LIMIT 1
      `)
            .then((rows) => rows.length > 0)
            .catch(() => false);
    }
    return targetsAltColumnReady;
}
function normalizeState(value) {
    return value.trim().toUpperCase();
}
function normalizeSegment(value) {
    return value.trim().toUpperCase();
}
function normalizeSource(value) {
    return value.trim();
}
function normalizeAccountId(value) {
    const raw = String(value || "").trim();
    if (!raw) {
        return "";
    }
    return raw.replace(/\.0+$/, "");
}
function getPerfScopedCteFromRaw() {
    return `
      WITH perf_base AS (
        SELECT
          DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
          UPPER(Data_State) AS state,
          UPPER(
            COALESCE(
              NULLIF(TRIM(Segments), ''),
              REGEXP_EXTRACT(UPPER(COALESCE(ChannelGroupName, '')), r'(MCH|MCR|SCH|SCR)')
            )
          ) AS segment,
          COALESCE(ChannelGroupName, '') AS channel_group_name,
          COALESCE(CAST(Account_Name AS STRING), '') AS account_name,
          COALESCE(CAST(CompanyAccountId AS STRING), '') AS company_account_id,
          SAFE_CAST(TotalBinds AS FLOAT64) AS total_binds,
          SAFE_CAST(Transaction_sold AS FLOAT64) AS transaction_sold,
          SAFE_CAST(TransactionSold AS FLOAT64) AS transaction_sold_alt,
          SAFE_CAST(Price AS FLOAT64) AS price,
          SAFE_CAST(Target_TargetCPB AS FLOAT64) AS bq_target_cpb,
          SAFE_CAST(ScoredPolicies AS FLOAT64) AS scored_policies,
          SAFE_CAST(LifetimePremium AS FLOAT64) AS lifetime_premium,
          SAFE_CAST(LifeTimeCost AS FLOAT64) AS lifetime_cost,
          SAFE_CAST(CustomValues_Profit AS FLOAT64) AS avg_profit,
          SAFE_CAST(Equity AS FLOAT64) AS avg_equity
        FROM ${RAW_CROSS_TACTIC_TABLE}
        WHERE (@stateSegmentActivityType = "" OR LOWER(activitytype) = LOWER(@stateSegmentActivityType))
          AND (@stateSegmentLeadType = "" OR LOWER(Leadtype) = LOWER(@stateSegmentLeadType))
      ),
      perf AS (
        SELECT
          state,
          segment,
          REGEXP_REPLACE(LOWER(account_name), r'[^a-z0-9]+', '') AS source_key,
          SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
          SUM(COALESCE(total_binds, 0)) AS binds,
          SUM(COALESCE(scored_policies, 0)) AS scored_policies,
          SAFE_DIVIDE(
            SUM(COALESCE(price, 0)),
            NULLIF(SUM(COALESCE(total_binds, 0)), 0)
          ) AS cpb,
          CASE
            WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
            ELSE SAFE_DIVIDE(
              SUM(COALESCE(bq_target_cpb, 0)),
              SUM(COALESCE(total_binds, 0))
            )
          END AS target_cpb,
          SAFE_DIVIDE(
            CASE
              WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
              ELSE SAFE_DIVIDE(
                SUM(COALESCE(bq_target_cpb, 0)),
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
        avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(COALESCE(lifetime_premium, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))"
    })} AS combined_ratio,
          SAFE_DIVIDE(
            SUM(COALESCE(avg_profit, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_profit,
          SAFE_DIVIDE(
            SUM(COALESCE(avg_equity, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_equity,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_premium, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_premium,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_cost, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_cost
        FROM perf_base
        WHERE (@startDate = "" OR event_date >= DATE(@startDate))
          AND (@endDate = "" OR event_date <= DATE(@endDate))
          AND segment IN ('MCH', 'MCR', 'SCH', 'SCR', 'HOME', 'RENT')
        GROUP BY state, segment, source_key
      )
  `;
}
function getPerfScopedCteFromDaily() {
    const tbl = table("targets_perf_daily");
    // PG regex: regexp_replace needs 'g' flag; BQ uses r'' prefix
    const regexpReplace = config.usePg
        ? "regexp_replace(lower(account_name), '[^a-z0-9]+', '', 'g')"
        : "REGEXP_REPLACE(LOWER(account_name), r'[^a-z0-9]+', '')";
    const castNull = config.usePg ? "NULL::double precision" : "CAST(NULL AS FLOAT64)";
    const dateExpr = (p) => config.usePg ? `${p}::date` : `DATE(${p})`;
    const emptyCheck = (col) => config.usePg
        ? `(@${col} = '' OR lower(${col === "stateSegmentActivityType" ? "activity_type" : "lead_type"}) = lower(@${col}))`
        : `(@${col} = "" OR LOWER(${col === "stateSegmentActivityType" ? "activity_type" : "lead_type"}) = LOWER(@${col}))`;
    const emptyStr = config.usePg ? "''" : '""';
    return `
      WITH perf_base AS (
        SELECT
          event_date,
          state,
          segment,
          source_key AS account_name,
          company_account_id,
          binds AS total_binds,
          sold AS transaction_sold,
          ${castNull} AS transaction_sold_alt,
          price_sum AS price,
          target_cpb_sum AS bq_target_cpb,
          scored_policies,
          lifetime_premium_sum AS lifetime_premium,
          lifetime_cost_sum AS lifetime_cost,
          avg_profit_sum AS avg_profit,
          avg_equity_sum AS avg_equity
        FROM ${tbl}
        WHERE ${emptyCheck("stateSegmentActivityType")}
          AND ${emptyCheck("stateSegmentLeadType")}
      ),
      perf AS (
        SELECT
          state,
          segment,
          ${regexpReplace} AS source_key,
          SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
          SUM(COALESCE(total_binds, 0)) AS binds,
          SUM(COALESCE(scored_policies, 0)) AS scored_policies,
          SAFE_DIVIDE(
            SUM(COALESCE(price, 0)),
            NULLIF(SUM(COALESCE(total_binds, 0)), 0)
          ) AS cpb,
          CASE
            WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
            ELSE SAFE_DIVIDE(
              SUM(COALESCE(bq_target_cpb, 0)),
              SUM(COALESCE(total_binds, 0))
            )
          END AS target_cpb,
          SAFE_DIVIDE(
            CASE
              WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
              ELSE SAFE_DIVIDE(
                SUM(COALESCE(bq_target_cpb, 0)),
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
        avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(COALESCE(lifetime_premium, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))"
    })} AS combined_ratio,
          SAFE_DIVIDE(
            SUM(COALESCE(avg_profit, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_profit,
          SAFE_DIVIDE(
            SUM(COALESCE(avg_equity, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_equity,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_premium, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_premium,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_cost, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_cost
        FROM perf_base
        WHERE (@startDate = ${emptyStr} OR event_date >= ${dateExpr("@startDate")})
          AND (@endDate = ${emptyStr} OR event_date <= ${dateExpr("@endDate")})
          AND segment IN ('MCH', 'MCR', 'SCH', 'SCR', 'HOME', 'RENT')
        GROUP BY state, segment, source_key
      )
  `;
}
function ensureTargetsTableExists() {
    if (config.usePg)
        return Promise.resolve();
    if (!targetsTableReady) {
        targetsTableReady = query(`
        CREATE TABLE IF NOT EXISTS ${table("targets")} (
          target_id STRING NOT NULL,
          plan_id STRING,
          state STRING NOT NULL,
          segment STRING NOT NULL,
          source STRING NOT NULL,
          target_value FLOAT64 NOT NULL,
          created_by STRING NOT NULL,
          created_at TIMESTAMP NOT NULL,
          updated_by STRING NOT NULL,
          updated_at TIMESTAMP NOT NULL
        )
      `).then(() => undefined);
    }
    return targetsTableReady;
}
export async function getTarget(targetId) {
    await ensureTargetsTableExists();
    const rows = await query(`SELECT target_id, state, segment, source, target_value, target_cor FROM ${table("targets")} WHERE target_id = @targetId LIMIT 1`, { targetId });
    return rows[0] ?? null;
}
export async function listTargets(filters) {
    await ensureTargetsTableExists();
    const normalized = normalizeFilters(filters);
    const perfScopedCte = (await hasTargetsPerfDailyTable()) ? getPerfScopedCteFromDaily() : getPerfScopedCteFromRaw();
    const hasPlanIdColumn = await hasTargetsPlanIdColumn();
    const hasAltColumn = await hasTargetsActivityLeadTypeColumn();
    const planId = String(filters.planId || "").trim();
    const activityLeadType = String(filters.activityLeadType || "").trim();
    const conditions = [];
    if (hasPlanIdColumn && planId)
        conditions.push("t.plan_id = @planId");
    if (hasAltColumn && activityLeadType)
        conditions.push("t.activity_lead_type = @altFilter");
    const whereClause = conditions.length ? "WHERE " + conditions.join(" AND ") : "";
    const queryParams = { ...normalized, planId, altFilter: activityLeadType };
    const castCreated = config.usePg ? "t.created_at::text" : "CAST(t.created_at AS STRING)";
    const castUpdated = config.usePg ? "t.updated_at::text" : "CAST(t.updated_at AS STRING)";
    // Safe division helper — works in both BQ and PG
    const sd = (num, den) => `COALESCE((${num}) / NULLIF((${den}), 0), 0)`;
    const rows = await query(`
      ${perfScopedCte},
      perf_agg AS (
        SELECT
          state,
          segment,
          COALESCE(SUM(sold), 0) AS sold,
          COALESCE(SUM(binds), 0) AS binds,
          COALESCE(SUM(scored_policies), 0) AS scored_policies,
          ${sd("SUM(COALESCE(cpb, 0) * COALESCE(binds, 0))", "SUM(COALESCE(binds, 0))")} AS cpb,
          ${sd("SUM(COALESCE(target_cpb, 0) * COALESCE(binds, 0))", "SUM(COALESCE(binds, 0))")} AS target_cpb,
          ${sd("SUM(COALESCE(avg_profit, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_profit,
          ${sd("SUM(COALESCE(avg_equity, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_equity,
          ${sd("SUM(COALESCE(avg_lifetime_premium, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_lifetime_premium,
          ${sd("SUM(COALESCE(avg_lifetime_cost, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_lifetime_cost
        FROM perf
        GROUP BY state, segment
      )
      SELECT
        t.target_id,
        t.state,
        t.segment,
        t.source,
        t.target_value,
        COALESCE(t.target_cor, 0) AS target_cor,
        pa.target_cpb AS current_target,
        pa.sold,
        pa.binds,
        COALESCE(pa.scored_policies, 0) AS scored_policies,
        pa.cpb,
        pa.target_cpb,
        ${sd("pa.target_cpb", "pa.cpb")} AS performance,
        CASE
          WHEN COALESCE(pa.scored_policies, 0) = 0 OR COALESCE(pa.avg_equity, 0) = 0 THEN 0
          ELSE ${sd(`(pa.avg_profit - 0.8 * (${sd("pa.cpb", "0.81")} + @qbc))`, "pa.avg_equity")}
        END AS roe,
        CASE
          WHEN COALESCE(pa.scored_policies, 0) = 0 OR COALESCE(pa.avg_lifetime_premium, 0) = 0 THEN 0
          ELSE ${sd(`(${sd("pa.cpb", "0.81")} + @qbc + pa.avg_lifetime_cost)`, "pa.avg_lifetime_premium")}
        END AS combined_ratio,
        pa.avg_profit,
        pa.avg_equity,
        pa.avg_lifetime_premium,
        pa.avg_lifetime_cost,
        ${castCreated} AS created_at,
        ${castUpdated} AS updated_at
      FROM ${table("targets")} t
      LEFT JOIN perf_agg pa
        ON pa.state = t.state
       AND pa.segment = t.segment
      ${whereClause}
      ORDER BY t.state, t.segment, t.source
    `, queryParams);
    // Populate target_cor from plan strategy rules (authoritative source)
    if (planId) {
        try {
            const rules = await getStrategyRulesForPlan(planId, filters.activityLeadType);
            if (rules.length > 0) {
                // Build state+segment -> corTarget lookup from rules
                const corLookup = new Map();
                for (const rule of rules) {
                    if (rule.corTarget <= 0)
                        continue;
                    for (const st of rule.states) {
                        for (const seg of rule.segments) {
                            const key = `${st}|${seg}`;
                            if (!corLookup.has(key))
                                corLookup.set(key, rule.corTarget);
                        }
                    }
                }
                for (const row of rows) {
                    const ruleVal = corLookup.get(`${row.state}|${row.segment}`);
                    if (ruleVal) {
                        row.target_cor = ruleVal;
                    }
                }
            }
        }
        catch {
            // Non-fatal: strategy rules lookup failed, rows keep their DB target_cor
        }
    }
    return rows;
}
export async function getTargetsMetrics(rows, filters) {
    const normalized = normalizeFilters(filters);
    const perfScopedCte = (await hasTargetsPerfDailyTable()) ? getPerfScopedCteFromDaily() : getPerfScopedCteFromRaw();
    const rowsJson = JSON.stringify(rows.map((row) => ({
        state: normalizeState(row.state || ""),
        segment: normalizeSegment(row.segment || ""),
        source: normalizeSource(row.source || ""),
        accountId: normalizeAccountId(row.accountId)
    })));
    const regexpReplace = config.usePg
        ? "regexp_replace(lower(account_name), '[^a-z0-9]+', '', 'g')"
        : "REGEXP_REPLACE(LOWER(account_name), r'[^a-z0-9]+', '')";
    const emptyStr = config.usePg ? "''" : '""';
    const dateExpr = (p) => config.usePg ? `${p}::date` : `DATE(${p})`;
    // BQ and PG use different JSON/array-unnest syntax for input_rows CTE
    const inputRowsCte = config.usePg
        ? `
      input_rows AS (
        SELECT
          upper(item->>'state') AS state,
          upper(item->>'segment') AS segment,
          item->>'source' AS source,
          COALESCE(item->>'accountId', '') AS account_id,
          regexp_replace(lower(item->>'source'), '[^a-z0-9]+', '', 'g') AS source_key
        FROM jsonb_array_elements(@rowsJson::jsonb) AS item
      )`
        : `
      input_rows AS (
        SELECT
          UPPER(JSON_VALUE(item, '$.state')) AS state,
          UPPER(JSON_VALUE(item, '$.segment')) AS segment,
          JSON_VALUE(item, '$.source') AS source,
          COALESCE(JSON_VALUE(item, '$.accountId'), '') AS account_id,
          REGEXP_REPLACE(LOWER(JSON_VALUE(item, '$.source')), r'[^a-z0-9]+', '') AS source_key
        FROM UNNEST(JSON_EXTRACT_ARRAY(@rowsJson)) AS item
      )`;
    const emptyIdCheck = config.usePg ? "''" : '""';
    return query(`
      ${perfScopedCte},
      perf_account AS (
        SELECT
          state,
          segment,
          company_account_id,
          ${regexpReplace} AS source_key,
          SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
          SUM(COALESCE(total_binds, 0)) AS binds,
          SUM(COALESCE(scored_policies, 0)) AS scored_policies,
          SAFE_DIVIDE(
            SUM(COALESCE(price, 0)),
            NULLIF(SUM(COALESCE(total_binds, 0)), 0)
          ) AS cpb,
          CASE
            WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
            ELSE SAFE_DIVIDE(
              SUM(COALESCE(bq_target_cpb, 0)),
              SUM(COALESCE(total_binds, 0))
            )
          END AS target_cpb,
          SAFE_DIVIDE(
            CASE
              WHEN SUM(COALESCE(total_binds, 0)) = 0 THEN 0
              ELSE SAFE_DIVIDE(
                SUM(COALESCE(bq_target_cpb, 0)),
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
        avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(COALESCE(lifetime_premium, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0))"
    })} AS combined_ratio,
          SAFE_DIVIDE(
            SUM(COALESCE(avg_profit, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_profit,
          SAFE_DIVIDE(
            SUM(COALESCE(avg_equity, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_equity,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_premium, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_premium,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_cost, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_cost
        FROM perf_base
        WHERE (@startDate = ${emptyStr} OR event_date >= ${dateExpr("@startDate")})
          AND (@endDate = ${emptyStr} OR event_date <= ${dateExpr("@endDate")})
          AND segment IN ('MCH', 'MCR', 'SCH', 'SCR', 'HOME', 'RENT')
        GROUP BY state, segment, company_account_id, source_key
      ),
      ${inputRowsCte}
      SELECT
        i.state,
        i.segment,
        i.source,
        i.account_id,
        COALESCE(pa.target_cpb, p.target_cpb) AS current_target,
        COALESCE(pa.sold, p.sold) AS sold,
        COALESCE(pa.binds, p.binds) AS binds,
        COALESCE(pa.scored_policies, p.scored_policies, 0) AS scored_policies,
        COALESCE(pa.cpb, p.cpb) AS cpb,
        COALESCE(pa.target_cpb, p.target_cpb) AS target_cpb,
        COALESCE(pa.performance, p.performance) AS performance,
        COALESCE(pa.roe, p.roe) AS roe,
        COALESCE(pa.combined_ratio, p.combined_ratio) AS combined_ratio,
        COALESCE(pa.avg_profit, p.avg_profit) AS avg_profit,
        COALESCE(pa.avg_equity, p.avg_equity) AS avg_equity,
        COALESCE(pa.avg_lifetime_premium, p.avg_lifetime_premium) AS avg_lifetime_premium,
        COALESCE(pa.avg_lifetime_cost, p.avg_lifetime_cost) AS avg_lifetime_cost
      FROM input_rows i
      LEFT JOIN perf_account pa
        ON pa.state = i.state
       AND pa.segment = i.segment
       AND i.account_id != ${emptyIdCheck}
       AND pa.company_account_id = i.account_id
       AND pa.source_key = i.source_key
      LEFT JOIN perf p
        ON p.state = i.state
       AND p.segment = i.segment
       AND i.account_id = ${emptyIdCheck}
       AND p.source_key = i.source_key
    `, { ...normalized, rowsJson });
}
export async function createTarget(userId, planId, activityLeadType) {
    await ensureTargetsTableExists();
    const targetId = randomUUID();
    const hasPlanIdColumn = await hasTargetsPlanIdColumn();
    const hasAltColumn = await hasTargetsActivityLeadTypeColumn();
    const normalizedPlanId = String(planId || "").trim();
    const alt = String(activityLeadType || "").trim();
    const altCol = hasAltColumn ? ", activity_lead_type" : "";
    const altVal = hasAltColumn ? ", @alt" : "";
    if (hasPlanIdColumn) {
        await query(`
        INSERT INTO ${table("targets")}
        (target_id, plan_id${altCol}, state, segment, source, target_value, target_cor, created_by, created_at, updated_by, updated_at)
        VALUES (@targetId, NULLIF(@planId, '')${altVal}, 'NA', 'MCH', 'New Source', 0, 0, @userId, CURRENT_TIMESTAMP(), @userId, CURRENT_TIMESTAMP())
      `, { targetId, userId, planId: normalizedPlanId, alt });
        return { targetId };
    }
    await query(`
      INSERT INTO ${table("targets")}
      (target_id${altCol}, state, segment, source, target_value, target_cor, created_by, created_at, updated_by, updated_at)
      VALUES (@targetId${altVal}, 'NA', 'MCH', 'New Source', 0, 0, @userId, CURRENT_TIMESTAMP(), @userId, CURRENT_TIMESTAMP())
    `, { targetId, userId, alt });
    return { targetId };
}
export async function batchCreateTargets(rows, userId, planId, activityLeadType) {
    await ensureTargetsTableExists();
    const hasPlanIdColumn = await hasTargetsPlanIdColumn();
    const hasAltColumn = await hasTargetsActivityLeadTypeColumn();
    const alt = String(activityLeadType || "").trim();
    const BATCH = 500;
    const esc = (s) => s.replace(/'/g, "''");
    for (let i = 0; i < rows.length; i += BATCH) {
        const batch = rows.slice(i, i + BATCH);
        const vals = batch
            .map((r) => {
            const id = randomUUID();
            const st = normalizeState(r.state);
            const seg = normalizeSegment(r.segment);
            const src = normalizeSource(r.source);
            const val = Number(r.targetValue) || 0;
            const altPart = hasAltColumn ? `, '${esc(alt)}'` : "";
            if (hasPlanIdColumn) {
                return `('${id}', '${esc(planId)}'${altPart}, '${esc(st)}', '${esc(seg)}', '${esc(src)}', ${val}, 0, '${esc(userId)}', CURRENT_TIMESTAMP(), '${esc(userId)}', CURRENT_TIMESTAMP())`;
            }
            return `('${id}'${altPart}, '${esc(st)}', '${esc(seg)}', '${esc(src)}', ${val}, 0, '${esc(userId)}', CURRENT_TIMESTAMP(), '${esc(userId)}', CURRENT_TIMESTAMP())`;
        })
            .join(",\n");
        let cols = hasPlanIdColumn
            ? "target_id, plan_id, state, segment, source, target_value, target_cor, created_by, created_at, updated_by, updated_at"
            : "target_id, state, segment, source, target_value, target_cor, created_by, created_at, updated_by, updated_at";
        if (hasAltColumn) {
            cols = hasPlanIdColumn
                ? "target_id, plan_id, activity_lead_type, state, segment, source, target_value, target_cor, created_by, created_at, updated_by, updated_at"
                : "target_id, activity_lead_type, state, segment, source, target_value, target_cor, created_by, created_at, updated_by, updated_at";
        }
        await query(`INSERT INTO ${table("targets")} (${cols}) VALUES ${vals}`);
    }
    return rows.length;
}
export async function updateTarget(targetId, input, userId, planId, activityLeadType) {
    await ensureTargetsTableExists();
    const updates = [];
    const params = { targetId, userId };
    const hasPlanIdColumn = await hasTargetsPlanIdColumn();
    const hasAltColumn = await hasTargetsActivityLeadTypeColumn();
    const normalizedPlanId = String(planId || "").trim();
    const alt = String(activityLeadType || "").trim();
    if (typeof input.state === "string") {
        updates.push("state = @state");
        params.state = normalizeState(input.state);
    }
    if (typeof input.segment === "string") {
        updates.push("segment = @segment");
        params.segment = normalizeSegment(input.segment);
    }
    if (typeof input.source === "string") {
        updates.push("source = @source");
        params.source = normalizeSource(input.source);
    }
    if (typeof input.targetValue === "number" && Number.isFinite(input.targetValue)) {
        updates.push("target_value = @targetValue");
        params.targetValue = input.targetValue;
    }
    if (typeof input.targetCor === "number" && Number.isFinite(input.targetCor)) {
        updates.push("target_cor = @targetCor");
        params.targetCor = input.targetCor;
    }
    if (!updates.length) {
        return;
    }
    updates.push("updated_by = @userId");
    updates.push("updated_at = CURRENT_TIMESTAMP()");
    const allParams = { ...params, ...(hasPlanIdColumn && normalizedPlanId ? { planId: normalizedPlanId } : {}), ...(hasAltColumn && alt ? { alt } : {}) };
    await query(`
      UPDATE ${table("targets")}
      SET ${updates.join(",\n          ")}
      WHERE target_id = @targetId
      ${hasPlanIdColumn && normalizedPlanId ? "  AND plan_id = @planId" : ""}
      ${hasAltColumn && alt ? "  AND activity_lead_type = @alt" : ""}
    `, allParams);
}
export async function listDefaultTargetsWithPerf(targetKey, filters) {
    const normalized = normalizeFilters(filters);
    const perfScopedCte = (await hasTargetsPerfDailyTable()) ? getPerfScopedCteFromDaily() : getPerfScopedCteFromRaw();
    const planId = String(filters.planId || "").trim();
    const coalesce = config.usePg ? "COALESCE" : "IFNULL";
    const sd = (num, den) => `COALESCE((${num}) / NULLIF((${den}), 0), 0)`;
    const rows = await query(`
      ${perfScopedCte},
      perf_agg AS (
        SELECT
          state,
          segment,
          COALESCE(SUM(sold), 0) AS sold,
          COALESCE(SUM(binds), 0) AS binds,
          COALESCE(SUM(scored_policies), 0) AS scored_policies,
          ${sd("SUM(COALESCE(cpb, 0) * COALESCE(binds, 0))", "SUM(COALESCE(binds, 0))")} AS cpb,
          ${sd("SUM(COALESCE(target_cpb, 0) * COALESCE(binds, 0))", "SUM(COALESCE(binds, 0))")} AS target_cpb,
          ${sd("SUM(COALESCE(avg_profit, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_profit,
          ${sd("SUM(COALESCE(avg_equity, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_equity,
          ${sd("SUM(COALESCE(avg_lifetime_premium, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_lifetime_premium,
          ${sd("SUM(COALESCE(avg_lifetime_cost, 0) * COALESCE(scored_policies, 0))", "SUM(COALESCE(scored_policies, 0))")} AS avg_lifetime_cost
        FROM perf
        GROUP BY state, segment
      )
      SELECT
        dt.id AS target_id,
        dt.state,
        dt.segment,
        dt.source,
        dt.target_value,
        0 AS target_cor,
        ${coalesce}(dt.account_id, 0) AS account_id,
        ${coalesce}(dt.company_id, 0) AS company_id,
        ${coalesce}(dt.original_id, '') AS original_id,
        ${coalesce}(dt.segment_name, '') AS segment_name,
        ${coalesce}(dt.attributes, '') AS attributes,
        pa.target_cpb AS current_target,
        pa.sold,
        pa.binds,
        COALESCE(pa.scored_policies, 0) AS scored_policies,
        pa.cpb,
        pa.target_cpb,
        ${sd("pa.target_cpb", "pa.cpb")} AS performance,
        CASE
          WHEN COALESCE(pa.scored_policies, 0) = 0 OR COALESCE(pa.avg_equity, 0) = 0 THEN 0
          ELSE ${sd(`(pa.avg_profit - 0.8 * (${sd("pa.cpb", "0.81")} + @qbc))`, "pa.avg_equity")}
        END AS roe,
        CASE
          WHEN COALESCE(pa.scored_policies, 0) = 0 OR COALESCE(pa.avg_lifetime_premium, 0) = 0 THEN 0
          ELSE ${sd(`(${sd("pa.cpb", "0.81")} + @qbc + pa.avg_lifetime_cost)`, "pa.avg_lifetime_premium")}
        END AS combined_ratio,
        pa.avg_profit,
        pa.avg_equity,
        pa.avg_lifetime_premium,
        pa.avg_lifetime_cost
      FROM ${table("default_targets")} dt
      LEFT JOIN perf_agg pa
        ON pa.state = dt.state
       AND pa.segment = dt.segment
      WHERE dt.target_key = @targetKey
      ORDER BY dt.state, dt.segment, dt.source
    `, { ...normalized, targetKey });
    // Populate target_cor from plan strategy rules
    if (planId) {
        try {
            const rules = await getStrategyRulesForPlan(planId, filters.activityLeadType);
            if (rules.length > 0) {
                const corLookup = new Map();
                for (const rule of rules) {
                    if (rule.corTarget <= 0)
                        continue;
                    for (const st of rule.states) {
                        for (const seg of rule.segments) {
                            const k = `${st}|${seg}`;
                            if (!corLookup.has(k))
                                corLookup.set(k, rule.corTarget);
                        }
                    }
                }
                for (const row of rows) {
                    const ruleVal = corLookup.get(`${row.state}|${row.segment}`);
                    if (ruleVal)
                        row.target_cor = ruleVal;
                }
            }
        }
        catch {
            // Non-fatal
        }
    }
    return rows;
}
