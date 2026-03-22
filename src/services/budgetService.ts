import { query, table } from "../db/index.js";
import { pgTransaction } from "../db/postgres.js";

// ── Types ────────────────────────────────────────────────────────────

export type BudgetRow = {
  budget_id: string;
  year: number;
  month: number;
  activity_type: string;
  lead_type: string;
  amount: number;
  created_by: string;
  created_at: string;
  updated_by: string;
  updated_at: string;
};

export type AllocationRow = {
  allocation_id: string;
  budget_id: string;
  account_name: string;
  allocation_pct: number;
  created_at: string;
  updated_at: string;
};

export type BudgetWithAllocations = BudgetRow & {
  allocations: AllocationRow[];
};

export type ActualRow = {
  budget_id: string;
  activity_type: string;
  lead_type: string;
  planned: number;
  actual_spend: number;
  variance: number;
  variance_pct: number | null;
};

export type ActualAccountRow = {
  budget_id: string;
  account_name: string;
  allocation_pct: number;
  allocated_amount: number;
  actual_spend: number;
  variance: number;
  variance_pct: number | null;
};

export type ForecastRow = {
  activity_type: string;
  lead_type: string;
  account_name: string | null;
  last_month_spend: number;
  mtd_spend: number;
  predicted_month_end: number;
  planned_budget: number;
  allocated_budget: number | null;
  predicted_vs_budget: number | null;
};

// ── CRUD ─────────────────────────────────────────────────────────────

export async function listBudgets(year: number): Promise<BudgetWithAllocations[]> {
  const budgets = await query<BudgetRow>(
    `SELECT * FROM ${table("budgets")} WHERE year = @year ORDER BY month, activity_type, lead_type`,
    { year }
  );

  if (budgets.length === 0) return [];

  const budgetIds = budgets.map((b) => b.budget_id);
  const allocations = await query<AllocationRow>(
    `SELECT * FROM ${table("budget_allocations")} WHERE budget_id = ANY(@budgetIds) ORDER BY account_name`,
    { budgetIds }
  );

  const allocMap = new Map<string, AllocationRow[]>();
  for (const a of allocations) {
    const arr = allocMap.get(a.budget_id) || [];
    arr.push(a);
    allocMap.set(a.budget_id, arr);
  }

  return budgets.map((b) => ({
    ...b,
    allocations: allocMap.get(b.budget_id) || [],
  }));
}

export async function upsertBudget(
  year: number,
  month: number,
  activityType: string,
  leadType: string,
  amount: number,
  userId: string
): Promise<{ budgetId: string }> {
  const rows = await query<{ budget_id: string }>(
    `INSERT INTO ${table("budgets")} (year, month, activity_type, lead_type, amount, created_by, updated_by)
     VALUES (@year, @month, @activityType, @leadType, @amount, @userId, @userId)
     ON CONFLICT (year, month, activity_type, lead_type)
     DO UPDATE SET amount = @amount, updated_by = @userId, updated_at = NOW()
     RETURNING budget_id`,
    { year, month, activityType, leadType, amount, userId }
  );
  return { budgetId: rows[0].budget_id };
}

export async function deleteBudget(budgetId: string): Promise<void> {
  await query(`DELETE FROM ${table("budgets")} WHERE budget_id = @budgetId`, { budgetId });
}

export async function upsertAllocations(
  budgetId: string,
  allocations: { accountName: string; allocationPct: number }[]
): Promise<void> {
  await pgTransaction(async (exec) => {
    await exec(`DELETE FROM budget_allocations WHERE budget_id = '${budgetId}'`);
    for (const a of allocations) {
      await exec(
        `INSERT INTO budget_allocations (budget_id, account_name, allocation_pct)
         VALUES ('${budgetId}', '${a.accountName.replace(/'/g, "''")}', ${a.allocationPct})`
      );
    }
  });
}

export async function listAccountNames(): Promise<string[]> {
  const rows = await query<{ source_key: string }>(
    `SELECT DISTINCT source_key FROM ${table("targets_perf_daily")}
     WHERE source_key IS NOT NULL AND source_key != ''
     ORDER BY source_key`
  );
  return rows.map((r) => r.source_key);
}

// ── Actuals vs Planned ───────────────────────────────────────────────

export async function getActualsVsPlanned(
  year: number,
  month: number
): Promise<{ summary: ActualRow[]; byAccount: ActualAccountRow[] }> {
  const summary = await query<ActualRow>(
    `SELECT
       b.budget_id,
       b.activity_type,
       b.lead_type,
       b.amount AS planned,
       COALESCE(SUM(t.price_sum), 0) AS actual_spend,
       b.amount - COALESCE(SUM(t.price_sum), 0) AS variance,
       CASE WHEN b.amount > 0
         THEN (b.amount - COALESCE(SUM(t.price_sum), 0)) / b.amount * 100
         ELSE NULL END AS variance_pct
     FROM ${table("budgets")} b
     LEFT JOIN ${table("targets_perf_daily")} t
       ON t.activity_type = b.activity_type
       AND t.lead_type = b.lead_type
       AND EXTRACT(YEAR FROM t.event_date) = b.year
       AND EXTRACT(MONTH FROM t.event_date) = b.month
     WHERE b.year = @year AND b.month = @month
     GROUP BY b.budget_id, b.activity_type, b.lead_type, b.amount
     ORDER BY b.activity_type, b.lead_type`,
    { year, month }
  );

  const byAccount = await query<ActualAccountRow>(
    `SELECT
       ba.budget_id,
       ba.account_name,
       ba.allocation_pct,
       b.amount * ba.allocation_pct / 100 AS allocated_amount,
       COALESCE(SUM(t.price_sum), 0) AS actual_spend,
       (b.amount * ba.allocation_pct / 100) - COALESCE(SUM(t.price_sum), 0) AS variance,
       CASE WHEN b.amount * ba.allocation_pct / 100 > 0
         THEN ((b.amount * ba.allocation_pct / 100) - COALESCE(SUM(t.price_sum), 0))
              / (b.amount * ba.allocation_pct / 100) * 100
         ELSE NULL END AS variance_pct
     FROM ${table("budget_allocations")} ba
     JOIN ${table("budgets")} b ON b.budget_id = ba.budget_id
     LEFT JOIN ${table("targets_perf_daily")} t
       ON t.source_key = ba.account_name
       AND t.activity_type = b.activity_type
       AND t.lead_type = b.lead_type
       AND EXTRACT(YEAR FROM t.event_date) = b.year
       AND EXTRACT(MONTH FROM t.event_date) = b.month
     WHERE b.year = @year AND b.month = @month
     GROUP BY ba.budget_id, ba.account_name, ba.allocation_pct, b.amount
     ORDER BY ba.account_name`,
    { year, month }
  );

  return { summary, byAccount };
}

// ── Forecasting ──────────────────────────────────────────────────────

export async function getForecast(
  year: number,
  month: number
): Promise<{
  total: ForecastRow;
  byActivityLead: ForecastRow[];
  byAccount: ForecastRow[];
}> {
  // Compute previous month
  const lastMonth = month === 1 ? 12 : month - 1;
  const lastYear = month === 1 ? year - 1 : year;

  // Days remaining in current month
  const daysInMonth = new Date(year, month, 0).getDate();
  const today = new Date();
  const currentDay = today.getDate();
  const isCurrentMonth = today.getFullYear() === year && today.getMonth() + 1 === month;
  const remainingDays = isCurrentMonth ? Math.max(0, daysInMonth - currentDay) : 0;

  // Get raw data from PG — grouped by activity_type, lead_type, source_key
  const rawRows = await query<{
    activity_type: string;
    lead_type: string;
    source_key: string;
    last_month_spend: number;
    mtd_spend: number;
    avg_daily_7d: number;
  }>(
    `WITH last_month AS (
       SELECT activity_type, lead_type, source_key,
              SUM(price_sum) AS spend
       FROM ${table("targets_perf_daily")}
       WHERE EXTRACT(YEAR FROM event_date) = @lastYear
         AND EXTRACT(MONTH FROM event_date) = @lastMonth
       GROUP BY activity_type, lead_type, source_key
     ),
     mtd AS (
       SELECT activity_type, lead_type, source_key,
              SUM(price_sum) AS spend
       FROM ${table("targets_perf_daily")}
       WHERE EXTRACT(YEAR FROM event_date) = @year
         AND EXTRACT(MONTH FROM event_date) = @month
         AND event_date < CURRENT_DATE
       GROUP BY activity_type, lead_type, source_key
     ),
     last_7d AS (
       SELECT activity_type, lead_type, source_key,
              SUM(price_sum) / NULLIF(COUNT(DISTINCT event_date), 0) AS avg_daily
       FROM ${table("targets_perf_daily")}
       WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'
         AND event_date < CURRENT_DATE
       GROUP BY activity_type, lead_type, source_key
     )
     SELECT
       COALESCE(m.activity_type, l.activity_type, s.activity_type) AS activity_type,
       COALESCE(m.lead_type, l.lead_type, s.lead_type) AS lead_type,
       COALESCE(m.source_key, l.source_key, s.source_key) AS source_key,
       COALESCE(l.spend, 0) AS last_month_spend,
       COALESCE(m.spend, 0) AS mtd_spend,
       COALESCE(s.avg_daily, 0) AS avg_daily_7d
     FROM mtd m
     FULL OUTER JOIN last_month l
       ON l.activity_type = m.activity_type AND l.lead_type = m.lead_type AND l.source_key = m.source_key
     FULL OUTER JOIN last_7d s
       ON s.activity_type = COALESCE(m.activity_type, l.activity_type)
       AND s.lead_type = COALESCE(m.lead_type, l.lead_type)
       AND s.source_key = COALESCE(m.source_key, l.source_key)`,
    { year, month, lastYear, lastMonth }
  );

  // Get budgets for this month to compute planned amounts
  const budgets = await query<BudgetRow>(
    `SELECT * FROM ${table("budgets")} WHERE year = @year AND month = @month`,
    { year, month }
  );
  const budgetMap = new Map<string, number>();
  for (const b of budgets) {
    budgetMap.set(`${b.activity_type}|${b.lead_type}`, b.amount);
  }

  // Get allocations for these budgets
  const budgetIds = budgets.map((b) => b.budget_id);
  let allocations: AllocationRow[] = [];
  if (budgetIds.length > 0) {
    allocations = await query<AllocationRow>(
      `SELECT * FROM ${table("budget_allocations")} WHERE budget_id = ANY(@budgetIds)`,
      { budgetIds }
    );
  }
  // Map: budget_id -> { account_name -> pct }
  const allocMap = new Map<string, Map<string, number>>();
  for (const a of allocations) {
    if (!allocMap.has(a.budget_id)) allocMap.set(a.budget_id, new Map());
    allocMap.get(a.budget_id)!.set(a.account_name, a.allocation_pct);
  }
  // budget_id lookup by activity+lead
  const budgetIdMap = new Map<string, string>();
  for (const b of budgets) {
    budgetIdMap.set(`${b.activity_type}|${b.lead_type}`, b.budget_id);
  }

  // Build per-account forecast rows
  const byAccountRows: ForecastRow[] = rawRows.map((r) => {
    const predicted = r.mtd_spend + r.avg_daily_7d * remainingDays;
    const key = `${r.activity_type}|${r.lead_type}`;
    const totalBudget = budgetMap.get(key) || 0;
    const bId = budgetIdMap.get(key);
    const pct = bId ? allocMap.get(bId)?.get(r.source_key) : undefined;
    const allocatedBudget = pct != null ? totalBudget * pct / 100 : null;
    return {
      activity_type: r.activity_type,
      lead_type: r.lead_type,
      account_name: r.source_key,
      last_month_spend: r.last_month_spend,
      mtd_spend: r.mtd_spend,
      predicted_month_end: predicted,
      planned_budget: totalBudget,
      allocated_budget: allocatedBudget,
      predicted_vs_budget: allocatedBudget != null && allocatedBudget > 0
        ? (predicted / allocatedBudget - 1) * 100
        : null,
    };
  });

  // Aggregate by activity+lead
  const alGroupMap = new Map<string, ForecastRow>();
  for (const r of byAccountRows) {
    const key = `${r.activity_type}|${r.lead_type}`;
    const existing = alGroupMap.get(key);
    if (existing) {
      existing.last_month_spend += r.last_month_spend;
      existing.mtd_spend += r.mtd_spend;
      existing.predicted_month_end += r.predicted_month_end;
    } else {
      alGroupMap.set(key, {
        activity_type: r.activity_type,
        lead_type: r.lead_type,
        account_name: null,
        last_month_spend: r.last_month_spend,
        mtd_spend: r.mtd_spend,
        predicted_month_end: r.predicted_month_end,
        planned_budget: budgetMap.get(key) || 0,
        allocated_budget: null,
        predicted_vs_budget: null,
      });
    }
  }
  const byActivityLead = Array.from(alGroupMap.values()).map((r) => ({
    ...r,
    predicted_vs_budget:
      r.planned_budget > 0 ? (r.predicted_month_end / r.planned_budget - 1) * 100 : null,
  }));

  // Grand total
  const totalPlanned = byActivityLead.reduce((s, r) => s + r.planned_budget, 0);
  const totalRow: ForecastRow = {
    activity_type: "TOTAL",
    lead_type: "TOTAL",
    account_name: null,
    last_month_spend: byActivityLead.reduce((s, r) => s + r.last_month_spend, 0),
    mtd_spend: byActivityLead.reduce((s, r) => s + r.mtd_spend, 0),
    predicted_month_end: byActivityLead.reduce((s, r) => s + r.predicted_month_end, 0),
    planned_budget: totalPlanned,
    allocated_budget: null,
    predicted_vs_budget: null,
  };
  totalRow.predicted_vs_budget =
    totalPlanned > 0 ? (totalRow.predicted_month_end / totalPlanned - 1) * 100 : null;

  return { total: totalRow, byActivityLead, byAccount: byAccountRows };
}
