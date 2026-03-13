import { randomUUID } from "node:crypto";
import { Storage } from "@google-cloud/storage";
import { query, table } from "../db/index.js";
import { bigquery, query as bqQuery } from "../db/bigquery.js";
import { config } from "../config.js";
import { cacheGet, cacheSet } from "../cache.js";

const storage = new Storage({ projectId: config.projectId });

// ── Types ──────────────────────────────────────────────────────────

export type ReportStatus = "pending" | "processing" | "done" | "error";

export type ReportRow = {
  report_id: string;
  report_name: string;
  user_id: string;
  date_start: string;
  date_end: string;
  fixed_filters: string; // JSONB stringified
  dynamic_filters: string;
  selected_columns: string;
  status: ReportStatus;
  file_url: string | null;
  row_count: number | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
};

export type ColumnSchema = {
  column_name: string;
  data_type: string;
};

export type DynamicFilter = {
  column: string;
  operator: string;
  value: string | number | (string | number)[];
};

export type FixedFilters = {
  account_name?: string[];
  campaign_name?: string[];
  attribution_channel?: string[];
  data_state?: string[];
  transaction_sold?: "0" | "1" | "all";
};

export type CreateReportInput = {
  reportName: string;
  dateStart: string;
  dateEnd: string;
  fixedFilters: FixedFilters;
  dynamicFilters: DynamicFilter[];
  selectedColumns: string[];
};

// ── Schema Discovery (cached 24h) ─────────────────────────────────

const SCHEMA_CACHE_KEY = "report:table-schema";
const SCHEMA_TTL = 24 * 60 * 60 * 1000;

export async function getTableSchema(): Promise<ColumnSchema[]> {
  const cached = cacheGet<ColumnSchema[]>(SCHEMA_CACHE_KEY);
  if (cached) return cached;

  // Query INFORMATION_SCHEMA for the Cross Tactic table
  // The table is in Custom_Reports dataset, not planning_app
  const rows = await bqQuery<ColumnSchema>(
    `SELECT column_name, data_type
     FROM \`crblx-beacon-prod.Custom_Reports.INFORMATION_SCHEMA.COLUMNS\`
     WHERE table_name = 'Cross Tactic Analysis Full Data'
     ORDER BY ordinal_position`
  );

  cacheSet(SCHEMA_CACHE_KEY, rows, SCHEMA_TTL);
  return rows;
}

/** Validate column names against the schema to prevent SQL injection */
async function validateColumns(columns: string[]): Promise<void> {
  const schema = await getTableSchema();
  const validNames = new Set(schema.map((c) => c.column_name));
  for (const col of columns) {
    if (!validNames.has(col)) {
      throw new Error(`Invalid column name: ${col}`);
    }
  }
}

// ── Filter Values (for fixed filter dropdowns) ─────────────────────

export async function getFilterValues(columnName: string): Promise<string[]> {
  // Validate column name against schema
  await validateColumns([columnName]);

  const cacheKey = `report:filter-values:${columnName}`;
  const cached = cacheGet<string[]>(cacheKey);
  if (cached) return cached;

  const rows = await bqQuery<{ val: string }>(
    `SELECT DISTINCT \`${columnName}\` AS val
     FROM ${config.rawCrossTacticTable}
     WHERE \`${columnName}\` IS NOT NULL
     ORDER BY val
     LIMIT 1000`
  );

  const values = rows.map((r) => String(r.val));
  cacheSet(cacheKey, values, SCHEMA_TTL);
  return values;
}

// ── CRUD ───────────────────────────────────────────────────────────

export async function listReports(userId: string): Promise<ReportRow[]> {
  return query<ReportRow>(
    `SELECT report_id, report_name, status, row_count,
            fixed_filters::text AS fixed_filters,
            dynamic_filters::text AS dynamic_filters,
            selected_columns::text AS selected_columns,
            date_start::text AS date_start,
            date_end::text AS date_end,
            error_message, user_id,
            created_at::text AS created_at,
            updated_at::text AS updated_at,
            completed_at::text AS completed_at
     FROM ${table("reports")}
     WHERE user_id = @userId
     ORDER BY created_at DESC
     LIMIT 100`,
    { userId }
  );
}

export async function getReport(reportId: string): Promise<ReportRow | null> {
  const rows = await query<ReportRow>(
    `SELECT report_id, report_name, status, row_count, file_url,
            fixed_filters::text AS fixed_filters,
            dynamic_filters::text AS dynamic_filters,
            selected_columns::text AS selected_columns,
            date_start::text AS date_start,
            date_end::text AS date_end,
            error_message, user_id,
            created_at::text AS created_at,
            updated_at::text AS updated_at,
            completed_at::text AS completed_at
     FROM ${table("reports")}
     WHERE report_id = @reportId`,
    { reportId }
  );
  return rows[0] ?? null;
}

export async function createReport(
  userId: string,
  input: CreateReportInput
): Promise<{ reportId: string }> {
  // Validate all column names before saving
  await validateColumns(input.selectedColumns);
  for (const f of input.dynamicFilters) {
    await validateColumns([f.column]);
  }

  const reportId = randomUUID();
  await query(
    `INSERT INTO ${table("reports")} (
       report_id, report_name, user_id, date_start, date_end,
       fixed_filters, dynamic_filters, selected_columns,
       status, created_at, updated_at
     ) VALUES (
       @reportId, @reportName, @userId, @dateStart, @dateEnd,
       @fixedFilters, @dynamicFilters, @selectedColumns,
       'pending', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
     )`,
    {
      reportId,
      reportName: input.reportName,
      userId,
      dateStart: input.dateStart,
      dateEnd: input.dateEnd,
      fixedFilters: JSON.stringify(input.fixedFilters),
      dynamicFilters: JSON.stringify(input.dynamicFilters),
      selectedColumns: JSON.stringify(input.selectedColumns),
    }
  );

  // Fire and forget — generate report asynchronously
  generateReport(reportId).catch((err) =>
    console.error(`Report ${reportId} generation failed:`, err)
  );

  return { reportId };
}

export async function deleteReport(reportId: string): Promise<void> {
  const report = await getReport(reportId);
  if (report?.file_url) {
    try {
      await storage.bucket(config.reportsBucket).file(report.file_url).delete();
    } catch {
      // File may already be deleted
    }
  }
  await query(
    `DELETE FROM ${table("reports")} WHERE report_id = @reportId`,
    { reportId }
  );
}

// ── Download URL ───────────────────────────────────────────────────

export async function getDownloadUrl(reportId: string): Promise<string | null> {
  const report = await getReport(reportId);
  if (!report?.file_url || report.status !== "done") return null;

  const [url] = await storage
    .bucket(config.reportsBucket)
    .file(report.file_url)
    .getSignedUrl({
      version: "v4",
      action: "read",
      expires: Date.now() + 15 * 60 * 1000, // 15 minutes
    });
  return url;
}

// ── Async Report Generation ────────────────────────────────────────

const ALLOWED_OPERATORS = new Set(["=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN"]);

async function updateStatus(
  reportId: string,
  status: ReportStatus,
  extra: Record<string, unknown> = {}
): Promise<void> {
  const sets = ["status = @status", "updated_at = CURRENT_TIMESTAMP()"];
  const params: Record<string, unknown> = { reportId, status };

  if (extra.fileUrl !== undefined) {
    sets.push("file_url = @fileUrl");
    params.fileUrl = extra.fileUrl;
  }
  if (extra.rowCount !== undefined) {
    sets.push("row_count = @rowCount");
    params.rowCount = extra.rowCount;
  }
  if (extra.errorMessage !== undefined) {
    sets.push("error_message = @errorMessage");
    params.errorMessage = extra.errorMessage;
  }
  if (status === "done" || status === "error") {
    sets.push("completed_at = CURRENT_TIMESTAMP()");
  }

  await query(
    `UPDATE ${table("reports")} SET ${sets.join(", ")} WHERE report_id = @reportId`,
    params
  );
}

function buildSelectSql(
  columns: string[],
  fixedFilters: FixedFilters,
  dynamicFilters: DynamicFilter[],
  dateStart: string,
  dateEnd: string
): { sql: string; params: Record<string, unknown> } {
  // Columns are already validated
  const selectCols = columns.map((c) => `\`${c}\``).join(", ");
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  // Date filter — use Data_DateCreated which is the standard date column
  conditions.push("`Data_DateCreated` >= @dateStart");
  conditions.push("`Data_DateCreated` <= @dateEnd");
  params.dateStart = dateStart;
  params.dateEnd = dateEnd;

  // Fixed filters
  let paramIdx = 0;
  const addArrayFilter = (column: string, values: string[]) => {
    if (values.length === 0) return;
    const placeholders = values.map((v) => {
      const key = `fp${paramIdx++}`;
      params[key] = v;
      return `@${key}`;
    });
    conditions.push(`\`${column}\` IN (${placeholders.join(", ")})`);
  };

  if (fixedFilters.account_name?.length) addArrayFilter("Account_Name", fixedFilters.account_name);
  if (fixedFilters.campaign_name?.length) addArrayFilter("Campaign_Name", fixedFilters.campaign_name);
  if (fixedFilters.attribution_channel?.length) addArrayFilter("Attribution_Channel", fixedFilters.attribution_channel);
  if (fixedFilters.data_state?.length) addArrayFilter("Data_State", fixedFilters.data_state);
  if (fixedFilters.transaction_sold && fixedFilters.transaction_sold !== "all") {
    params.transactionSold = Number(fixedFilters.transaction_sold);
    conditions.push("`Transaction_Sold` = @transactionSold");
  }

  // Dynamic filters
  for (const f of dynamicFilters) {
    if (!ALLOWED_OPERATORS.has(f.operator)) continue;

    const colRef = `\`${f.column}\``;

    if (f.operator === "BETWEEN" && Array.isArray(f.value) && f.value.length === 2) {
      const k1 = `dp${paramIdx++}`;
      const k2 = `dp${paramIdx++}`;
      params[k1] = f.value[0];
      params[k2] = f.value[1];
      conditions.push(`${colRef} BETWEEN @${k1} AND @${k2}`);
    } else if (f.operator === "IN") {
      const vals = Array.isArray(f.value) ? f.value : String(f.value).split(",").map((s) => s.trim());
      const placeholders = vals.map((v) => {
        const key = `dp${paramIdx++}`;
        params[key] = v;
        return `@${key}`;
      });
      conditions.push(`${colRef} IN (${placeholders.join(", ")})`);
    } else if (f.operator === "LIKE") {
      const key = `dp${paramIdx++}`;
      params[key] = `%${f.value}%`;
      conditions.push(`${colRef} LIKE @${key}`);
    } else {
      const key = `dp${paramIdx++}`;
      params[key] = f.value;
      conditions.push(`${colRef} ${f.operator} @${key}`);
    }
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const sql = `SELECT ${selectCols} FROM ${config.rawCrossTacticTable} ${whereClause}`;

  return { sql, params };
}

export async function generateReport(reportId: string): Promise<void> {
  try {
    await updateStatus(reportId, "processing");

    const report = await getReport(reportId);
    if (!report) throw new Error("Report not found");

    const fixedFilters: FixedFilters = JSON.parse(report.fixed_filters);
    const dynamicFilters: DynamicFilter[] = JSON.parse(report.dynamic_filters);
    const selectedColumns: string[] = JSON.parse(report.selected_columns);

    // Re-validate columns
    await validateColumns(selectedColumns);
    for (const f of dynamicFilters) {
      await validateColumns([f.column]);
    }

    // Build the query
    const { sql, params } = buildSelectSql(
      selectedColumns,
      fixedFilters,
      dynamicFilters,
      report.date_start,
      report.date_end
    );

    // Create a destination temp table in BQ
    const sanitizedId = reportId.replace(/-/g, "_");
    const destTableId = `report_${sanitizedId}`;
    const destRef = bigquery.dataset(config.dataset).table(destTableId);

    // Run query into destination table
    const [job] = await bigquery.createQueryJob({
      query: sql,
      params,
      useLegacySql: false,
      destination: destRef,
      writeDisposition: "WRITE_TRUNCATE",
    });
    const [rows] = await job.getQueryResults({ maxResults: 0 });

    // Get row count from table metadata
    const [metadata] = await destRef.getMetadata();
    const rowCount = Number(metadata.numRows ?? rows.length ?? 0);

    // Export to GCS as CSV
    const gcsPath = `reports/${reportId}.csv`;
    const gcsFile = storage.bucket(config.reportsBucket).file(gcsPath);

    await destRef.extract(gcsFile, { format: "CSV" });

    // Clean up temp BQ table
    try {
      await destRef.delete();
    } catch {
      // Non-critical — table will expire anyway
    }

    await updateStatus(reportId, "done", { fileUrl: gcsPath, rowCount });
    console.log(`Report ${reportId} generated: ${rowCount} rows`);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`Report ${reportId} failed:`, message);
    await updateStatus(reportId, "error", { errorMessage: message }).catch(() => {});
  }
}
