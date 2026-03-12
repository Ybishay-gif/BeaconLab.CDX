/**
 * Seed remaining tables (targets + analytics) that failed in the first run.
 */
import { query as bqQuery, table as bqTable } from "../src/db/bigquery.js";
import { pgExec, pgClose } from "../src/db/postgres.js";

type Row = Record<string, unknown>;

function pgLiteral(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "object" && value !== null && "value" in value) {
    return pgLiteral((value as { value: unknown }).value);
  }
  const str = String(value).replace(/'/g, "''");
  return `'${str}'`;
}

async function seedTable(tableName: string, cols: string[], bqCols?: string) {
  console.log(`\n--- Seeding ${tableName} ---`);
  const selectCols = bqCols || cols.join(", ");
  const rows = await bqQuery<Row>(`SELECT ${selectCols} FROM ${bqTable(tableName)}`);
  console.log(`  BQ rows: ${rows.length}`);
  if (rows.length === 0) return;

  await pgExec(`DELETE FROM ${tableName}`);
  const BATCH = 200;
  for (let offset = 0; offset < rows.length; offset += BATCH) {
    const batch = rows.slice(offset, offset + BATCH);
    const valueSets = batch.map((row) => {
      const vals = cols.map((col) => pgLiteral(row[col]));
      return `(${vals.join(", ")})`;
    });
    await pgExec(`INSERT INTO ${tableName} (${cols.join(", ")}) VALUES\n${valueSets.join(",\n")}`);
    process.stdout.write(`  Inserted ${Math.min(offset + BATCH, rows.length)}/${rows.length}\r`);
  }
  console.log(`  Done: ${rows.length} rows inserted.`);
}

async function main() {
  try {
    // targets — BQ doesn't have plan_id column
    await seedTable("targets", [
      "target_id", "state", "segment", "source", "target_value",
      "created_by", "created_at", "updated_by", "updated_at",
    ]);

    // Analytics tables
    await seedTable("state_segment_daily", [
      "event_date", "state", "segment", "channel_group_name", "activity_type",
      "lead_type", "bids", "sold", "total_cost", "quote_started", "quotes",
      "binds", "scored_policies", "target_cpb_sum", "lifetime_premium_sum",
      "lifetime_cost_sum", "avg_profit_sum", "avg_equity_sum", "avg_mrltv_sum",
    ]);

    await seedTable("targets_perf_daily", [
      "event_date", "state", "segment", "source_key", "company_account_id",
      "activity_type", "lead_type", "sold", "binds", "scored_policies",
      "price_sum", "target_cpb_sum", "lifetime_premium_sum", "lifetime_cost_sum",
      "avg_profit_sum", "avg_equity_sum",
    ]);

    console.log("\n=== Seed complete ===");
  } catch (err) {
    console.error("Seed failed:", err);
    process.exit(1);
  } finally {
    await pgClose();
  }
}

main();
