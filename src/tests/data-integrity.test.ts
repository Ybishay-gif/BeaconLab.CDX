/**
 * Data Integrity Test — January 2026 Snapshot
 *
 * Compares live API responses against a verified snapshot.
 * January data is historical and should never change.
 * Run: npx tsx src/tests/data-integrity.test.ts
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const API_BASE =
  process.env.API_BASE || "https://planning-app-api-758008223769.us-central1.run.app";
const ADMIN_CODE = process.env.ADMIN_CODE;
if (!ADMIN_CODE) {
  console.error("❌ ADMIN_CODE env var is required. Set it before running tests.");
  process.exit(1);
}

/* tolerance: 0.5% for floats, exact for integers */
const FLOAT_TOLERANCE = 0.005;
const FLOAT_KEYS = new Set([
  "cpb", "performance", "roe", "combined_ratio", "mrltv", "total_cost",
]);
const INT_KEYS = new Set(["bids", "sold", "binds", "scored_policies"]);

type Snapshot = {
  params: { startDate: string; endDate: string; activityLeadType: string; qbc: number };
  state_level: Record<string, Record<string, number>>;
  state_segment_level: Record<string, Record<string, number>>;
};

async function getToken(): Promise<string> {
  const res = await fetch(`${API_BASE}/api/auth/admin-login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code: ADMIN_CODE }),
  });
  if (!res.ok) throw new Error(`Auth failed: ${res.status}`);
  const data = (await res.json()) as { token: string };
  return data.token;
}

async function fetchSSP(
  token: string,
  groupBy: string,
  states: string[]
): Promise<Record<string, Record<string, number>>> {
  const snapshot: Snapshot = JSON.parse(
    readFileSync(join(__dirname, "snapshot-jan-2026.json"), "utf-8")
  );
  const { startDate, endDate, activityLeadType, qbc } = snapshot.params;
  const url =
    `${API_BASE}/api/analytics/state-segment-performance` +
    `?startDate=${startDate}&endDate=${endDate}` +
    `&activityLeadType=${activityLeadType}&qbc=${qbc}` +
    `&groupBy=${groupBy}&states=${states.join(",")}`;

  const res = await fetch(url, { headers: { "x-session-token": token } });
  if (!res.ok) throw new Error(`SSP ${groupBy} failed: ${res.status} ${await res.text()}`);
  const data = (await res.json()) as { rows: Record<string, unknown>[] };

  const map: Record<string, Record<string, number>> = {};
  for (const row of data.rows) {
    const state = String(row.state);
    const segment = String(row.segment);
    const key = groupBy === "state" ? state : `${state}|${segment}`;
    map[key] = {
      bids: Number(row.bids) || 0,
      sold: Number(row.sold) || 0,
      total_cost: Math.round((Number(row.total_cost) || 0) * 100) / 100,
      binds: Number(row.binds) || 0,
      scored_policies: Number(row.scored_policies) || 0,
      cpb: Math.round((Number(row.cpb) || 0) * 100) / 100,
      performance: Math.round((Number(row.performance) || 0) * 10000) / 10000,
      roe: Math.round((Number(row.roe) || 0) * 10000) / 10000,
      combined_ratio: Math.round((Number(row.combined_ratio) || 0) * 10000) / 10000,
      mrltv: Math.round((Number(row.mrltv) || 0) * 100) / 100,
    };
  }
  return map;
}

type Failure = {
  key: string;
  metric: string;
  expected: number;
  actual: number;
  diff: string;
};

function compare(
  label: string,
  expected: Record<string, Record<string, number>>,
  actual: Record<string, Record<string, number>>
): Failure[] {
  const failures: Failure[] = [];

  for (const [key, expectedRow] of Object.entries(expected)) {
    const actualRow = actual[key];
    if (!actualRow) {
      failures.push({ key, metric: "(row)", expected: 0, actual: 0, diff: "MISSING" });
      continue;
    }

    for (const [metric, expectedVal] of Object.entries(expectedRow)) {
      const actualVal = actualRow[metric];
      if (actualVal === undefined) {
        failures.push({ key, metric, expected: expectedVal, actual: 0, diff: "MISSING" });
        continue;
      }

      if (INT_KEYS.has(metric)) {
        if (expectedVal !== actualVal) {
          failures.push({
            key,
            metric,
            expected: expectedVal,
            actual: actualVal,
            diff: `${actualVal - expectedVal}`,
          });
        }
      } else if (FLOAT_KEYS.has(metric)) {
        const pctDiff =
          expectedVal !== 0 ? Math.abs(actualVal - expectedVal) / Math.abs(expectedVal) : actualVal !== 0 ? 1 : 0;
        if (pctDiff > FLOAT_TOLERANCE) {
          failures.push({
            key,
            metric,
            expected: expectedVal,
            actual: actualVal,
            diff: `${(pctDiff * 100).toFixed(2)}%`,
          });
        }
      }
    }
  }

  return failures;
}

async function run() {
  const snapshot: Snapshot = JSON.parse(
    readFileSync(join(__dirname, "snapshot-jan-2026.json"), "utf-8")
  );

  console.log("🔐 Authenticating...");
  const token = await getToken();

  console.log("📊 Fetching state-level data (AL, AZ, CO, CT, FL)...");
  const stateActual = await fetchSSP(token, "state", ["AL", "AZ", "CO", "CT", "FL"]);
  const stateFailures = compare("state", snapshot.state_level, stateActual);

  console.log("📊 Fetching state+segment data (AL, AZ)...");
  const segActual = await fetchSSP(token, "state_segment", ["AL", "AZ"]);
  const segFailures = compare("state_segment", snapshot.state_segment_level, segActual);

  const allFailures = [...stateFailures, ...segFailures];

  console.log("\n" + "=".repeat(70));
  if (allFailures.length === 0) {
    console.log("✅ ALL CHECKS PASSED — January 2026 data matches snapshot");
    console.log(`   Verified: ${Object.keys(snapshot.state_level).length} states, ${Object.keys(snapshot.state_segment_level).length} state+segment rows`);
  } else {
    console.log(`❌ ${allFailures.length} DATA DISCREPANCIES FOUND:\n`);
    for (const f of allFailures) {
      console.log(`   ${f.key} → ${f.metric}: expected=${f.expected}, actual=${f.actual} (${f.diff})`);
    }
    console.log("\n⚠️  January data should never change. Investigate immediately.");
    process.exit(1);
  }
  console.log("=".repeat(70));
}

run().catch((err) => {
  console.error("❌ Test failed with error:", err.message);
  process.exit(1);
});
