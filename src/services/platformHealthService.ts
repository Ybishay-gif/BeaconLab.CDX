import { pgExec } from "../db/postgres.js";

// ── Types ────────────────────────────────────────────────────────────

export interface SyncHistoryRow {
  sync_id: string;
  started_at: string;
  completed_at: string | null;
  ok: boolean;
  total_ms: number | null;
  total_rows: number;
  error: string | null;
  tables_json: Array<{ table: string; rows: number; ms: number; error?: string }>;
  created_at: string;
}

export interface SecurityTestRow {
  result_id: string;
  test_type: "auth-security" | "pentest";
  ran_at: string;
  environment: string;
  target_url: string | null;
  passed: number;
  failed: number;
  critical_fails: number;
  high_fails: number;
  medium_fails: number;
  low_fails: number;
  findings_json: unknown[];
  passed_checks: string[];
  status: "no_errors" | "critical_errors" | "minor_errors";
  created_at: string;
}

// ── Sync History ─────────────────────────────────────────────────────

export async function listSyncHistory(
  limit = 50,
  offset = 0,
): Promise<{ rows: SyncHistoryRow[]; total: number }> {
  const countRes = await pgExec("SELECT COUNT(*)::int AS total FROM sync_history");
  const total = (countRes.rows[0] as { total: number }).total;

  const dataRes = await pgExec(
    `SELECT * FROM sync_history ORDER BY started_at DESC LIMIT $1 OFFSET $2`,
    [limit, offset],
  );

  return { rows: dataRes.rows as unknown as SyncHistoryRow[], total };
}

// ── Security Tests ───────────────────────────────────────────────────

export async function listSecurityTests(
  limit = 50,
  offset = 0,
): Promise<{ rows: SecurityTestRow[]; total: number }> {
  const countRes = await pgExec("SELECT COUNT(*)::int AS total FROM security_test_results");
  const total = (countRes.rows[0] as { total: number }).total;

  const dataRes = await pgExec(
    `SELECT * FROM security_test_results ORDER BY ran_at DESC LIMIT $1 OFFSET $2`,
    [limit, offset],
  );

  return { rows: dataRes.rows as unknown as SecurityTestRow[], total };
}

export async function getSecurityTestById(resultId: string): Promise<SecurityTestRow | null> {
  const res = await pgExec(
    `SELECT * FROM security_test_results WHERE result_id = $1`,
    [resultId],
  );
  return (res.rows[0] as unknown as SecurityTestRow) ?? null;
}

/** Compute status from summary counts and insert a security test result row. */
export async function insertSecurityTestResult(data: {
  suite: string;
  timestamp: string;
  target: string;
  environment: string;
  summary: {
    passed: number;
    failed: number;
    criticalFails: number;
    highFails: number;
    mediumFails: number;
    lowFails: number;
  };
  findings: unknown[];
  passedChecks: string[];
}): Promise<string> {
  const { summary } = data;

  let status: "no_errors" | "critical_errors" | "minor_errors" = "no_errors";
  if (summary.criticalFails > 0 || summary.highFails > 0) {
    status = "critical_errors";
  } else if (summary.mediumFails > 0 || summary.lowFails > 0) {
    status = "minor_errors";
  }

  const testType = data.suite === "pentest" ? "pentest" : "auth-security";

  const res = await pgExec(
    `INSERT INTO security_test_results
       (test_type, ran_at, environment, target_url, passed, failed,
        critical_fails, high_fails, medium_fails, low_fails,
        findings_json, passed_checks, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
     RETURNING result_id`,
    [
      testType,
      data.timestamp,
      data.environment,
      data.target,
      summary.passed,
      summary.failed,
      summary.criticalFails,
      summary.highFails,
      summary.mediumFails,
      summary.lowFails,
      JSON.stringify(data.findings),
      JSON.stringify(data.passedChecks),
      status,
    ],
  );

  return (res.rows[0] as { result_id: string }).result_id;
}
