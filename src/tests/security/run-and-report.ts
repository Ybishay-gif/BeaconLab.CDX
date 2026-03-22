#!/usr/bin/env npx tsx
/**
 * Wrapper: runs a security test suite and POSTs the structured JSON result
 * to the Platform Health ingestion endpoint.
 *
 * Usage:
 *   npx tsx src/tests/security/run-and-report.ts <test-file> <test-target-url> <api-base-url> <scheduler-secret>
 *
 * Example:
 *   npx tsx src/tests/security/run-and-report.ts \
 *     src/tests/security/auth-security.test.ts \
 *     https://planning-app-api-758008223769.us-central1.run.app \
 *     https://planning-app-api-758008223769.us-central1.run.app \
 *     my-scheduler-secret
 */

import { execSync } from "node:child_process";

const JSON_MARKER = "--- STRUCTURED RESULTS (JSON) ---";

async function main(): Promise<void> {
  const [testFile, testTarget, apiBase, schedulerSecret] = process.argv.slice(2);

  if (!testFile || !apiBase || !schedulerSecret) {
    console.error("Usage: run-and-report.ts <test-file> <test-target-url> <api-base-url> <scheduler-secret>");
    process.exit(1);
  }

  // Run the test and capture stdout (allow non-zero exit — test failures are expected)
  let stdout = "";
  try {
    stdout = execSync(`npx tsx ${testFile}${testTarget ? ` ${testTarget}` : ""}`, {
      encoding: "utf-8",
      timeout: 300_000, // 5 min max
      maxBuffer: 10 * 1024 * 1024,
    });
  } catch (err: unknown) {
    // execSync throws on non-zero exit. Capture stdout from the error object.
    const execErr = err as { stdout?: string; stderr?: string };
    stdout = execErr.stdout ?? "";
    if (!stdout) {
      console.error("Test produced no output. stderr:", execErr.stderr);
      process.exit(1);
    }
  }

  // Print the full test output for logging visibility
  console.log(stdout);

  // Extract JSON block
  const markerIdx = stdout.indexOf(JSON_MARKER);
  if (markerIdx === -1) {
    console.error("No structured results marker found in test output");
    process.exit(1);
  }

  const jsonStr = stdout.slice(markerIdx + JSON_MARKER.length).trim();
  let result: Record<string, unknown>;
  try {
    result = JSON.parse(jsonStr);
  } catch {
    console.error("Failed to parse structured JSON from test output");
    process.exit(1);
  }

  // POST to ingestion endpoint
  const url = `${apiBase.replace(/\/$/, "")}/api/admin/security-test-result`;
  console.log(`\nPOSTing result to ${url}...`);

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Scheduler-Secret": schedulerSecret,
    },
    body: JSON.stringify(result),
  });

  const body = await resp.text();
  if (resp.ok) {
    console.log(`Ingested successfully: ${body}`);
  } else {
    console.error(`Ingestion failed (${resp.status}): ${body}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("run-and-report failed:", err);
  process.exit(1);
});
