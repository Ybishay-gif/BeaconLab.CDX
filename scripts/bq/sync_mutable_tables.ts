import dotenv from "dotenv";
import { BigQuery } from "@google-cloud/bigquery";

if (!process.env.K_SERVICE) {
  dotenv.config();
}

const projectId = process.env.GOOGLE_CLOUD_PROJECT;
const sourceDataset = process.env.BQ_SYNC_SOURCE_DATASET;
const targetDataset = process.env.BQ_SYNC_TARGET_DATASET;
const dryRun = String(process.env.BQ_SYNC_DRY_RUN || "").toLowerCase() === "true";

if (!projectId) {
  throw new Error("Missing GOOGLE_CLOUD_PROJECT");
}
if (!sourceDataset) {
  throw new Error("Missing BQ_SYNC_SOURCE_DATASET");
}
if (!targetDataset) {
  throw new Error("Missing BQ_SYNC_TARGET_DATASET");
}

const MUTABLE_TABLES = [
  "users",
  "user_credentials",
  "auth_sessions",
  "plans",
  "plan_parameters",
  "plan_decisions",
  "plan_runs",
  "plan_results",
  "targets",
  "change_log"
];

function qid(dataset: string, table?: string): string {
  if (!table) {
    return `\`${projectId}.${dataset}\``;
  }
  return `\`${projectId}.${dataset}.${table}\``;
}

async function run(): Promise<void> {
  const bq = new BigQuery({ projectId });

  const statements = [
    `CREATE SCHEMA IF NOT EXISTS ${qid(targetDataset)}`
  ];
  for (const tableName of MUTABLE_TABLES) {
    statements.push(
      `CREATE OR REPLACE TABLE ${qid(targetDataset, tableName)} CLONE ${qid(sourceDataset, tableName)}`
    );
  }

  if (dryRun) {
    console.log("BQ_SYNC_DRY_RUN=true, planned statements:");
    for (const stmt of statements) {
      console.log(`${stmt};`);
    }
    return;
  }

  for (const stmt of statements) {
    console.log(`Executing: ${stmt}`);
    await bq.query({ query: stmt, useLegacySql: false });
  }

  console.log(
    `Done. Cloned mutable app tables from ${projectId}.${sourceDataset} to ${projectId}.${targetDataset}.`
  );
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
