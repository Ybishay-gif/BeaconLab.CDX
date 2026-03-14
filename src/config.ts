import dotenv from "dotenv";

// In Cloud Run we rely on service env vars, not local .env files baked into the image.
if (!process.env.K_SERVICE) {
  dotenv.config();
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

export const config = {
  port: Number(process.env.PORT ?? 8080),
  projectId: required("GOOGLE_CLOUD_PROJECT"),
  dataset: required("BQ_DATASET"),
  analyticsDataset: process.env.BQ_ANALYTICS_DATASET || required("BQ_DATASET"),
  adminAccessCode: required("ADMIN_ACCESS_CODE"),
  rawCrossTacticTable:
    process.env.BQ_RAW_CROSS_TACTIC_TABLE || "`crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `",
  rawCrossTacticWithOppsTable:
    process.env.BQ_RAW_CROSS_TACTIC_OPPS_TABLE || "`crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data - with opps`",

  // Reports
  reportsBucket: process.env.REPORTS_GCS_BUCKET || "beacon-lab-reports",

  // PostgreSQL — runtime DB (BQ kept only for daily sync)
  usePg: process.env.USE_PG === "true",
  cloudSqlConnectionName: process.env.CLOUD_SQL_CONNECTION_NAME || "",
};
