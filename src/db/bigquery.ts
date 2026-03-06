import { BigQuery } from "@google-cloud/bigquery";
import { config } from "../config.js";

export const bigquery = new BigQuery({ projectId: config.projectId });

export async function query<T>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> {
  const types: Record<string, string | string[]> = {};
  for (const [key, value] of Object.entries(params)) {
    if (value === null) {
      types[key] = "STRING";
      continue;
    }
    if (Array.isArray(value) && value.length === 0) {
      types[key] = ["STRING"];
    }
  }

  const queryOptions: {
    query: string;
    params: Record<string, unknown>;
    types?: Record<string, string | string[]>;
    useLegacySql: boolean;
  } = {
    query: sql,
    params,
    useLegacySql: false
  };

  if (Object.keys(types).length > 0) {
    queryOptions.types = types;
  }

  const [rows] = await bigquery.query(queryOptions);

  return rows as T[];
}

export function table(tableName: string): string {
  return `\`${config.projectId}.${config.dataset}.${tableName}\``;
}

export function analyticsTable(tableName: string): string {
  return `\`${config.projectId}.${config.analyticsDataset}.${tableName}\``;
}

export function analyticsRoutine(routineName: string): string {
  return `\`${config.projectId}.${config.analyticsDataset}.${routineName}\``;
}
