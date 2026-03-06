import { BigQuery } from "@google-cloud/bigquery";
import { config } from "../config.js";
export const bigquery = new BigQuery({ projectId: config.projectId });
export async function query(sql, params = {}) {
    const types = {};
    for (const [key, value] of Object.entries(params)) {
        if (value === null) {
            types[key] = "STRING";
            continue;
        }
        if (Array.isArray(value) && value.length === 0) {
            types[key] = ["STRING"];
        }
    }
    const queryOptions = {
        query: sql,
        params,
        useLegacySql: false
    };
    if (Object.keys(types).length > 0) {
        queryOptions.types = types;
    }
    const [rows] = await bigquery.query(queryOptions);
    return rows;
}
export function table(tableName) {
    return `\`${config.projectId}.${config.dataset}.${tableName}\``;
}
export function analyticsTable(tableName) {
    return `\`${config.projectId}.${config.analyticsDataset}.${tableName}\``;
}
export function analyticsRoutine(routineName) {
    return `\`${config.projectId}.${config.analyticsDataset}.${routineName}\``;
}
