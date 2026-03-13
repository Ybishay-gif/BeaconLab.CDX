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
    // BQ client wraps DATE, TIMESTAMP, ARRAY subquery values in {value: x} objects.
    // Unwrap them so consumers get plain JS primitives.
    return rows.map(unwrapRow);
}
/** Recursively unwrap BigQuery {value} wrappers (BigQueryDate, BigQueryTimestamp, etc.) */
function unwrapValue(v) {
    if (v === null || v === undefined)
        return v;
    if (Array.isArray(v))
        return v.map(unwrapValue);
    if (typeof v === "object") {
        const obj = v;
        // BigQueryDate / BigQueryTimestamp / ARRAY element: single `value` key
        if ("value" in obj && Object.keys(obj).length === 1)
            return obj.value;
        // Nested struct — unwrap each field
        const out = {};
        for (const [k, val] of Object.entries(obj))
            out[k] = unwrapValue(val);
        return out;
    }
    return v;
}
function unwrapRow(row) {
    const out = {};
    for (const [k, v] of Object.entries(row))
        out[k] = unwrapValue(v);
    return out;
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
