import XLSX from "xlsx";
import { query as bqQuery } from "../db/bigquery.js";
import { config } from "../config.js";
import { REPORT_COLUMNS, COLUMN_MAP, type ColumnMeta } from "../data/reportColumns.js";

// ── Types ──────────────────────────────────────────────────────────

export type FileColumnMeta = {
  name: string;
  example: string | null;
  dataType: "string" | "number" | "boolean" | "date" | "unknown";
  suggestedBqColumn: string | null;
};

export type ColumnMapping = {
  fileColumn: string;
  bqColumn: string;
  isMatchKey: boolean;
};

export type ComparisonRow = {
  rowIndex: number;
  matchStatus: "matched" | "unmatched" | "multiple";
  fileValues: Record<string, unknown>;
  bqValues: Record<string, unknown>;
  diffs: string[]; // list of bqColumn names that differ
};

export type CompareResult = {
  rows: ComparisonRow[];
  stats: {
    totalFileRows: number;
    matched: number;
    unmatched: number;
    multipleMatches: number;
    diffCount: number;
  };
};

// ── Constants ──────────────────────────────────────────────────────

const MAX_ROWS = 10_000;

// ── File Parsing ───────────────────────────────────────────────────

export function parseUploadedFile(base64: string, mimeType: string) {
  const buffer = Buffer.from(base64, "base64");
  const wb = XLSX.read(buffer, { type: "buffer", dateNF: "yyyy-mm-dd" });

  const sheetName = wb.SheetNames[0];
  if (!sheetName) throw new Error("File contains no sheets");

  const rows: Record<string, unknown>[] = XLSX.utils.sheet_to_json(wb.Sheets[sheetName]!, {
    defval: null,
    raw: false,
  });

  if (rows.length === 0) throw new Error("File contains no data rows");
  if (rows.length > MAX_ROWS) {
    throw new Error(`File has ${rows.length} rows — maximum is ${MAX_ROWS}. Please reduce the file and try again.`);
  }

  // Extract column metadata from first rows
  const columnNames = Object.keys(rows[0]!);
  const columns: FileColumnMeta[] = columnNames.map((name) => {
    const example = findFirstNonEmpty(rows, name);
    const dataType = inferDataType(rows, name);
    const suggestedBqColumn = suggestMapping(name);
    return { name, example, dataType, suggestedBqColumn };
  });

  return { columns, rowCount: rows.length, rows };
}

// ── Auto-Mapping ───────────────────────────────────────────────────

function suggestMapping(fileColumnName: string): string | null {
  const normalized = fileColumnName.toLowerCase().replace(/[_\s-]+/g, "");

  // Exact match on display_name or column_name
  for (const col of REPORT_COLUMNS) {
    const dispNorm = col.display_name.toLowerCase().replace(/[_\s-]+/g, "");
    const colNorm = col.column_name.toLowerCase().replace(/[_\s-]+/g, "");
    if (normalized === dispNorm || normalized === colNorm) {
      return col.column_name;
    }
  }

  // Contains match (file column name is contained in display_name or vice versa)
  for (const col of REPORT_COLUMNS) {
    const dispNorm = col.display_name.toLowerCase().replace(/[_\s-]+/g, "");
    const colNorm = col.column_name.toLowerCase().replace(/[_\s-]+/g, "");
    if (
      (normalized.length >= 3 && dispNorm.includes(normalized)) ||
      (normalized.length >= 3 && colNorm.includes(normalized)) ||
      (dispNorm.length >= 3 && normalized.includes(dispNorm))
    ) {
      return col.column_name;
    }
  }

  return null;
}

// ── BQ Query ───────────────────────────────────────────────────────

export async function fetchMatchingBqRows(
  mappings: ColumnMapping[],
  fileRows: Record<string, unknown>[]
): Promise<Record<string, unknown>[]> {
  const matchKeys = mappings.filter((m) => m.isMatchKey);
  const allMapped = mappings.filter((m) => m.bqColumn);

  if (matchKeys.length === 0) throw new Error("At least one match key is required");

  // Validate all BQ columns exist
  for (const m of allMapped) {
    if (!COLUMN_MAP.has(m.bqColumn)) {
      throw new Error(`Unknown BQ column: ${m.bqColumn}`);
    }
  }

  // Collect unique values for each match key
  const matchParams: Record<string, unknown[]> = {};
  for (const mk of matchKeys) {
    const values = new Set<string>();
    for (const row of fileRows) {
      const val = row[mk.fileColumn];
      if (val != null && String(val).trim() !== "") {
        values.add(String(val).trim());
      }
    }
    matchParams[mk.bqColumn] = Array.from(values);
  }

  // Build SELECT list (deduplicated BQ columns)
  const selectCols = [...new Set(allMapped.map((m) => m.bqColumn))];
  const selectClause = selectCols.map((c) => `\`${c}\``).join(", ");

  // Build WHERE clause
  const whereParts: string[] = [];
  const params: Record<string, unknown> = {};
  let paramIdx = 0;
  for (const mk of matchKeys) {
    const paramName = `match_${paramIdx++}`;
    whereParts.push(`CAST(\`${mk.bqColumn}\` AS STRING) IN UNNEST(@${paramName})`);
    params[paramName] = matchParams[mk.bqColumn];
  }

  const sql = `
    SELECT ${selectClause}
    FROM ${config.rawCrossTacticTable}
    WHERE ${whereParts.join("\n      AND ")}
  `;

  return bqQuery<Record<string, unknown>>(sql, params);
}

// ── Comparison ─────────────────────────────────────────────────────

export function compareRows(
  fileRows: Record<string, unknown>[],
  bqRows: Record<string, unknown>[],
  mappings: ColumnMapping[]
): CompareResult {
  const matchKeys = mappings.filter((m) => m.isMatchKey);
  const compareCols = mappings.filter((m) => m.bqColumn && !m.isMatchKey);

  // Index BQ rows by composite match key
  const bqIndex = new Map<string, Record<string, unknown>[]>();
  for (const bqRow of bqRows) {
    const key = buildCompositeKey(bqRow, matchKeys.map((m) => m.bqColumn));
    const existing = bqIndex.get(key) || [];
    existing.push(bqRow);
    bqIndex.set(key, existing);
  }

  const rows: ComparisonRow[] = [];
  let matched = 0;
  let unmatched = 0;
  let multipleMatches = 0;
  let diffCount = 0;

  for (let i = 0; i < fileRows.length; i++) {
    const fileRow = fileRows[i]!;
    const fileKey = buildCompositeKey(
      fileRow,
      matchKeys.map((m) => m.fileColumn)
    );

    const matchedBqRows = bqIndex.get(fileKey);

    if (!matchedBqRows || matchedBqRows.length === 0) {
      // Unmatched
      unmatched++;
      rows.push({
        rowIndex: i,
        matchStatus: "unmatched",
        fileValues: extractValues(fileRow, mappings, "file"),
        bqValues: {},
        diffs: [],
      });
      continue;
    }

    const isMultiple = matchedBqRows.length > 1;
    if (isMultiple) multipleMatches++;
    else matched++;

    // Compare against first BQ match (for multiple, we show first)
    const bqRow = matchedBqRows[0]!;
    const diffs: string[] = [];

    for (const cm of compareCols) {
      const fileVal = fileRow[cm.fileColumn];
      const bqVal = bqRow[cm.bqColumn];
      if (!valuesEqual(fileVal, bqVal)) {
        diffs.push(cm.bqColumn);
      }
    }

    // Also include match key comparisons
    for (const mk of matchKeys) {
      const fileVal = fileRow[mk.fileColumn];
      const bqVal = bqRow[mk.bqColumn];
      if (!valuesEqual(fileVal, bqVal)) {
        diffs.push(mk.bqColumn);
      }
    }

    if (diffs.length > 0) diffCount++;

    rows.push({
      rowIndex: i,
      matchStatus: isMultiple ? "multiple" : "matched",
      fileValues: extractValues(fileRow, mappings, "file"),
      bqValues: extractValues(bqRow, mappings, "bq"),
      diffs,
    });
  }

  return {
    rows,
    stats: {
      totalFileRows: fileRows.length,
      matched,
      unmatched,
      multipleMatches,
      diffCount,
    },
  };
}

// ── Helpers ────────────────────────────────────────────────────────

function findFirstNonEmpty(rows: Record<string, unknown>[], col: string): string | null {
  for (const row of rows.slice(0, 20)) {
    const val = row[col];
    if (val != null && String(val).trim() !== "") {
      return String(val);
    }
  }
  return null;
}

function inferDataType(rows: Record<string, unknown>[], col: string): FileColumnMeta["dataType"] {
  const samples = rows.slice(0, 50).map((r) => r[col]).filter((v) => v != null && String(v).trim() !== "");
  if (samples.length === 0) return "unknown";

  const allNumbers = samples.every((v) => !isNaN(Number(v)));
  if (allNumbers) return "number";

  const datePattern = /^\d{4}[-/]\d{1,2}[-/]\d{1,2}|^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}/;
  const allDates = samples.every((v) => datePattern.test(String(v)));
  if (allDates) return "date";

  const allBools = samples.every((v) => ["true", "false", "0", "1", "yes", "no"].includes(String(v).toLowerCase()));
  if (allBools) return "boolean";

  return "string";
}

function buildCompositeKey(row: Record<string, unknown>, columns: string[]): string {
  return columns
    .map((c) => {
      const v = row[c];
      return v == null ? "" : String(v).trim().toLowerCase();
    })
    .join("|");
}

function extractValues(
  row: Record<string, unknown>,
  mappings: ColumnMapping[],
  side: "file" | "bq"
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const m of mappings) {
    const key = side === "file" ? m.fileColumn : m.bqColumn;
    result[m.bqColumn] = row[key];
  }
  return result;
}

function valuesEqual(fileVal: unknown, bqVal: unknown): boolean {
  // Both empty
  const fStr = fileVal == null ? "" : String(fileVal).trim();
  const bStr = bqVal == null ? "" : String(bqVal).trim();

  if (fStr === "" && bStr === "") return true;
  if (fStr === "" || bStr === "") return false;

  // Try numeric comparison
  const fNum = Number(fStr);
  const bNum = Number(bStr);
  if (!isNaN(fNum) && !isNaN(bNum)) {
    return Math.abs(fNum - bNum) < 0.01;
  }

  // Case-insensitive string comparison
  return fStr.toLowerCase() === bStr.toLowerCase();
}

// ── Column List for Frontend ───────────────────────────────────────

export function getBqColumns(): { column_name: string; display_name: string; category: string; data_type: string }[] {
  return REPORT_COLUMNS.map((c) => ({
    column_name: c.column_name,
    display_name: c.display_name,
    category: c.category,
    data_type: c.data_type,
  }));
}
