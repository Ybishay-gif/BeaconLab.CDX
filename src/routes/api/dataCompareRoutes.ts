import { Router } from "express";
import { z } from "zod";
import {
  parseUploadedFile,
  fetchMatchingBqRows,
  compareRows,
  getBqColumns,
  type ColumnMapping,
} from "../../services/dataCompareService.js";

export const dataCompareRoutes = Router();

// ── Schemas ────────────────────────────────────────────────────────

const parseSchema = z.object({
  filename: z.string().min(1),
  data: z.string().min(1), // base64
  mimeType: z.string().min(1),
});

const mappingSchema = z.object({
  fileColumn: z.string().min(1),
  bqColumn: z.string().min(1),
  isMatchKey: z.boolean(),
});

const compareSchema = z.object({
  file: z.object({
    filename: z.string().min(1),
    data: z.string().min(1),
    mimeType: z.string().min(1),
  }),
  mappings: z.array(mappingSchema).min(1),
});

// ── GET /data-compare/columns ──────────────────────────────────────

dataCompareRoutes.get("/data-compare/columns", (_req, res) => {
  res.json({ columns: getBqColumns() });
});

// ── POST /data-compare/parse ───────────────────────────────────────

dataCompareRoutes.post("/data-compare/parse", (req, res, next) => {
  (async () => {
    const parsed = parseSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: "Invalid input", details: parsed.error.issues });
    }

    const { data, mimeType } = parsed.data;
    const result = parseUploadedFile(data, mimeType);

    res.json({
      columns: result.columns,
      rowCount: result.rowCount,
    });
  })().catch(next);
});

// ── POST /data-compare/compare ─────────────────────────────────────

dataCompareRoutes.post("/data-compare/compare", (req, res, next) => {
  (async () => {
    const parsed = compareSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: "Invalid input", details: parsed.error.issues });
    }

    const { file, mappings } = parsed.data;

    // Validate at least one match key
    const matchKeys = mappings.filter((m: ColumnMapping) => m.isMatchKey);
    if (matchKeys.length === 0) {
      return res.status(400).json({ error: "At least one column must be marked as a match key" });
    }

    // Parse the file
    const { rows: fileRows } = parseUploadedFile(file.data, file.mimeType);

    // Fetch matching BQ rows
    const bqRows = await fetchMatchingBqRows(mappings, fileRows);

    // Compare
    const result = compareRows(fileRows, bqRows, mappings);

    res.json(result);
  })().catch(next);
});
