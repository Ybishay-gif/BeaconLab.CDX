import { Router } from "express";
import { z } from "zod";
import {
  listReports,
  createReport,
  getReport,
  getDownloadUrl,
  deleteReport,
  getTableSchema,
  getFilterValues,
  checkRowCount,
  listTemplates,
  getTemplate,
  createTemplate,
  deleteTemplate,
} from "../../services/reportService.js";
import {
  listPresets,
  getPreset,
  createPreset,
  updatePreset,
  deletePreset,
} from "../../services/columnPresetService.js";

const createReportSchema = z.object({
  reportName: z.string().min(1).max(200),
  dateStart: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  dateEnd: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  fixedFilters: z
    .object({
      account_name: z.array(z.string()).optional(),
      campaign_name: z.array(z.string()).optional(),
      attribution_channel: z.array(z.string()).optional(),
      data_state: z.array(z.string()).optional(),
      transaction_sold: z.enum(["0", "1", "all"]).optional(),
    })
    .default({}),
  dynamicFilters: z
    .array(
      z.object({
        column: z.string(),
        operator: z.enum(["=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN"]),
        value: z.union([z.string(), z.number(), z.array(z.union([z.string(), z.number()]))]),
      })
    )
    .default([]),
  selectedColumns: z.array(z.string()).min(1),
  includeOpps: z.boolean().default(false),
});

export const reportRoutes = Router();

// ── Template schema ────────────────────────────────────────────────

const createTemplateSchema = z.object({
  templateName: z.string().min(1).max(200),
  fixedFilters: z
    .object({
      account_name: z.array(z.string()).optional(),
      campaign_name: z.array(z.string()).optional(),
      attribution_channel: z.array(z.string()).optional(),
      data_state: z.array(z.string()).optional(),
      transaction_sold: z.enum(["0", "1", "all"]).optional(),
    })
    .default({}),
  dynamicFilters: z
    .array(
      z.object({
        column: z.string(),
        operator: z.enum(["=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN"]),
        value: z.union([z.string(), z.number(), z.array(z.union([z.string(), z.number()]))]),
      })
    )
    .default([]),
  selectedColumns: z.array(z.string()).min(1),
  includeOpps: z.boolean().default(false),
});

// ── Template routes (must be ABOVE :id routes) ────────────────────

reportRoutes.get("/reports/templates", async (req, res, next) => {
  try {
    const isAdmin = req.user!.role === "admin";
    const templates = await listTemplates(req.user!.userId, isAdmin);
    res.json({ templates });
  } catch (error) {
    next(error);
  }
});

reportRoutes.get("/reports/templates/:templateId", async (req, res, next) => {
  try {
    const template = await getTemplate(req.params.templateId);
    if (!template) return res.status(404).json({ error: "Template not found" });
    res.json({ template });
  } catch (error) {
    next(error);
  }
});

reportRoutes.post("/reports/templates", async (req, res, next) => {
  try {
    const parsed = createTemplateSchema.parse(req.body);
    const result = await createTemplate(req.user!.userId, parsed);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

reportRoutes.delete("/reports/templates/:templateId", async (req, res, next) => {
  try {
    const template = await getTemplate(req.params.templateId);
    if (!template) return res.status(404).json({ error: "Template not found" });
    if (template.user_id !== req.user!.userId && req.user!.role !== "admin") {
      return res.status(403).json({ error: "Not authorized" });
    }
    await deleteTemplate(req.params.templateId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// ── Column Preset schema ──────────────────────────────────────────

const createPresetSchema = z.object({
  presetName: z.string().min(1).max(200),
  columns: z
    .array(
      z.object({
        column_name: z.string().min(1),
        display_name: z.string().min(1),
      })
    )
    .min(1),
});

const updatePresetSchema = z.object({
  presetName: z.string().min(1).max(200).optional(),
  columns: z
    .array(
      z.object({
        column_name: z.string().min(1),
        display_name: z.string().min(1),
      })
    )
    .min(1)
    .optional(),
});

// ── Column Preset routes (must be ABOVE :id routes) ──────────────

reportRoutes.get("/reports/column-presets", async (req, res, next) => {
  try {
    const isAdmin = req.user!.role === "admin";
    const presets = await listPresets(req.user!.userId, isAdmin);
    res.json({ presets });
  } catch (error) {
    next(error);
  }
});

reportRoutes.get("/reports/column-presets/:presetId", async (req, res, next) => {
  try {
    const preset = await getPreset(req.params.presetId);
    if (!preset) return res.status(404).json({ error: "Preset not found" });
    res.json({ preset });
  } catch (error) {
    next(error);
  }
});

reportRoutes.post("/reports/column-presets", async (req, res, next) => {
  try {
    const parsed = createPresetSchema.parse(req.body);
    const result = await createPreset(req.user!.userId, parsed);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

reportRoutes.put("/reports/column-presets/:presetId", async (req, res, next) => {
  try {
    const parsed = updatePresetSchema.parse(req.body);
    const preset = await getPreset(req.params.presetId);
    if (!preset) return res.status(404).json({ error: "Preset not found" });
    if (preset.user_id !== req.user!.userId && req.user!.role !== "admin") {
      return res.status(403).json({ error: "Not authorized" });
    }
    await updatePreset(req.params.presetId, parsed);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

reportRoutes.delete("/reports/column-presets/:presetId", async (req, res, next) => {
  try {
    const preset = await getPreset(req.params.presetId);
    if (!preset) return res.status(404).json({ error: "Preset not found" });
    if (preset.user_id !== req.user!.userId && req.user!.role !== "admin") {
      return res.status(403).json({ error: "Not authorized" });
    }
    await deletePreset(req.params.presetId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// Static routes FIRST (before :id param routes)

reportRoutes.get("/reports/schema", async (req, res, next) => {
  try {
    const includeOpps = req.query.includeOpps === "true";
    const columns = await getTableSchema(includeOpps);
    res.json({ columns });
  } catch (error) {
    next(error);
  }
});

reportRoutes.get("/reports/filter-values/:column", async (req, res, next) => {
  try {
    const includeOpps = req.query.includeOpps === "true";
    const values = await getFilterValues(req.params.column, includeOpps);
    res.json({ values });
  } catch (error) {
    next(error);
  }
});

// Check row count (preview)
const checkReportSchema = z.object({
  dateStart: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  dateEnd: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  fixedFilters: z
    .object({
      account_name: z.array(z.string()).optional(),
      campaign_name: z.array(z.string()).optional(),
      attribution_channel: z.array(z.string()).optional(),
      data_state: z.array(z.string()).optional(),
      transaction_sold: z.enum(["0", "1", "all"]).optional(),
    })
    .default({}),
  dynamicFilters: z
    .array(
      z.object({
        column: z.string(),
        operator: z.enum(["=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN"]),
        value: z.union([z.string(), z.number(), z.array(z.union([z.string(), z.number()]))]),
      })
    )
    .default([]),
  includeOpps: z.boolean().default(false),
});

reportRoutes.post("/reports/check", async (req, res, next) => {
  try {
    const parsed = checkReportSchema.parse(req.body);
    const result = await checkRowCount(parsed);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// List reports (admins see all reports)
reportRoutes.get("/reports", async (req, res, next) => {
  try {
    const isAdmin = req.user!.role === "admin";
    const reports = await listReports(req.user!.userId, isAdmin);
    res.json({ reports });
  } catch (error) {
    next(error);
  }
});

// Create report
reportRoutes.post("/reports", async (req, res, next) => {
  try {
    const parsed = createReportSchema.parse(req.body);
    const result = await createReport(req.user!.userId, parsed);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

// Get single report
reportRoutes.get("/reports/:id", async (req, res, next) => {
  try {
    const report = await getReport(req.params.id);
    if (!report) return res.status(404).json({ error: "Report not found" });
    res.json({ report });
  } catch (error) {
    next(error);
  }
});

// Download report — return signed URL as JSON (redirect breaks cross-origin fetch)
reportRoutes.get("/reports/:id/download", async (req, res, next) => {
  try {
    const url = await getDownloadUrl(req.params.id);
    if (!url) return res.status(404).json({ error: "Report not ready or not found" });
    res.json({ url });
  } catch (error) {
    next(error);
  }
});

// Delete report
reportRoutes.delete("/reports/:id", async (req, res, next) => {
  try {
    const report = await getReport(req.params.id);
    if (!report) return res.status(404).json({ error: "Report not found" });
    if (report.user_id !== req.user!.userId && req.user!.role !== "admin") {
      return res.status(403).json({ error: "Not authorized" });
    }
    await deleteReport(req.params.id);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});
