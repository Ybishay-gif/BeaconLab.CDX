import { Router } from "express";
import { z } from "zod";
import {
  listConnections,
  getConnection,
  createConnection,
  updateConnection,
  deleteConnection,
  testConnection,
  uploadReportToSftp,
  listUploads,
} from "../../services/sftpService.js";
import { getReport } from "../../services/reportService.js";

const createConnectionSchema = z.object({
  name: z.string().min(1).max(200),
  host: z.string().min(1).max(255),
  port: z.number().int().min(1).max(65535).default(22),
  username: z.string().min(1).max(100),
  password: z.string().min(1).max(500),
  remotePath: z.string().max(500).default("/"),
});

const updateConnectionSchema = z.object({
  name: z.string().min(1).max(200).optional(),
  host: z.string().min(1).max(255).optional(),
  port: z.number().int().min(1).max(65535).optional(),
  username: z.string().min(1).max(100).optional(),
  password: z.string().min(1).max(500).optional(),
  remotePath: z.string().max(500).optional(),
});

const uploadSchema = z.object({
  reportId: z.string().min(1),
  connectionId: z.string().min(1),
});

export const sftpRoutes = Router();

// ── Connections CRUD ───────────────────────────────────────────────

sftpRoutes.get("/sftp/connections", async (req, res, next) => {
  try {
    const connections = await listConnections();
    res.json({ connections });
  } catch (error) {
    next(error);
  }
});

sftpRoutes.post("/sftp/connections", async (req, res, next) => {
  try {
    const parsed = createConnectionSchema.parse(req.body);
    const result = await createConnection(parsed, req.user!.userId);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

sftpRoutes.put("/sftp/connections/:id", async (req, res, next) => {
  try {
    const conn = await getConnection(req.params.id);
    if (!conn) return res.status(404).json({ error: "Connection not found" });
    const parsed = updateConnectionSchema.parse(req.body);
    await updateConnection(req.params.id, parsed);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

sftpRoutes.delete("/sftp/connections/:id", async (req, res, next) => {
  try {
    const conn = await getConnection(req.params.id);
    if (!conn) return res.status(404).json({ error: "Connection not found" });
    await deleteConnection(req.params.id);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

sftpRoutes.post("/sftp/connections/:id/test", async (req, res, next) => {
  try {
    const result = await testConnection(req.params.id);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ── Upload ─────────────────────────────────────────────────────────

sftpRoutes.post("/sftp/upload", async (req, res, next) => {
  try {
    const { reportId, connectionId } = uploadSchema.parse(req.body);

    // Validate report exists and is done
    const report = await getReport(reportId);
    if (!report) return res.status(404).json({ error: "Report not found" });
    if (report.status !== "done") return res.status(400).json({ error: "Report is not ready" });

    // Validate connection exists
    const conn = await getConnection(connectionId);
    if (!conn || !conn.is_active) return res.status(404).json({ error: "SFTP connection not found" });

    const result = await uploadReportToSftp(reportId, connectionId, req.user!.userId);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

sftpRoutes.get("/sftp/uploads/:reportId", async (req, res, next) => {
  try {
    const uploads = await listUploads(req.params.reportId);
    res.json({ uploads });
  } catch (error) {
    next(error);
  }
});
