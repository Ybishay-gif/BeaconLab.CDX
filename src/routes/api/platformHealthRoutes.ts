import { Router } from "express";
import {
  listSyncHistory,
  listSecurityTests,
  getSecurityTestById,
} from "../../services/platformHealthService.js";
import { generateSecurityReportPdf } from "../../services/securityReportPdf.js";
import { requirePermission } from "../../middleware/auth.js";

export const platformHealthRoutes = Router();

const requirePlatformHealth = requirePermission("platform_health:view");

// ── Sync History ─────────────────────────────────────────────────────

platformHealthRoutes.get("/platform-health/sync-history", requirePlatformHealth, async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 50, 200);
    const offset = Number(req.query.offset) || 0;
    const result = await listSyncHistory(limit, offset);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch sync history", detail: String(err) });
  }
});

// ── Security Tests ───────────────────────────────────────────────────

platformHealthRoutes.get("/platform-health/security-tests", requirePlatformHealth, async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 50, 200);
    const offset = Number(req.query.offset) || 0;
    const result = await listSecurityTests(limit, offset);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch security tests", detail: String(err) });
  }
});

// ── Security Test PDF Download ───────────────────────────────────────

platformHealthRoutes.get("/platform-health/security-tests/:id/pdf", requirePlatformHealth, async (req, res) => {
  try {
    const row = await getSecurityTestById(req.params.id);
    if (!row) {
      res.status(404).json({ error: "Security test result not found" });
      return;
    }

    const pdf = await generateSecurityReportPdf(row);

    const dateStr = new Date(row.ran_at).toISOString().slice(0, 10);
    const filename = `security-report-${row.test_type}-${dateStr}.pdf`;

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send(pdf);
  } catch (err) {
    res.status(500).json({ error: "Failed to generate PDF", detail: String(err) });
  }
});
