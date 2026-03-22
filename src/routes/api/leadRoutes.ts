/**
 * Lead Browse & Detail API routes.
 */

import { Router } from "express";
import { requirePermission } from "../../middleware/auth.js";
import {
  browseLeads,
  getLeadFilterValues,
  getRelatedLeads,
  type LeadBrowseParams,
} from "../../services/leadBrowseService.js";
import { getLeadDetails } from "../../services/leadLookupService.js";

export const leadRoutes = Router();

// ── POST /leads/search — paginated, filtered lead listing ────────────

leadRoutes.post("/leads/search", requirePermission("leads:view"), async (req, res, next) => {
  try {
    const body = req.body as Partial<LeadBrowseParams>;

    if (!body.startDate || !body.endDate) {
      res.status(400).json({ error: "startDate and endDate are required" });
      return;
    }

    const params: LeadBrowseParams = {
      startDate: body.startDate,
      endDate: body.endDate,
      activityTypes: body.activityTypes,
      leadTypes: body.leadTypes,
      accountNames: body.accountNames,
      campaigns: body.campaigns,
      segments: body.segments,
      states: body.states,
      channels: body.channels,
      rc1Statuses: body.rc1Statuses,
      rejectReasons: body.rejectReasons,
      rc1Reasons: body.rc1Reasons,
      statuses: body.statuses,
      dynamicFilters: body.dynamicFilters,
      sortColumn: body.sortColumn,
      sortDir: body.sortDir,
      offset: body.offset,
      limit: body.limit,
    };

    const result = await browseLeads(params);
    res.json(result);
  } catch (err) {
    next(err);
  }
});

// ── GET /leads/filter-values — distinct values for a filter column ───

leadRoutes.get("/leads/filter-values", requirePermission("leads:view"), async (req, res, next) => {
  try {
    const column = req.query.column as string;
    if (!column) {
      res.status(400).json({ error: "column query param is required" });
      return;
    }

    const values = await getLeadFilterValues(column);
    res.json({ values });
  } catch (err) {
    next(err);
  }
});

// ── GET /leads/:leadId — full lead details (all sections) ───────────

leadRoutes.get("/leads/:leadId", requirePermission("leads:view"), async (req, res, next) => {
  try {
    const { leadId } = req.params;
    const result = await getLeadDetails("beacon_id", leadId, ["all"]);
    res.json(result);
  } catch (err) {
    next(err);
  }
});

// ── GET /leads/:leadId/related — related leads by shared identifiers ─

leadRoutes.get("/leads/:leadId/related", requirePermission("leads:view"), async (req, res, next) => {
  try {
    const { leadId } = req.params;
    const relatedLeads = await getRelatedLeads(leadId);
    res.json({ relatedLeads });
  } catch (err) {
    next(err);
  }
});
