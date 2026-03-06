import { Router } from "express";
import {
  getPlanMergedAnalytics,
  getPriceExploration,
  getStateAnalysis,
  getStateSegmentPerformance,
  getStrategyAnalysis,
  listPlanMergedFilters,
  listPriceExplorationFilters,
  listStateSegmentFilters
} from "../../services/analyticsService.js";
import { parseOptionalNumber, parseQueryArray } from "./queryParsers.js";

export const analyticsRoutes = Router();

analyticsRoutes.get("/analytics/state-segment-performance/filters", async (req, res, next) => {
  try {
    const filters = await listStateSegmentFilters({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json(filters);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/state-segment-performance", async (req, res, next) => {
  try {
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    const rows = await getStateSegmentPerformance({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      states: parseQueryArray(req.query.states),
      segments: parseQueryArray(req.query.segments),
      channelGroups: parseQueryArray(req.query.channelGroups),
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/price-exploration/filters", async (req, res, next) => {
  try {
    const filters = await listPriceExplorationFilters({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json(filters);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/price-exploration", async (req, res, next) => {
  try {
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }
    const limit = parseOptionalNumber(req.query.limit);

    const rows = await getPriceExploration({
      planId: typeof req.query.planId === "string" ? req.query.planId : undefined,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      q2bStartDate: typeof req.query.q2bStartDate === "string" ? req.query.q2bStartDate : undefined,
      q2bEndDate: typeof req.query.q2bEndDate === "string" ? req.query.q2bEndDate : undefined,
      states: parseQueryArray(req.query.states),
      channelGroups: parseQueryArray(req.query.channelGroups),
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc,
      limit
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/plan-merged/filters", async (req, res, next) => {
  try {
    const filters = await listPlanMergedFilters({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json(filters);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/plan-merged", async (req, res, next) => {
  try {
    const rows = await getPlanMergedAnalytics({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      states: parseQueryArray(req.query.states),
      segments: parseQueryArray(req.query.segments),
      channelGroups: parseQueryArray(req.query.channelGroups),
      testingPoints: parseQueryArray(req.query.testingPoints),
      statSig: parseQueryArray(req.query.statSig),
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/strategy-analysis", async (req, res, next) => {
  try {
    const planId = typeof req.query.planId === "string" ? req.query.planId.trim() : "";
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    if (!planId) {
      res.status(400).json({ error: "planId is required" });
      return;
    }

    const rows = await getStrategyAnalysis({
      planId,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/state-analysis", async (req, res, next) => {
  try {
    const planId = typeof req.query.planId === "string" ? req.query.planId.trim() : "";
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    if (!planId) {
      res.status(400).json({ error: "planId is required" });
      return;
    }

    const payload = await getStateAnalysis({
      planId,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    res.json(payload);
  } catch (error) {
    next(error);
  }
});
