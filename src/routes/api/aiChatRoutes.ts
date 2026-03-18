import { Router } from "express";
import { z } from "zod";
import { handleAiChat, checkRateLimit, type PlanContext } from "../../services/aiChatService.js";
import {
  listSessions,
  getMessages,
  deleteSession,
  updateSessionTitle,
} from "../../services/aiChatSessionService.js";

const planContextSchema = z.object({
  planId: z.string().optional(),
  activityLeadType: z.string().optional(),
  perfStartDate: z.string().optional(),
  perfEndDate: z.string().optional(),
  priceStartDate: z.string().optional(),
  priceEndDate: z.string().optional(),
  qbcClicks: z.number().optional(),
  qbcLeadsCalls: z.number().optional(),
}).optional();

const attachmentSchema = z.object({
  filename: z.string().min(1).max(255),
  mimeType: z.string().min(1),
  sizeBytes: z.number().max(3 * 1024 * 1024),
  data: z.string(),
});

const aiChatSchema = z.object({
  message: z.string().min(1).max(2000),
  sessionId: z.string().min(1).max(100),
  planContext: planContextSchema,
  attachments: z.array(attachmentSchema).max(3).optional(),
  currentPath: z.string().max(500).optional(),
});

export const aiChatRoutes = Router();

/* ---- Send message ---- */
aiChatRoutes.post("/ai-chat", async (req, res, next) => {
  try {
    if (!checkRateLimit(req.user!.userId)) {
      res.status(429).json({ error: "Rate limit exceeded. Please wait a moment." });
      return;
    }

    const { message, sessionId, planContext, attachments, currentPath } = aiChatSchema.parse(req.body);
    const userObj = {
      userId: req.user!.userId,
      email: req.user!.email ?? "",
      permissions: req.user!.permissions ?? [],
    };
    const ctx = planContext ? { ...planContext, currentPath } as PlanContext : currentPath ? { currentPath } as PlanContext : undefined;
    const result = await handleAiChat(message, sessionId, userObj, ctx, attachments);
    res.json(result);
  } catch (error) {
    // Friendly error messages for common Gemini failures
    const msg = error instanceof Error ? error.message : String(error);
    if (msg.includes("429") || msg.includes("quota")) {
      res.status(429).json({ answer: "The AI service is temporarily at capacity. Please try again in a minute.", error: "rate_limited" });
      return;
    }
    if (msg.includes("API_KEY_INVALID") || msg.includes("API key not valid")) {
      res.status(503).json({ answer: "The AI service is not configured properly. Please contact your administrator.", error: "config_error" });
      return;
    }
    next(error);
  }
});

/* ---- List sessions ---- */
aiChatRoutes.get("/ai-chat/sessions", async (req, res, next) => {
  try {
    const sessions = await listSessions(req.user!.userId);
    res.json(sessions);
  } catch (error) {
    next(error);
  }
});

/* ---- Get session messages ---- */
aiChatRoutes.get("/ai-chat/sessions/:sessionId/messages", async (req, res, next) => {
  try {
    const messages = await getMessages(req.params.sessionId, req.user!.userId);
    res.json(messages);
  } catch (error) {
    next(error);
  }
});

/* ---- Update session title ---- */
aiChatRoutes.put("/ai-chat/sessions/:sessionId", async (req, res, next) => {
  try {
    const { title } = z.object({ title: z.string().min(1).max(200) }).parse(req.body);
    await updateSessionTitle(req.params.sessionId, req.user!.userId, title);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

/* ---- Delete session ---- */
aiChatRoutes.delete("/ai-chat/sessions/:sessionId", async (req, res, next) => {
  try {
    const deleted = await deleteSession(req.params.sessionId, req.user!.userId);
    if (!deleted) {
      res.status(404).json({ error: "Session not found" });
      return;
    }
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});
