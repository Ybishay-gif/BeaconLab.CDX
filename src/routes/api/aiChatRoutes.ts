import { Router } from "express";
import { z } from "zod";
import { handleAiChat, checkRateLimit, type PlanContext } from "../../services/aiChatService.js";

const planContextSchema = z.object({
  activityLeadType: z.string().optional(),
  perfStartDate: z.string().optional(),
  perfEndDate: z.string().optional(),
  qbcClicks: z.number().optional(),
  qbcLeadsCalls: z.number().optional(),
}).optional();

const aiChatSchema = z.object({
  message: z.string().min(1).max(2000),
  sessionId: z.string().min(1).max(100),
  planContext: planContextSchema,
});

export const aiChatRoutes = Router();

aiChatRoutes.post("/ai-chat", async (req, res, next) => {
  try {
    if (!checkRateLimit(req.user!.userId)) {
      res.status(429).json({ error: "Rate limit exceeded. Please wait a moment." });
      return;
    }

    const { message, sessionId, planContext } = aiChatSchema.parse(req.body);
    const result = await handleAiChat(message, sessionId, req.user!.userId, planContext as PlanContext | undefined);
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
