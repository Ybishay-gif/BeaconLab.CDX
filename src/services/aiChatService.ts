import { GoogleGenerativeAI, type Content } from "@google/generative-ai";
import { query, table } from "../db/index.js";
import { buildSystemPrompt } from "./aiKnowledge.js";
import { ACTION_TOOLS, executeAction, type ActionResult, type ActionPlanContext, type ActionUser } from "./aiActions.js";
import type { Attachment } from "./ticketsService.js";
import {
  ensureSession,
  saveMessage,
  getMessages as getDbMessages,
  loadRecentContext,
  updateSessionTitle,
  deriveTitle,
} from "./aiChatSessionService.js";
import { config } from "../config.js";

/* ------------------------------------------------------------------ */
/*  Gemini client                                                     */
/* ------------------------------------------------------------------ */

const GEMINI_MODEL = "gemini-2.5-flash";

function getGenAI() {
  const key = process.env.GEMINI_API_KEY;
  if (!key) throw Object.assign(new Error("GEMINI_API_KEY not configured"), { status: 503 });
  return new GoogleGenerativeAI(key);
}

/** Retry helper for transient Gemini errors (503, 429 rate limits) */
async function withRetry<T>(fn: () => Promise<T>, retries = 3, delayMs = 3000): Promise<T> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const is503 = msg.includes("503") || msg.includes("Service Unavailable") || msg.includes("overloaded");
      const is429 = msg.includes("429") || msg.includes("quota") || msg.includes("RESOURCE_EXHAUSTED");
      const isRetryable = is503 || is429;
      if (isRetryable && attempt < retries) {
        // Longer delay for rate limits, shorter for 503
        const wait = is429 ? delayMs * (attempt + 2) : delayMs * (attempt + 1);
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
      throw err;
    }
  }
  throw new Error("Unreachable");
}

/* ------------------------------------------------------------------ */
/*  Conversation memory — per-session, in-memory                      */
/* ------------------------------------------------------------------ */

interface Session {
  history: Content[];
  lastAccess: number;
}

const sessions = new Map<string, Session>();
const MAX_HISTORY = 20; // messages (user + model combined)
const SESSION_TTL = 60 * 60 * 1000; // 1 hour

function getOrCreateSession(sessionId: string): Session {
  let session = sessions.get(sessionId);
  if (!session) {
    session = { history: [], lastAccess: Date.now() };
    sessions.set(sessionId, session);
  }
  session.lastAccess = Date.now();
  return session;
}

/** Hydrate in-memory session from DB if empty (e.g. after server restart) */
async function hydrateSession(sessionId: string, userId: string): Promise<Session> {
  const session = getOrCreateSession(sessionId);

  if (session.history.length === 0 && config.usePg) {
    try {
      // Load messages from this session's DB history
      const dbMsgs = await getDbMessages(sessionId, userId, MAX_HISTORY);
      for (const msg of dbMsgs) {
        session.history.push({
          role: msg.role as "user" | "model",
          parts: [{ text: msg.content }],
        });
      }

      // If this is a brand new session, load context from previous sessions
      if (session.history.length === 0) {
        const prevContext = await loadRecentContext(userId, sessionId);
        session.history.push(...prevContext);
      }
    } catch (err) {
      console.warn("Failed to hydrate AI chat session from DB:", err);
    }
  }

  return session;
}

function trimHistory(session: Session) {
  while (session.history.length > MAX_HISTORY) {
    session.history.shift();
  }
}

// Periodic cleanup of expired sessions
setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.lastAccess > SESSION_TTL) sessions.delete(id);
  }
}, 5 * 60 * 1000);

/* ------------------------------------------------------------------ */
/*  Rate limiting — per-user, in-memory                               */
/* ------------------------------------------------------------------ */

const rateLimits = new Map<string, { count: number; resetAt: number }>();
const RATE_LIMIT = 30; // requests per window
const RATE_WINDOW = 60 * 1000; // 1 minute

export function checkRateLimit(userId: string): boolean {
  const now = Date.now();
  let entry = rateLimits.get(userId);
  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + RATE_WINDOW };
    rateLimits.set(userId, entry);
  }
  entry.count++;
  return entry.count <= RATE_LIMIT;
}

/* ------------------------------------------------------------------ */
/*  SQL safety                                                        */
/* ------------------------------------------------------------------ */

const DANGEROUS_KEYWORDS = /\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b/i;

function validateSql(sql: string): boolean {
  const trimmed = sql.trim().replace(/^--.*$/gm, "").trim(); // strip comments
  if (!trimmed.toUpperCase().startsWith("SELECT") && !trimmed.toUpperCase().startsWith("WITH")) {
    return false;
  }
  if (DANGEROUS_KEYWORDS.test(trimmed)) {
    return false;
  }
  return true;
}

function extractSqlBlock(text: string): string | null {
  const match = text.match(/```sql\s*\n([\s\S]*?)```/);
  if (!match) return null;
  // Strip trailing semicolon — we append LIMIT ourselves
  return match[1].trim().replace(/;\s*$/, "");
}

/** Qualify bare table names with dataset prefix when running against BQ */
const KNOWN_TABLES = [
  "state_segment_daily",
  "price_exploration_daily",
  "targets",
  "change_log",
  "plans",
  "plan_parameters",
  "targets_perf_daily",
];

function qualifyTableNames(sql: string): string {
  let result = sql;
  for (const t of KNOWN_TABLES) {
    // Replace bare table name (not already qualified) with table() output
    // Match: FROM/JOIN + optional whitespace + table name (word boundary)
    const qualified = table(t);
    if (qualified !== t) {
      result = result.replace(
        new RegExp(`(\\bFROM|\\bJOIN)\\s+${t}\\b`, "gi"),
        (match, keyword) => `${keyword} ${qualified}`,
      );
    }
  }
  return result;
}

/* ------------------------------------------------------------------ */
/*  Main chat handler                                                 */
/* ------------------------------------------------------------------ */

export interface AiChatResponse {
  answer: string;
  sql?: string;
  action?: {
    type: string;
    payload: unknown;
  };
  error?: string;
}

export interface PlanContext {
  planId?: string;
  activityLeadType?: string;
  perfStartDate?: string;
  perfEndDate?: string;
  priceStartDate?: string;
  priceEndDate?: string;
  qbcClicks?: number;
  qbcLeadsCalls?: number;
  currentPath?: string;
}

/** Persist user + model messages to DB (fire-and-forget, non-blocking) */
async function persistMessages(
  sessionId: string,
  userId: string,
  userMessage: string,
  modelMessage: string,
  sqlQuery?: string,
  action?: { type: string; payload: unknown },
  isFirstMessage?: boolean,
): Promise<void> {
  if (!config.usePg) return;
  try {
    await saveMessage(sessionId, "user", userMessage);
    await saveMessage(sessionId, "model", modelMessage, sqlQuery, action);
    if (isFirstMessage) {
      await updateSessionTitle(sessionId, userId, deriveTitle(userMessage));
    }
  } catch (err) {
    console.warn("Failed to persist AI chat message:", err);
  }
}

export async function handleAiChat(
  message: string,
  sessionId: string,
  user: ActionUser,
  planContext?: PlanContext,
  attachments?: Attachment[],
): Promise<AiChatResponse> {
  const genAI = getGenAI();
  const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
  const systemPrompt = buildSystemPrompt(planContext);

  // Ensure session exists in DB and hydrate in-memory history
  if (config.usePg) {
    await ensureSession(sessionId, user.userId);
  }
  const session = await hydrateSession(sessionId, user.userId);
  const firstPart = session.history[0]?.parts[0];
  const isFirstMessage = session.history.length === 0 ||
    (session.history.length === 2 && firstPart && "text" in firstPart && (firstPart.text ?? "").startsWith("[Previous conversation"));

  // --- Pass 1: Ask Gemini (may return SQL, a function call, or a direct answer) ---
  session.history.push({ role: "user", parts: [{ text: message }] });
  trimHistory(session);

  const chat = model.startChat({
    history: session.history.slice(0, -1), // all except the latest user msg
    systemInstruction: { role: "user", parts: [{ text: systemPrompt }] },
    tools: [ACTION_TOOLS],
  });

  const pass1 = await withRetry(() => chat.sendMessage(message));
  const pass1Response = pass1.response;

  // --- Check for function calls (actions) ---
  const functionCallPart = pass1Response.candidates?.[0]?.content?.parts?.find(
    (p) => p.functionCall,
  );

  if (functionCallPart?.functionCall) {
    return await handleFunctionCall(
      chat,
      session,
      functionCallPart.functionCall.name,
      (functionCallPart.functionCall.args as Record<string, unknown>) || {},
      user,
      { sessionId, userMessage: message, isFirstMessage },
      planContext,
      attachments,
    );
  }

  // --- No function call — check for SQL or direct answer ---
  let pass1Text: string;
  try {
    pass1Text = pass1Response.text();
  } catch {
    // Response has no text (e.g. blocked by safety filters)
    const fallback = "I wasn't able to generate a response. Could you rephrase your question?";
    session.history.push({ role: "model", parts: [{ text: fallback }] });
    trimHistory(session);
    return { answer: fallback };
  }

  const sql = extractSqlBlock(pass1Text);

  if (!sql) {
    // Direct answer (no data query needed)
    session.history.push({ role: "model", parts: [{ text: pass1Text }] });
    trimHistory(session);
    await persistMessages(sessionId, user.userId, message, pass1Text, undefined, undefined, isFirstMessage);
    return { answer: pass1Text };
  }

  // Store Gemini's SQL response in history so follow-ups have context
  session.history.push({ role: "model", parts: [{ text: pass1Text }] });
  trimHistory(session);

  // --- Validate & execute SQL ---
  if (!validateSql(sql)) {
    const errorMsg = "I generated a query but it was blocked by safety checks. I can only run SELECT queries. Could you rephrase your question?";
    session.history.push({ role: "model", parts: [{ text: errorMsg }] });
    trimHistory(session);
    return { answer: errorMsg, sql };
  }

  let queryResults: Record<string, unknown>[];
  try {
    const execSql = qualifyTableNames(sql.includes("LIMIT") ? sql : `${sql}\nLIMIT 100`);
    queryResults = await query<Record<string, unknown>>(execSql);
  } catch (err) {
    const dbError = err instanceof Error ? err.message : String(err);
    // Let Gemini know the query failed and ask it to fix or explain
    const retryPrompt = `The SQL query failed with this error:\n\`\`\`\n${dbError}\n\`\`\`\n\nOriginal query:\n\`\`\`sql\n${sql}\n\`\`\`\n\nPlease either fix the query (respond with a new \`\`\`sql block) or explain what went wrong to the user.`;

    const retryResponse = await withRetry(() => chat.sendMessage(retryPrompt));
    let retryText: string;
    try {
      retryText = retryResponse.response.text();
    } catch {
      retryText = "I tried to fix the query but wasn't able to generate a response. Could you rephrase your question?";
    }
    const retrySql = extractSqlBlock(retryText);

    if (retrySql && validateSql(retrySql)) {
      try {
        const execRetry = qualifyTableNames(retrySql.includes("LIMIT") ? retrySql : `${retrySql}\nLIMIT 100`);
        queryResults = await query<Record<string, unknown>>(execRetry);
        // Fall through to pass 2 with retried results
        return await summarizeResults(chat, session, queryResults, retrySql, { sessionId, userId: user.userId, userMessage: message, isFirstMessage });
      } catch {
        // Both attempts failed
        const failMsg = `I tried to query the data but encountered an error. ${retryText}`;
        session.history.push({ role: "model", parts: [{ text: failMsg }] });
        trimHistory(session);
        await persistMessages(sessionId, user.userId, message, failMsg, retrySql, undefined, isFirstMessage);
        return { answer: failMsg, sql: retrySql };
      }
    }

    // Gemini explained the error instead of retrying
    session.history.push({ role: "model", parts: [{ text: retryText }] });
    trimHistory(session);
    await persistMessages(sessionId, user.userId, message, retryText, sql, undefined, isFirstMessage);
    return { answer: retryText, sql };
  }

  // --- Pass 2: Summarize results ---
  return await summarizeResults(chat, session, queryResults, sql, { sessionId, userId: user.userId, userMessage: message, isFirstMessage });
}

/* ------------------------------------------------------------------ */
/*  Function call handler                                              */
/* ------------------------------------------------------------------ */

async function handleFunctionCall(
  chat: ReturnType<ReturnType<GoogleGenerativeAI["getGenerativeModel"]>["startChat"]>,
  session: Session,
  functionName: string,
  args: Record<string, unknown>,
  user: ActionUser,
  persistCtx?: { sessionId: string; userMessage: string; isFirstMessage: boolean },
  planContext?: PlanContext,
  attachments?: Attachment[],
): Promise<AiChatResponse> {
  const MAX_TOOL_ROUNDS = 5; // prevent infinite loops
  let currentName = functionName;
  let currentArgs = args;
  let lastAction: ActionResult["action"] | undefined;

  // Build ActionPlanContext from PlanContext for tools that need plan-scoped data
  const actionPlanCtx: ActionPlanContext | undefined = planContext
    ? {
        planId: planContext.planId,
        activityLeadType: planContext.activityLeadType,
        perfStartDate: planContext.perfStartDate,
        perfEndDate: planContext.perfEndDate,
        qbcClicks: planContext.qbcClicks,
        qbcLeadsCalls: planContext.qbcLeadsCalls,
      }
    : undefined;

  for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
    // Execute the action — pass attachments for create_ticket
    let actionResult: ActionResult;
    try {
      actionResult = await executeAction(currentName, currentArgs, user, actionPlanCtx, attachments);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      actionResult = { response: { error: `Action failed: ${msg}` } };
    }

    // Track the last user-facing action (report_created, action_list)
    if (actionResult.action) lastAction = actionResult.action;

    // Send function result back to Gemini
    const functionResponse = await withRetry(() =>
      chat.sendMessage([
        {
          functionResponse: {
            name: currentName,
            response: actionResult.response,
          },
        },
      ]),
    );

    // Check if Gemini wants to call another function
    const nextCall = functionResponse.response.candidates?.[0]?.content?.parts?.find(
      (p) => p.functionCall,
    );

    if (nextCall?.functionCall) {
      // Chain to the next tool call
      currentName = nextCall.functionCall.name;
      currentArgs = (nextCall.functionCall.args as Record<string, unknown>) || {};
      continue;
    }

    // No more function calls — Gemini produced a text response
    let answer: string;
    try {
      answer = functionResponse.response.text();
    } catch {
      answer = "I completed the action but wasn't able to generate a summary. Could you try asking again?";
    }
    session.history.push({ role: "model", parts: [{ text: answer }] });
    trimHistory(session);

    if (persistCtx) {
      await persistMessages(persistCtx.sessionId, user.userId, persistCtx.userMessage, answer, undefined, lastAction, persistCtx.isFirstMessage);
    }

    return {
      answer,
      action: lastAction,
    };
  }

  // Exhausted rounds — return what we have
  const fallback = "I ran into a limit while processing your request. Could you try simplifying your question?";
  session.history.push({ role: "model", parts: [{ text: fallback }] });
  trimHistory(session);
  if (persistCtx) {
    await persistMessages(persistCtx.sessionId, user.userId, persistCtx.userMessage, fallback, undefined, lastAction, persistCtx.isFirstMessage);
  }
  return { answer: fallback, action: lastAction };
}

/* ------------------------------------------------------------------ */
/*  SQL result summarizer                                              */
/* ------------------------------------------------------------------ */

async function summarizeResults(
  chat: ReturnType<ReturnType<GoogleGenerativeAI["getGenerativeModel"]>["startChat"]>,
  session: Session,
  results: Record<string, unknown>[],
  sql: string,
  persistCtx?: { sessionId: string; userId: string; userMessage: string; isFirstMessage: boolean },
): Promise<AiChatResponse> {
  const resultText =
    results.length === 0
      ? "The query returned no results."
      : `The query returned ${results.length} row(s):\n\`\`\`json\n${JSON.stringify(results.slice(0, 50), null, 2)}\n\`\`\`${results.length > 50 ? `\n(Showing first 50 of ${results.length} rows)` : ""}`;

  const pass2Prompt = `Here are the query results. Please provide a clear, concise summary for the user. Format numbers nicely (percentages with %, currency with $, ratios as decimals). If there are multiple rows, present them in a readable way.\n\n${resultText}`;

  // Store the results exchange in session history so follow-ups have context
  session.history.push({ role: "user", parts: [{ text: pass2Prompt }] });
  trimHistory(session);

  const pass2 = await withRetry(() => chat.sendMessage(pass2Prompt));
  let answer: string;
  try {
    answer = pass2.response.text();
  } catch {
    answer = resultText; // Fallback to raw results if summarization fails
  }

  session.history.push({ role: "model", parts: [{ text: answer }] });
  trimHistory(session);

  if (persistCtx) {
    await persistMessages(persistCtx.sessionId, persistCtx.userId, persistCtx.userMessage, answer, sql, undefined, persistCtx.isFirstMessage);
  }

  return { answer, sql };
}
