import Anthropic from "@anthropic-ai/sdk";
import { config } from "../config.js";

/* ------------------------------------------------------------------ */
/*  Anthropic client                                                   */
/* ------------------------------------------------------------------ */

let anthropicClient: Anthropic | null = null;

function getAnthropic(): Anthropic {
  if (!anthropicClient) {
    if (!config.anthropicApiKey) {
      throw new Error("ANTHROPIC_API_KEY not configured");
    }
    anthropicClient = new Anthropic({ apiKey: config.anthropicApiKey });
  }
  return anthropicClient;
}

const MODEL = "claude-sonnet-4-20250514";
const MAX_TOKENS = 1024;
const SYSTEM_PROMPT = `You are BeaconLab Assistant, a helpful AI chatbot on Telegram. You assist users with general questions, provide information, and have friendly conversations. Keep responses concise and well-formatted for Telegram (use markdown sparingly — Telegram supports *bold*, _italic_, and \`code\`). If you don't know something, say so honestly.`;

/* ------------------------------------------------------------------ */
/*  Conversation memory — per chat ID, in-memory                       */
/* ------------------------------------------------------------------ */

interface Message {
  role: "user" | "assistant";
  content: string;
}

interface Session {
  history: Message[];
  lastAccess: number;
}

const sessions = new Map<number, Session>();
const MAX_HISTORY = 20;
const SESSION_TTL = 60 * 60 * 1000; // 1 hour

function getOrCreateSession(chatId: number): Session {
  let session = sessions.get(chatId);
  if (!session) {
    session = { history: [], lastAccess: Date.now() };
    sessions.set(chatId, session);
  }
  session.lastAccess = Date.now();
  return session;
}

function trimHistory(session: Session) {
  while (session.history.length > MAX_HISTORY) {
    session.history.shift();
  }
}

// Periodic cleanup
setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.lastAccess > SESSION_TTL) sessions.delete(id);
  }
}, 5 * 60 * 1000);

/* ------------------------------------------------------------------ */
/*  Rate limiting — per chat ID                                        */
/* ------------------------------------------------------------------ */

const rateLimits = new Map<number, { count: number; resetAt: number }>();
const RATE_LIMIT = 20;
const RATE_WINDOW = 60 * 1000;

function checkRateLimit(chatId: number): boolean {
  const now = Date.now();
  let entry = rateLimits.get(chatId);
  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + RATE_WINDOW };
    rateLimits.set(chatId, entry);
  }
  entry.count++;
  return entry.count <= RATE_LIMIT;
}

/* ------------------------------------------------------------------ */
/*  Message splitting (Telegram 4096 char limit)                       */
/* ------------------------------------------------------------------ */

const TELEGRAM_MAX_LENGTH = 4096;

export function splitMessage(text: string): string[] {
  if (text.length <= TELEGRAM_MAX_LENGTH) return [text];
  const chunks: string[] = [];
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= TELEGRAM_MAX_LENGTH) {
      chunks.push(remaining);
      break;
    }
    // Try to split at a newline near the limit
    let splitIdx = remaining.lastIndexOf("\n", TELEGRAM_MAX_LENGTH);
    if (splitIdx < TELEGRAM_MAX_LENGTH * 0.5) {
      // No good newline break — split at space
      splitIdx = remaining.lastIndexOf(" ", TELEGRAM_MAX_LENGTH);
    }
    if (splitIdx < TELEGRAM_MAX_LENGTH * 0.3) {
      // No good break point — hard split
      splitIdx = TELEGRAM_MAX_LENGTH;
    }
    chunks.push(remaining.slice(0, splitIdx));
    remaining = remaining.slice(splitIdx).trimStart();
  }
  return chunks;
}

/* ------------------------------------------------------------------ */
/*  Core handler                                                       */
/* ------------------------------------------------------------------ */

export async function handleTelegramMessage(chatId: number, text: string): Promise<string> {
  if (!checkRateLimit(chatId)) {
    return "You're sending messages too quickly. Please wait a moment and try again.";
  }

  const session = getOrCreateSession(chatId);

  // Handle /start and /help commands
  if (text === "/start") {
    return "Hello! I'm BeaconLab Assistant powered by Claude AI. Send me any message and I'll do my best to help!";
  }
  if (text === "/help") {
    return "Just send me a message and I'll respond! Commands:\n/start — Welcome message\n/help — This help text\n/clear — Clear conversation history";
  }
  if (text === "/clear") {
    session.history = [];
    return "Conversation history cleared.";
  }

  // Add user message to history
  session.history.push({ role: "user", content: text });
  trimHistory(session);

  try {
    const client = getAnthropic();
    const response = await client.messages.create({
      model: MODEL,
      max_tokens: MAX_TOKENS,
      system: SYSTEM_PROMPT,
      messages: session.history,
    });

    const assistantText =
      response.content
        .filter((block) => block.type === "text")
        .map((block) => block.text)
        .join("\n") || "I couldn't generate a response. Please try again.";

    // Add assistant response to history
    session.history.push({ role: "assistant", content: assistantText });
    trimHistory(session);

    return assistantText;
  } catch (err) {
    // Remove the user message if we failed
    session.history.pop();

    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[telegram-bot] Claude API error for chat ${chatId}:`, msg);

    if (msg.includes("429") || msg.includes("rate")) {
      return "I'm receiving too many requests right now. Please try again in a moment.";
    }
    return "Sorry, I encountered an error processing your message. Please try again.";
  }
}
