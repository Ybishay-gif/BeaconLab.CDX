import { pgQuery as db } from "../db/postgres.js";
import type { Content } from "@google/generative-ai";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

export interface AiChatSession {
  session_id: string;
  user_id: string;
  title: string;
  created_at: string;
  updated_at: string;
  preview?: string;
  message_count?: number;
}

export interface AiChatMessage {
  message_id: number;
  session_id: string;
  role: "user" | "model";
  content: string;
  sql_query?: string;
  action?: unknown;
  created_at: string;
}

/* ------------------------------------------------------------------ */
/*  Session CRUD                                                       */
/* ------------------------------------------------------------------ */

export async function listSessions(userId: string, limit = 50): Promise<AiChatSession[]> {
  return db<AiChatSession>(`
    SELECT s.session_id, s.user_id, s.title, s.created_at, s.updated_at,
      (SELECT content FROM ai_chat_messages
       WHERE session_id = s.session_id AND role = 'user'
       ORDER BY created_at DESC LIMIT 1) AS preview,
      (SELECT COUNT(*)::int FROM ai_chat_messages
       WHERE session_id = s.session_id) AS message_count
    FROM ai_chat_sessions s
    WHERE s.user_id = @userId
    ORDER BY s.updated_at DESC
    LIMIT @limit
  `, { userId, limit });
}

export async function getSession(sessionId: string, userId: string): Promise<AiChatSession | null> {
  const rows = await db<AiChatSession>(`
    SELECT * FROM ai_chat_sessions
    WHERE session_id = @sessionId AND user_id = @userId
  `, { sessionId, userId });
  return rows[0] ?? null;
}

export async function ensureSession(sessionId: string, userId: string, title?: string): Promise<void> {
  await db(`
    INSERT INTO ai_chat_sessions (session_id, user_id, title)
    VALUES (@sessionId, @userId, @title)
    ON CONFLICT (session_id) DO UPDATE SET updated_at = NOW()
  `, { sessionId, userId, title: title || "New conversation" });
}

export async function updateSessionTitle(sessionId: string, userId: string, title: string): Promise<void> {
  await db(`
    UPDATE ai_chat_sessions SET title = @title, updated_at = NOW()
    WHERE session_id = @sessionId AND user_id = @userId
  `, { title, sessionId, userId });
}

export async function deleteSession(sessionId: string, userId: string): Promise<boolean> {
  const rows = await db<{ session_id: string }>(`
    DELETE FROM ai_chat_sessions
    WHERE session_id = @sessionId AND user_id = @userId
    RETURNING session_id
  `, { sessionId, userId });
  return rows.length > 0;
}

/* ------------------------------------------------------------------ */
/*  Message persistence                                                */
/* ------------------------------------------------------------------ */

export async function saveMessage(
  sessionId: string,
  role: "user" | "model",
  content: string,
  sqlQuery?: string,
  action?: unknown,
): Promise<void> {
  await db(`
    INSERT INTO ai_chat_messages (session_id, role, content, sql_query, action)
    VALUES (@sessionId, @role, @content, @sqlQuery, @action)
  `, { sessionId, role, content, sqlQuery: sqlQuery ?? null, action: action ? JSON.stringify(action) : null });

  await db(`UPDATE ai_chat_sessions SET updated_at = NOW() WHERE session_id = @sessionId`, { sessionId });
}

export async function getMessages(sessionId: string, userId: string, limit = 100): Promise<AiChatMessage[]> {
  return db<AiChatMessage>(`
    SELECT m.message_id, m.session_id, m.role, m.content, m.sql_query, m.action, m.created_at
    FROM ai_chat_messages m
    JOIN ai_chat_sessions s ON s.session_id = m.session_id
    WHERE m.session_id = @sessionId AND s.user_id = @userId
    ORDER BY m.created_at ASC
    LIMIT @limit
  `, { sessionId, userId, limit });
}

/* ------------------------------------------------------------------ */
/*  Cross-session memory: load recent context from past sessions       */
/* ------------------------------------------------------------------ */

export async function loadRecentContext(userId: string, currentSessionId: string, maxMessages = 10): Promise<Content[]> {
  const rows = await db<{ role: string; content: string }>(`
    SELECT m.role, m.content
    FROM ai_chat_messages m
    JOIN ai_chat_sessions s ON s.session_id = m.session_id
    WHERE s.user_id = @userId
      AND s.session_id != @currentSessionId
      AND m.role IN ('user', 'model')
    ORDER BY m.created_at DESC
    LIMIT @maxMessages
  `, { userId, currentSessionId, maxMessages });

  if (rows.length === 0) return [];

  const reversed = rows.reverse();
  const summary = reversed.map(r => `[${r.role}]: ${r.content.slice(0, 200)}`).join("\n");

  return [
    { role: "user", parts: [{ text: `[Previous conversation context for reference]\n${summary}\n[End of previous context]` }] },
    { role: "model", parts: [{ text: "I have the context from your previous conversations. How can I help you?" }] },
  ];
}

/* ------------------------------------------------------------------ */
/*  Auto-title                                                         */
/* ------------------------------------------------------------------ */

export function deriveTitle(firstMessage: string): string {
  const trimmed = firstMessage.trim().replace(/\s+/g, " ");
  if (trimmed.length <= 60) return trimmed;
  const cut = trimmed.slice(0, 60);
  const lastSpace = cut.lastIndexOf(" ");
  return (lastSpace > 30 ? cut.slice(0, lastSpace) : cut) + "...";
}
