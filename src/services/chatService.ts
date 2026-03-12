import { pgQuery as query } from "../db/postgres.js";

// Chat always uses PG directly (chat tables only exist in PG, not BQ).
// table() is not needed since PG uses plain table names.

/* ---------- types ---------- */

export interface ChatRoom {
  room_id: number;
  room_type: "general" | "dm";
  room_name: string | null;
  created_at: string;
  unread_count: number;
  last_message_at: string | null;
  other_user_id: string | null;
  other_user_email: string | null;
  other_user_name: string | null;
}

export interface ChatMessage {
  message_id: number;
  room_id: number;
  sender_id: string;
  sender_email: string;
  sender_name: string | null;
  content:
    | { type: "text"; text: string }
    | { type: "image"; data: string; mimeType: string }
    | { type: "file"; data: string; mimeType: string; filename: string };
  created_at: string;
}

export interface ChatUser {
  user_id: string;
  email: string;
  name: string | null;
}

/* ---------- helpers ---------- */

function fail(status: number, message: string): never {
  const error = new Error(message) as Error & { status: number };
  error.status = status;
  throw error;
}

/** Ensure content is a parsed object — pg sometimes returns JSONB as a raw string */
function normalizeContent(content: unknown): ChatMessage["content"] {
  if (typeof content === "string") {
    try { return JSON.parse(content); } catch { return { type: "text", text: content }; }
  }
  return content as ChatMessage["content"];
}

function normalizeMessages(rows: ChatMessage[]): ChatMessage[] {
  return rows.map((r) => ({ ...r, content: normalizeContent(r.content) }));
}

/* ---------- rooms ---------- */

export async function listRoomsForUser(userId: string): Promise<ChatRoom[]> {
  const rows = await query<ChatRoom>(
    `WITH user_rooms AS (
       -- General room (implicit membership for everyone)
       SELECT room_id FROM chat_rooms WHERE room_type = 'general'
       UNION
       -- DM rooms user is a member of
       SELECT room_id FROM chat_room_members WHERE user_id = @userId
     )
     SELECT
       r.room_id,
       r.room_type,
       r.room_name,
       r.created_at::text AS created_at,
       COALESCE(unread.cnt, 0)::int AS unread_count,
       lm.last_message_at::text AS last_message_at,
       other_member.user_id AS other_user_id,
       other_member.email AS other_user_email,
       other_member.name AS other_user_name
     FROM user_rooms ur
     JOIN chat_rooms r ON r.room_id = ur.room_id
     LEFT JOIN LATERAL (
       SELECT COUNT(*)::int AS cnt
       FROM chat_messages m
       LEFT JOIN chat_read_status rs
         ON rs.room_id = m.room_id AND rs.user_id = @userId
       WHERE m.room_id = r.room_id
         AND m.created_at > COALESCE(rs.last_read_at, '1970-01-01'::timestamptz)
         AND m.sender_id != @userId
     ) unread ON true
     LEFT JOIN LATERAL (
       SELECT MAX(created_at) AS last_message_at
       FROM chat_messages
       WHERE room_id = r.room_id
     ) lm ON true
     LEFT JOIN LATERAL (
       SELECT u.user_id, u.email, u.name
       FROM chat_room_members crm
       JOIN users u ON u.user_id = crm.user_id
       WHERE crm.room_id = r.room_id AND crm.user_id != @userId
       LIMIT 1
     ) other_member ON r.room_type = 'dm'
     ORDER BY r.room_type ASC, lm.last_message_at DESC NULLS LAST`,
    { userId }
  );
  return rows;
}

export async function getOrCreateDmRoom(userId1: string, userId2: string): Promise<number> {
  if (userId1 === userId2) fail(400, "Cannot create DM with yourself");

  // Find existing DM room
  const existing = await query<{ room_id: number }>(
    `SELECT rm.room_id
     FROM chat_room_members rm
     JOIN chat_rooms r ON r.room_id = rm.room_id AND r.room_type = 'dm'
     WHERE rm.user_id IN (@userId1, @userId2)
     GROUP BY rm.room_id
     HAVING COUNT(DISTINCT rm.user_id) = 2`,
    { userId1, userId2 }
  );

  if (existing.length > 0) return existing[0].room_id;

  // Create new DM room
  const created = await query<{ room_id: number }>(
    `INSERT INTO chat_rooms (room_type) VALUES ('dm') RETURNING room_id`,
    {}
  );
  const roomId = created[0].room_id;

  // Add both members
  await query(
    `INSERT INTO chat_room_members (room_id, user_id) VALUES (@roomId, @userId1)`,
    { roomId, userId1 }
  );
  await query(
    `INSERT INTO chat_room_members (room_id, user_id) VALUES (@roomId, @userId2)`,
    { roomId, userId2 }
  );

  return roomId;
}

/* ---------- messages ---------- */

export async function getMessages(
  roomId: number,
  userId: string,
  before?: number,
  limit = 50
): Promise<ChatMessage[]> {
  await assertRoomAccess(roomId, userId);

  const safeLimit = Math.min(limit, 100);
  const conditions = ["m.room_id = @roomId"];
  const params: Record<string, unknown> = { roomId };

  if (before) {
    conditions.push("m.message_id < @before");
    params.before = before;
  }

  const rows = await query<ChatMessage>(
    `SELECT m.message_id, m.room_id, m.sender_id, m.sender_email,
            u.name AS sender_name,
            m.content, m.created_at::text AS created_at
     FROM chat_messages m
     LEFT JOIN users u ON u.user_id = m.sender_id
     WHERE ${conditions.join(" AND ")}
     ORDER BY m.created_at DESC
     LIMIT ${safeLimit}`,
    params
  );

  return normalizeMessages(rows.reverse()); // return in chronological order
}

export async function sendMessage(
  roomId: number,
  senderId: string,
  senderEmail: string,
  content: ChatMessage["content"]
): Promise<ChatMessage> {
  await assertRoomAccess(roomId, senderId);

  const rows = await query<ChatMessage>(
    `INSERT INTO chat_messages (room_id, sender_id, sender_email, content)
     VALUES (@roomId, @senderId, @senderEmail, @content::jsonb)
     RETURNING message_id, room_id, sender_id, sender_email, content, created_at::text AS created_at`,
    {
      roomId,
      senderId,
      senderEmail,
      content: JSON.stringify(content),
    }
  );

  // Also update read status for sender so they don't see their own message as unread
  await markRoomRead(roomId, senderId);

  return { ...rows[0], sender_name: null, content: normalizeContent(rows[0].content) };
}

/* ---------- read status ---------- */

export async function markRoomRead(roomId: number, userId: string): Promise<void> {
  await query(
    `INSERT INTO chat_read_status (room_id, user_id, last_read_at)
     VALUES (@roomId, @userId, NOW())
     ON CONFLICT (room_id, user_id) DO UPDATE SET last_read_at = NOW()`,
    { roomId, userId }
  );
}

export async function getUnreadCounts(userId: string): Promise<Record<string, number>> {
  const rows = await query<{ room_id: number; unread_count: number }>(
    `WITH user_rooms AS (
       SELECT room_id FROM chat_rooms WHERE room_type = 'general'
       UNION
       SELECT room_id FROM chat_room_members WHERE user_id = @userId
     )
     SELECT ur.room_id, COUNT(m.message_id)::int AS unread_count
     FROM user_rooms ur
     JOIN chat_messages m ON m.room_id = ur.room_id AND m.sender_id != @userId
     LEFT JOIN chat_read_status rs ON rs.room_id = ur.room_id AND rs.user_id = @userId
     WHERE m.created_at > COALESCE(rs.last_read_at, '1970-01-01'::timestamptz)
     GROUP BY ur.room_id
     HAVING COUNT(m.message_id) > 0`,
    { userId }
  );

  const counts: Record<string, number> = {};
  for (const r of rows) counts[r.room_id] = r.unread_count;
  return counts;
}

/* ---------- users ---------- */

export async function listChatUsers(): Promise<ChatUser[]> {
  return query<ChatUser>(
    `SELECT user_id, email, name FROM users WHERE is_active = true ORDER BY email`,
    {}
  );
}

/* ---------- access control ---------- */

async function assertRoomAccess(roomId: number, userId: string): Promise<void> {
  // General room — everyone has access
  const room = await query<{ room_type: string }>(
    `SELECT room_type FROM chat_rooms WHERE room_id = @roomId`,
    { roomId }
  );
  if (room.length === 0) fail(404, "Room not found");
  if (room[0].room_type === "general") return;

  // DM room — must be a member
  const member = await query<{ user_id: string }>(
    `SELECT user_id FROM chat_room_members
     WHERE room_id = @roomId AND user_id = @userId`,
    { roomId, userId }
  );
  if (member.length === 0) fail(403, "Not a member of this room");
}
