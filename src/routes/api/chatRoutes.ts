import { Router } from "express";
import { z } from "zod";
import { config } from "../../config.js";
import {
  listRoomsForUser,
  getOrCreateDmRoom,
  getMessages,
  sendMessage,
  markRoomRead,
  getUnreadCounts,
  listChatUsers,
} from "../../services/chatService.js";

// Chat requires PG. If PG is not configured, return empty results gracefully.
function requirePg(_req: import("express").Request, res: import("express").Response, next: import("express").NextFunction) {
  if (!process.env.PGPASSWORD && !config.usePg) {
    res.json({ rooms: [], counts: {}, messages: [], users: [], ok: true });
    return;
  }
  next();
}

const sendMessageSchema = z.object({
  content: z.discriminatedUnion("type", [
    z.object({ type: z.literal("text"), text: z.string().min(1).max(5000) }),
    z.object({
      type: z.literal("image"),
      data: z.string().max(14_000_000), // ~10MB file → ~13.3MB base64
      mimeType: z.string().regex(/^image\//).max(100),
    }),
    z.object({
      type: z.literal("file"),
      data: z.string().max(14_000_000),
      mimeType: z.string().max(200),
      filename: z.string().max(255),
    }),
  ]),
});

const dmRoomSchema = z.object({
  targetUserId: z.string().min(1),
});

export const chatRoutes = Router();

chatRoutes.use("/chat", requirePg);

// GET /api/chat/rooms — list rooms for current user (includes unread counts)
chatRoutes.get("/chat/rooms", async (req, res, next) => {
  try {
    const rooms = await listRoomsForUser(req.user!.userId);
    res.json({ rooms });
  } catch (error) {
    next(error);
  }
});

// POST /api/chat/rooms/dm — get or create a DM room
chatRoutes.post("/chat/rooms/dm", async (req, res, next) => {
  try {
    const { targetUserId } = dmRoomSchema.parse(req.body);
    const roomId = await getOrCreateDmRoom(req.user!.userId, targetUserId);
    // Return room list entry so frontend has full info
    const rooms = await listRoomsForUser(req.user!.userId);
    const room = rooms.find((r) => r.room_id === roomId);
    res.json({ room: room ?? { room_id: roomId } });
  } catch (error) {
    next(error);
  }
});

// GET /api/chat/rooms/:roomId/messages — paginated messages
chatRoutes.get("/chat/rooms/:roomId/messages", async (req, res, next) => {
  try {
    const roomId = Number(req.params.roomId);
    const before = req.query.before ? Number(req.query.before) : undefined;
    const limit = req.query.limit ? Number(req.query.limit) : 50;
    const messages = await getMessages(roomId, req.user!.userId, before, limit);
    res.json({ messages });
  } catch (error) {
    next(error);
  }
});

// POST /api/chat/rooms/:roomId/messages — send a message
chatRoutes.post("/chat/rooms/:roomId/messages", async (req, res, next) => {
  try {
    const roomId = Number(req.params.roomId);
    const rawContent = req.body?.content;
    console.log(`[chat] PRE-VALIDATE: type=${rawContent?.type}, mimeType=${rawContent?.mimeType}, dataLen=${rawContent?.data?.length ?? 0}, filename=${rawContent?.filename ?? "none"}`);
    const { content } = sendMessageSchema.parse(req.body);
    console.log(`[chat] POST message OK: type=${content.type}`);
    const message = await sendMessage(roomId, req.user!.userId, req.user!.email, content);
    res.json({ message });
  } catch (error) {
    next(error);
  }
});

// POST /api/chat/rooms/:roomId/read — mark room as read
chatRoutes.post("/chat/rooms/:roomId/read", async (req, res, next) => {
  try {
    const roomId = Number(req.params.roomId);
    await markRoomRead(roomId, req.user!.userId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// GET /api/chat/unread — lightweight unread counts for badge
chatRoutes.get("/chat/unread", async (req, res, next) => {
  try {
    const counts = await getUnreadCounts(req.user!.userId);
    res.json({ counts });
  } catch (error) {
    next(error);
  }
});

// GET /api/chat/users — all active users for DM picker
chatRoutes.get("/chat/users", async (req, res, next) => {
  try {
    const users = await listChatUsers();
    res.json({ users });
  } catch (error) {
    next(error);
  }
});
