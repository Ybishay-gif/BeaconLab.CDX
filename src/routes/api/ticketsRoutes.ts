import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import {
  createTicket,
  listTickets,
  getTicket,
  updateTicket,
  deleteTicket,
} from "../../services/ticketsService.js";

const MAX_ATTACHMENT_SIZE = 3 * 1024 * 1024; // 3MB

const attachmentSchema = z.object({
  filename: z.string().min(1).max(255),
  mimeType: z.string().min(1),
  sizeBytes: z.number().max(MAX_ATTACHMENT_SIZE),
  data: z.string(),
});

const createTicketSchema = z.object({
  type: z.enum(["bug", "feature"]),
  title: z.string().min(1).max(200),
  description: z.string().max(5000).default(""),
  module: z.string().min(1),
  page: z.string().min(1),
  attachments: z.array(attachmentSchema).max(3).optional(),
});

const updateTicketSchema = z.object({
  status: z.enum(["todo", "approved", "coded", "pending_review", "deploy_approved", "deployed"]).optional(),
  resolution_notes: z.string().max(2000).optional(),
  test_results: z.string().max(50000).optional(),
  documentation: z.array(attachmentSchema).max(10).optional(),
  assigned_to: z.string().nullable().optional(),
});

export const ticketsRoutes = Router();

// List tickets
ticketsRoutes.get("/tickets", async (req, res, next) => {
  try {
    const status = typeof req.query.status === "string" ? req.query.status.trim() : undefined;
    const type = typeof req.query.type === "string" ? req.query.type.trim() : undefined;
    const module = typeof req.query.module === "string" ? req.query.module.trim() : undefined;
    const rawLimit = typeof req.query.limit === "string" ? Number(req.query.limit.trim()) : 200;

    const tickets = await listTickets({
      status: status || undefined,
      type: type || undefined,
      module: module || undefined,
      limit: Number.isFinite(rawLimit) ? rawLimit : 200,
    });
    res.json({ tickets });
  } catch (error) {
    next(error);
  }
});

// Get single ticket (full attachments)
ticketsRoutes.get("/tickets/:ticketId", async (req, res, next) => {
  try {
    const ticket = await getTicket(req.params.ticketId);
    if (!ticket) return res.status(404).json({ error: "Ticket not found" });
    res.json({ ticket });
  } catch (error) {
    next(error);
  }
});

// Create ticket
ticketsRoutes.post("/tickets", requirePermission("tickets:add"), async (req, res, next) => {
  try {
    const parsed = createTicketSchema.parse(req.body);
    const result = await createTicket(
      { userId: req.user!.userId, email: req.user!.email },
      parsed
    );
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

// Update ticket (admin can update any; others can update their own)
ticketsRoutes.put("/tickets/:ticketId", async (req, res, next) => {
  try {
    const parsed = updateTicketSchema.parse(req.body);
    const ticket = await getTicket(req.params.ticketId);
    if (!ticket) return res.status(404).json({ error: "Ticket not found" });

    // Check permission based on status change
    const perms = req.user!.permissions ?? [];
    if (parsed.status === "approved" && !perms.includes("tickets:approve")) {
      return res.status(403).json({ error: "Insufficient permissions to approve tickets" });
    }
    if (parsed.status === "deploy_approved" && !perms.includes("tickets:deploy_approve")) {
      return res.status(403).json({ error: "Insufficient permissions to mark tickets as deploy approved" });
    }
    // Allow owner or anyone with tickets:approve to update other fields
    if (ticket.created_by !== req.user!.userId && !perms.includes("tickets:approve")) {
      return res.status(403).json({ error: "Not authorized to update this ticket" });
    }

    await updateTicket(req.params.ticketId, parsed);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// Delete ticket (admin only)
ticketsRoutes.delete("/tickets/:ticketId", requirePermission("tickets:approve"), async (req, res, next) => {
  try {
    await deleteTicket(req.params.ticketId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});
