import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import {
  createTicket,
  listTickets,
  getTicket,
  updateTicket,
  deleteTicket,
  getActivityLog,
  addComment,
  listComments,
  deleteComment as deleteCommentSvc,
  listChildTickets,
  getTicketTree,
  getTicketWithAncestors,
} from "../../services/ticketsService.js";

const MAX_ATTACHMENT_SIZE = 3 * 1024 * 1024; // 3MB

const attachmentSchema = z.object({
  filename: z.string().min(1).max(255),
  mimeType: z.string().min(1),
  sizeBytes: z.number().max(MAX_ATTACHMENT_SIZE),
  data: z.string(),
});

const moduleRequirementsSchema = z.object({
  requirements: z.string().max(10000).optional(),
  data_sources: z.string().max(5000).optional(),
  goals: z.string().max(5000).optional(),
  user_workflows: z.string().max(5000).optional(),
  technical_requirements: z.string().max(5000).optional(),
  business_impact: z.string().max(5000).optional(),
  strategy_alignment: z.string().max(5000).optional(),
}).optional();

const storyDetailsSchema = z.object({
  user_expects: z.string().max(5000).optional(),
  why: z.string().max(5000).optional(),
  technical_requirements: z.string().max(5000).optional(),
  permissions_needed: z.string().max(5000).optional(),
  business_spec: z.string().max(50000).optional(),
  user_spec: z.string().max(50000).optional(),
  product_impact: z.string().max(50000).optional(),
  product_design: z.string().max(50000).optional(),
  tech_design: z.string().max(50000).optional(),
}).optional();

const planningQaSchema = z.array(z.object({
  question: z.string().max(2000),
  answer: z.string().max(5000),
})).max(50).optional();

const createTicketSchema = z.object({
  type: z.enum(["bug", "feature", "module", "user_story"]),
  title: z.string().min(1).max(200),
  description: z.string().max(5000).default(""),
  module: z.string().min(1),
  page: z.string().default("N/A"),
  parent_id: z.string().nullable().optional(),
  attachments: z.array(attachmentSchema).max(3).optional(),
  module_requirements: moduleRequirementsSchema,
  story_details: storyDetailsSchema,
});

const testCheckItemSchema = z.object({
  scenario: z.string().min(1).max(500),
  passed: z.boolean(),
});

const testingScenarioSchema = z.object({
  scenario: z.string().min(1).max(1000),
  expected: z.string().min(1).max(1000),
});

const codeChangeSchema = z.object({
  file: z.string().min(1).max(500),
  description: z.string().min(1).max(1000),
});

const VALID_STATUSES = [
  "todo",
  "pending_spec",
  "pending_spec_approval",
  "spec_approved",
  "adjusted_spec",
  "pending_deployment",
  "deployment_approved",
  "deployed",
  "done",
  "reopened",
  // Module statuses
  "start_planning",
  "pending_product_review",
  "continue_planning",
  "plan_approved",
  "pending_detailed_plan_approval",
  "detailed_plan_approved",
  // User Story statuses
  "feature_list_completed",
] as const;

const updateTicketSchema = z.object({
  status: z.enum(VALID_STATUSES).optional(),
  resolution_notes: z.string().max(2000).optional(),
  assigned_to: z.string().nullable().optional(),
  // Legacy
  test_results: z.string().max(50000).optional(),
  documentation: z.array(attachmentSchema).max(10).optional(),
  test_checklist: z.array(testCheckItemSchema).max(50).optional(),
  // Spec phase
  complexity: z.enum(["low", "medium", "high", "critical"]).optional(),
  functional_spec: z.string().max(50000).optional(),
  design_notes: z.string().max(10000).optional(),
  ui_mockup: attachmentSchema.nullable().optional(),
  testing_scenarios: z.array(testingScenarioSchema).max(50).optional(),
  // Dev phase
  dev_summary: z.string().max(10000).optional(),
  code_changes: z.array(codeChangeSchema).max(100).optional(),
  dev_test_results: z.string().max(50000).optional(),
  dev_evidence: z.array(attachmentSchema).max(20).optional(),
  // Deploy phase
  deploy_info: z.string().max(10000).optional(),
  prod_test_results: z.string().max(50000).optional(),
  prod_evidence: z.array(attachmentSchema).max(20).optional(),
  // Hierarchy fields
  module_requirements: moduleRequirementsSchema,
  story_details: storyDetailsSchema,
  planning_qa: planningQaSchema,
  plan_content: z.string().max(100000).optional(),
  registry_module_id: z.string().max(100).optional(),
});

const createCommentSchema = z.object({
  body: z.string().min(1).max(5000),
});

export const ticketsRoutes = Router();

// List tickets
ticketsRoutes.get("/tickets", async (req, res, next) => {
  try {
    const status = typeof req.query.status === "string" ? req.query.status.trim() : undefined;
    const type = typeof req.query.type === "string" ? req.query.type.trim() : undefined;
    const module = typeof req.query.module === "string" ? req.query.module.trim() : undefined;
    const parentId = typeof req.query.parent_id === "string" ? req.query.parent_id.trim() : undefined;
    const rawLimit = typeof req.query.limit === "string" ? Number(req.query.limit.trim()) : 200;

    const tickets = await listTickets({
      status: status || undefined,
      type: type || undefined,
      module: module || undefined,
      parentId: parentId || undefined,
      limit: Number.isFinite(rawLimit) ? rawLimit : 200,
    });
    res.json({ tickets });
  } catch (error) {
    next(error);
  }
});

// Get ticket tree (hierarchical view)
ticketsRoutes.get("/tickets/tree", async (req, res, next) => {
  try {
    const tree = await getTicketTree();
    res.json({ tree });
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

// Get child tickets
ticketsRoutes.get("/tickets/:ticketId/children", async (req, res, next) => {
  try {
    const children = await listChildTickets(req.params.ticketId);
    res.json({ tickets: children });
  } catch (error) {
    next(error);
  }
});

// Get ticket with ancestor chain (for breadcrumbs)
ticketsRoutes.get("/tickets/:ticketId/ancestors", async (req, res, next) => {
  try {
    const chain = await getTicketWithAncestors(req.params.ticketId);
    res.json({ ancestors: chain });
  } catch (error) {
    next(error);
  }
});

// Create ticket
ticketsRoutes.post("/tickets", requirePermission("tickets:add"), async (req, res, next) => {
  try {
    const parsed = createTicketSchema.parse(req.body);
    const perms = req.user!.permissions ?? [];

    // Module tickets require tickets:create_module permission
    if (parsed.type === "module" && !perms.includes("tickets:create_module") && !perms.includes("tickets:approve")) {
      return res.status(403).json({ error: "Insufficient permissions to create module requests" });
    }

    // Validate parent_id hierarchy
    if (parsed.type === "module" && parsed.parent_id) {
      return res.status(400).json({ error: "Module tickets cannot have a parent" });
    }
    if (parsed.type === "user_story") {
      if (!parsed.parent_id) {
        return res.status(400).json({ error: "User story must have a parent module ticket" });
      }
      const parent = await getTicket(parsed.parent_id);
      if (!parent || parent.type !== "module") {
        return res.status(400).json({ error: "User story parent must be a module ticket" });
      }
    }
    if ((parsed.type === "feature" || parsed.type === "bug") && parsed.parent_id) {
      const parent = await getTicket(parsed.parent_id);
      if (!parent || parent.type !== "user_story") {
        return res.status(400).json({ error: "Feature/bug parent must be a user story ticket" });
      }
    }

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

    const perms = req.user!.permissions ?? [];
    const user = { userId: req.user!.userId, email: req.user!.email };

    // Permission checks for status transitions
    if (parsed.status) {
      const needsApprove = ["pending_spec", "spec_approved", "adjusted_spec"];
      if (needsApprove.includes(parsed.status) && !perms.includes("tickets:approve")) {
        return res.status(403).json({ error: "Insufficient permissions" });
      }
      if (parsed.status === "deployment_approved" && !perms.includes("tickets:deploy_approve")) {
        return res.status(403).json({ error: "Insufficient permissions to approve deployment" });
      }
    }

    // Allow owner or anyone with tickets:approve to update
    if (ticket.created_by !== req.user!.userId && !perms.includes("tickets:approve")) {
      return res.status(403).json({ error: "Not authorized to update this ticket" });
    }

    await updateTicket(req.params.ticketId, parsed, user);
    res.json({ ok: true });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Invalid transition")) {
      return res.status(400).json({ error: error.message });
    }
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

// ── Activity Log ──────────────────────────────────

ticketsRoutes.get("/tickets/:ticketId/activity", async (req, res, next) => {
  try {
    const activity = await getActivityLog(req.params.ticketId);
    res.json({ activity });
  } catch (error) {
    next(error);
  }
});

// ── Comments ──────────────────────────────────────

ticketsRoutes.get("/tickets/:ticketId/comments", async (req, res, next) => {
  try {
    const comments = await listComments(req.params.ticketId);
    res.json({ comments });
  } catch (error) {
    next(error);
  }
});

ticketsRoutes.post("/tickets/:ticketId/comments", async (req, res, next) => {
  try {
    const parsed = createCommentSchema.parse(req.body);
    const ticket = await getTicket(req.params.ticketId);
    if (!ticket) return res.status(404).json({ error: "Ticket not found" });

    const result = await addComment(
      req.params.ticketId,
      { userId: req.user!.userId, email: req.user!.email },
      parsed.body
    );
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

ticketsRoutes.delete("/tickets/:ticketId/comments/:commentId", async (req, res, next) => {
  try {
    const perms = req.user!.permissions ?? [];
    // Only comment owner or admin can delete
    if (!perms.includes("tickets:approve")) {
      const comments = await listComments(req.params.ticketId);
      const comment = comments.find((c) => c.comment_id === req.params.commentId);
      if (!comment || comment.user_id !== req.user!.userId) {
        return res.status(403).json({ error: "Not authorized to delete this comment" });
      }
    }
    await deleteCommentSvc(req.params.commentId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});
