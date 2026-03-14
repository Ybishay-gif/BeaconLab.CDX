import { randomUUID } from "node:crypto";
import { query, table } from "../db/index.js";

export type TicketStatus =
  | "todo"
  | "pending_spec"
  | "pending_spec_approval"
  | "spec_approved"
  | "adjusted_spec"
  | "pending_deployment"
  | "deployment_approved"
  | "deployed"
  | "done"
  | "reopened";

export type TicketComplexity = "low" | "medium" | "high" | "critical";

/** Valid next-status transitions per current status */
export const STATUS_TRANSITIONS: Record<TicketStatus, TicketStatus[]> = {
  todo: ["pending_spec"],
  pending_spec: ["pending_spec_approval"],
  pending_spec_approval: ["spec_approved", "adjusted_spec"],
  spec_approved: ["pending_deployment"],
  adjusted_spec: ["pending_spec_approval"],
  pending_deployment: ["deployment_approved", "adjusted_spec"],
  deployment_approved: ["deployed"],
  deployed: ["done", "reopened"],
  done: [],
  reopened: ["pending_spec_approval"],
};

export type TicketRow = {
  ticket_id: string;
  ticket_number: number;
  type: "bug" | "feature";
  status: TicketStatus;
  title: string;
  description: string;
  module: string;
  page: string;
  attachments: string;
  created_by: string;
  created_by_email: string;
  assigned_to: string | null;
  resolved_at: string | null;
  resolution_notes: string | null;
  // Legacy fields (kept for backward compat)
  test_results: string | null;
  documentation: string;
  test_checklist: string;
  // Spec phase
  complexity: TicketComplexity;
  functional_spec: string | null;
  design_notes: string | null;
  ui_mockup: string | null;
  testing_scenarios: string;
  // Dev phase
  dev_summary: string | null;
  code_changes: string;
  dev_test_results: string | null;
  dev_evidence: string;
  // Deploy phase
  deploy_info: string | null;
  prod_test_results: string | null;
  prod_evidence: string;
  created_at: string;
  updated_at: string;
};

export type Attachment = {
  filename: string;
  mimeType: string;
  sizeBytes: number;
  data: string;
};

export type CreateTicketInput = {
  type: "bug" | "feature";
  title: string;
  description: string;
  module: string;
  page: string;
  attachments?: Attachment[];
};

export type TestCheckItem = {
  scenario: string;
  passed: boolean;
};

export type TestingScenario = {
  scenario: string;
  expected: string;
};

export type CodeChange = {
  file: string;
  description: string;
};

export type UpdateTicketInput = {
  status?: TicketStatus;
  resolution_notes?: string;
  // Legacy
  test_results?: string;
  documentation?: Attachment[];
  test_checklist?: TestCheckItem[];
  assigned_to?: string | null;
  // Spec phase
  complexity?: TicketComplexity;
  functional_spec?: string;
  design_notes?: string;
  ui_mockup?: Attachment | null;
  testing_scenarios?: TestingScenario[];
  // Dev phase
  dev_summary?: string;
  code_changes?: CodeChange[];
  dev_test_results?: string;
  dev_evidence?: Attachment[];
  // Deploy phase
  deploy_info?: string;
  prod_test_results?: string;
  prod_evidence?: Attachment[];
};

export type TicketFilters = {
  status?: string;
  type?: string;
  module?: string;
  createdBy?: string;
  limit?: number;
};

export type ActivityLogRow = {
  log_id: string;
  ticket_id: string;
  action: string;
  old_value: string | null;
  new_value: string | null;
  details: string | null;
  user_id: string;
  user_email: string;
  created_at: string;
};

export type CommentRow = {
  comment_id: string;
  ticket_id: string;
  user_id: string;
  user_email: string;
  body: string;
  created_at: string;
  updated_at: string;
};

// ── CRUD ──────────────────────────────────────────

export async function createTicket(
  user: { userId: string; email: string },
  input: CreateTicketInput
): Promise<{ ticketId: string; ticketNumber: number }> {
  const ticketId = randomUUID();
  const rows = await query<{ ticket_number: number }>(
    `
      INSERT INTO ${table("tickets")} (
        ticket_id, type, title, description, module, page,
        attachments, created_by, created_by_email, created_at, updated_at
      )
      VALUES (
        @ticketId, @type, @title, @description, @module, @page,
        @attachments, @userId, @email, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
      )
      RETURNING ticket_number
    `,
    {
      ticketId,
      type: input.type,
      title: input.title,
      description: input.description || "",
      module: input.module,
      page: input.page,
      attachments: JSON.stringify(input.attachments ?? []),
      userId: user.userId,
      email: user.email,
    }
  );

  // Log creation
  await addActivityLog(ticketId, user, "ticket_created", null, "todo", `Created ${input.type}: ${input.title}`);

  return { ticketId, ticketNumber: rows[0]?.ticket_number ?? 0 };
}

const LIST_SELECT_COLS = `
  ticket_id, ticket_number, type, status, title, description,
  module, page,
  COALESCE(
    (SELECT jsonb_agg(jsonb_build_object(
      'filename', a->>'filename',
      'mimeType', a->>'mimeType',
      'sizeBytes', (a->>'sizeBytes')::int
    )) FROM jsonb_array_elements(attachments) a),
    '[]'::jsonb
  ) AS attachments,
  created_by, created_by_email, assigned_to,
  resolved_at::text AS resolved_at,
  resolution_notes, test_results,
  COALESCE(
    (SELECT jsonb_agg(jsonb_build_object(
      'filename', d->>'filename',
      'mimeType', d->>'mimeType',
      'sizeBytes', (d->>'sizeBytes')::int
    )) FROM jsonb_array_elements(COALESCE(documentation, '[]'::jsonb)) d),
    '[]'::jsonb
  ) AS documentation,
  COALESCE(test_checklist, '[]'::jsonb) AS test_checklist,
  COALESCE(complexity, 'medium') AS complexity,
  functional_spec, design_notes, ui_mockup,
  COALESCE(testing_scenarios, '[]'::jsonb) AS testing_scenarios,
  dev_summary,
  COALESCE(code_changes, '[]'::jsonb) AS code_changes,
  dev_test_results,
  COALESCE(
    (SELECT jsonb_agg(jsonb_build_object(
      'filename', e->>'filename',
      'mimeType', e->>'mimeType',
      'sizeBytes', (e->>'sizeBytes')::int
    )) FROM jsonb_array_elements(COALESCE(dev_evidence, '[]'::jsonb)) e),
    '[]'::jsonb
  ) AS dev_evidence,
  deploy_info, prod_test_results,
  COALESCE(
    (SELECT jsonb_agg(jsonb_build_object(
      'filename', p->>'filename',
      'mimeType', p->>'mimeType',
      'sizeBytes', (p->>'sizeBytes')::int
    )) FROM jsonb_array_elements(COALESCE(prod_evidence, '[]'::jsonb)) p),
    '[]'::jsonb
  ) AS prod_evidence,
  created_at::text AS created_at,
  updated_at::text AS updated_at
`;

export async function listTickets(filters: TicketFilters = {}): Promise<TicketRow[]> {
  const normalizedLimit = Math.max(1, Math.min(500, Math.floor(Number(filters.limit) || 200)));
  const conditions: string[] = [];
  const params: Record<string, unknown> = { limit: normalizedLimit };

  if (filters.status) {
    conditions.push("status = @status");
    params.status = filters.status;
  }
  if (filters.type) {
    conditions.push("type = @type");
    params.type = filters.type;
  }
  if (filters.module) {
    conditions.push("module = @module");
    params.module = filters.module;
  }
  if (filters.createdBy) {
    conditions.push("created_by = @createdBy");
    params.createdBy = filters.createdBy;
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  return query<TicketRow>(
    `SELECT ${LIST_SELECT_COLS} FROM ${table("tickets")} ${whereClause} ORDER BY created_at DESC LIMIT @limit`,
    params
  );
}

export async function getTicket(ticketId: string): Promise<TicketRow | null> {
  const rows = await query<TicketRow>(
    `
      SELECT
        ticket_id, ticket_number, type, status, title, description,
        module, page, attachments,
        created_by, created_by_email, assigned_to,
        resolved_at::text AS resolved_at,
        resolution_notes, test_results,
        COALESCE(documentation, '[]'::jsonb) AS documentation,
        COALESCE(test_checklist, '[]'::jsonb) AS test_checklist,
        COALESCE(complexity, 'medium') AS complexity,
        functional_spec, design_notes, ui_mockup,
        COALESCE(testing_scenarios, '[]'::jsonb) AS testing_scenarios,
        dev_summary,
        COALESCE(code_changes, '[]'::jsonb) AS code_changes,
        dev_test_results,
        COALESCE(dev_evidence, '[]'::jsonb) AS dev_evidence,
        deploy_info, prod_test_results,
        COALESCE(prod_evidence, '[]'::jsonb) AS prod_evidence,
        created_at::text AS created_at,
        updated_at::text AS updated_at
      FROM ${table("tickets")}
      WHERE ticket_id = @ticketId
    `,
    { ticketId }
  );
  return rows[0] ?? null;
}

export async function updateTicket(
  ticketId: string,
  input: UpdateTicketInput,
  user?: { userId: string; email: string }
): Promise<void> {
  const sets: string[] = ["updated_at = CURRENT_TIMESTAMP()"];
  const params: Record<string, unknown> = { ticketId };

  // Status transition validation + activity logging
  if (input.status !== undefined && user) {
    const current = await query<{ status: TicketStatus }>(
      `SELECT status FROM ${table("tickets")} WHERE ticket_id = @ticketId`,
      { ticketId }
    );
    const currentStatus = current[0]?.status;
    if (currentStatus) {
      const allowed = STATUS_TRANSITIONS[currentStatus] ?? [];
      if (!allowed.includes(input.status)) {
        throw new Error(`Invalid transition: ${currentStatus} → ${input.status}`);
      }
      await addActivityLog(ticketId, user, "status_change", currentStatus, input.status);
    }
  }

  if (input.status !== undefined) {
    sets.push("status = @status");
    params.status = input.status;
    if (input.status === "deployed") {
      sets.push("resolved_at = CURRENT_TIMESTAMP()");
    }
  }
  if (input.resolution_notes !== undefined) {
    sets.push("resolution_notes = @resolutionNotes");
    params.resolutionNotes = input.resolution_notes;
  }
  if (input.test_results !== undefined) {
    sets.push("test_results = @testResults");
    params.testResults = input.test_results;
  }
  if (input.documentation !== undefined) {
    sets.push("documentation = @documentation");
    params.documentation = JSON.stringify(input.documentation);
  }
  if (input.test_checklist !== undefined) {
    sets.push("test_checklist = @testChecklist");
    params.testChecklist = JSON.stringify(input.test_checklist);
  }
  if (input.assigned_to !== undefined) {
    sets.push("assigned_to = @assignedTo");
    params.assignedTo = input.assigned_to;
  }
  // Spec phase
  if (input.complexity !== undefined) {
    sets.push("complexity = @complexity");
    params.complexity = input.complexity;
  }
  if (input.functional_spec !== undefined) {
    sets.push("functional_spec = @functionalSpec");
    params.functionalSpec = input.functional_spec;
  }
  if (input.design_notes !== undefined) {
    sets.push("design_notes = @designNotes");
    params.designNotes = input.design_notes;
  }
  if (input.ui_mockup !== undefined) {
    sets.push("ui_mockup = @uiMockup");
    params.uiMockup = input.ui_mockup ? JSON.stringify(input.ui_mockup) : null;
  }
  if (input.testing_scenarios !== undefined) {
    sets.push("testing_scenarios = @testingScenarios");
    params.testingScenarios = JSON.stringify(input.testing_scenarios);
  }
  // Dev phase
  if (input.dev_summary !== undefined) {
    sets.push("dev_summary = @devSummary");
    params.devSummary = input.dev_summary;
  }
  if (input.code_changes !== undefined) {
    sets.push("code_changes = @codeChanges");
    params.codeChanges = JSON.stringify(input.code_changes);
  }
  if (input.dev_test_results !== undefined) {
    sets.push("dev_test_results = @devTestResults");
    params.devTestResults = input.dev_test_results;
  }
  if (input.dev_evidence !== undefined) {
    sets.push("dev_evidence = @devEvidence");
    params.devEvidence = JSON.stringify(input.dev_evidence);
  }
  // Deploy phase
  if (input.deploy_info !== undefined) {
    sets.push("deploy_info = @deployInfo");
    params.deployInfo = input.deploy_info;
  }
  if (input.prod_test_results !== undefined) {
    sets.push("prod_test_results = @prodTestResults");
    params.prodTestResults = input.prod_test_results;
  }
  if (input.prod_evidence !== undefined) {
    sets.push("prod_evidence = @prodEvidence");
    params.prodEvidence = JSON.stringify(input.prod_evidence);
  }

  await query(
    `UPDATE ${table("tickets")} SET ${sets.join(", ")} WHERE ticket_id = @ticketId`,
    params
  );
}

export async function deleteTicket(ticketId: string): Promise<void> {
  await query(`DELETE FROM ${table("tickets")} WHERE ticket_id = @ticketId`, { ticketId });
}

// ── Activity Log ──────────────────────────────────

export async function addActivityLog(
  ticketId: string,
  user: { userId: string; email: string },
  action: string,
  oldValue?: string | null,
  newValue?: string | null,
  details?: string
): Promise<void> {
  await query(
    `
      INSERT INTO ${table("ticket_activity_log")} (
        ticket_id, action, old_value, new_value, details,
        user_id, user_email, created_at
      )
      VALUES (
        @ticketId, @action, @oldValue, @newValue, @details,
        @userId, @userEmail, CURRENT_TIMESTAMP()
      )
    `,
    {
      ticketId,
      action,
      oldValue: oldValue ?? null,
      newValue: newValue ?? null,
      details: details ?? null,
      userId: user.userId,
      userEmail: user.email,
    }
  );
}

export async function getActivityLog(ticketId: string): Promise<ActivityLogRow[]> {
  return query<ActivityLogRow>(
    `
      SELECT log_id, ticket_id, action, old_value, new_value, details,
             user_id, user_email, created_at::text AS created_at
      FROM ${table("ticket_activity_log")}
      WHERE ticket_id = @ticketId
      ORDER BY created_at ASC
    `,
    { ticketId }
  );
}

// ── Comments ──────────────────────────────────────

export async function addComment(
  ticketId: string,
  user: { userId: string; email: string },
  body: string
): Promise<{ commentId: string }> {
  const commentId = randomUUID();
  await query(
    `
      INSERT INTO ${table("ticket_comments")} (
        comment_id, ticket_id, user_id, user_email, body,
        created_at, updated_at
      )
      VALUES (
        @commentId, @ticketId, @userId, @userEmail, @body,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
      )
    `,
    {
      commentId,
      ticketId,
      userId: user.userId,
      userEmail: user.email,
      body,
    }
  );

  await addActivityLog(ticketId, user, "comment_added", null, null, body.slice(0, 200));

  return { commentId };
}

export async function listComments(ticketId: string): Promise<CommentRow[]> {
  return query<CommentRow>(
    `
      SELECT comment_id, ticket_id, user_id, user_email, body,
             created_at::text AS created_at, updated_at::text AS updated_at
      FROM ${table("ticket_comments")}
      WHERE ticket_id = @ticketId
      ORDER BY created_at ASC
    `,
    { ticketId }
  );
}

export async function deleteComment(commentId: string): Promise<void> {
  await query(
    `DELETE FROM ${table("ticket_comments")} WHERE comment_id = @commentId`,
    { commentId }
  );
}
