import { randomUUID } from "node:crypto";
import { query, table } from "../db/index.js";

export type TicketType = "bug" | "feature" | "module" | "user_story";

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
  | "reopened"
  // Module statuses
  | "start_planning"
  | "pending_product_review"
  | "continue_planning"
  | "plan_approved"
  | "pending_detailed_plan_approval"
  | "detailed_plan_approved"
  // User Story statuses
  | "feature_list_completed";

export type TicketComplexity = "low" | "medium" | "high" | "critical";

// ── Status Transitions per ticket type ────────────

/** Feature/Bug status transitions (original workflow) */
export const FEATURE_STATUS_TRANSITIONS: Record<string, TicketStatus[]> = {
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

/** Module status transitions */
export const MODULE_STATUS_TRANSITIONS: Record<string, TicketStatus[]> = {
  todo: ["start_planning"],
  start_planning: ["pending_product_review"],
  pending_product_review: ["continue_planning", "plan_approved"],
  continue_planning: ["pending_product_review"],
  plan_approved: ["pending_detailed_plan_approval"],
  pending_detailed_plan_approval: ["detailed_plan_approved"],
  detailed_plan_approved: [],
};

/** User Story status transitions */
export const STORY_STATUS_TRANSITIONS: Record<string, TicketStatus[]> = {
  todo: ["pending_spec"],
  pending_spec: ["pending_spec_approval"],
  pending_spec_approval: ["spec_approved", "adjusted_spec"],
  adjusted_spec: ["pending_spec_approval"],
  spec_approved: ["feature_list_completed"],
  feature_list_completed: [],
};

/** Legacy alias — selects correct map based on ticket type */
export const STATUS_TRANSITIONS = FEATURE_STATUS_TRANSITIONS;

/** Get the transitions map for a given ticket type */
export function getTransitionsForType(type: TicketType): Record<string, TicketStatus[]> {
  switch (type) {
    case "module":
      return MODULE_STATUS_TRANSITIONS;
    case "user_story":
      return STORY_STATUS_TRANSITIONS;
    default:
      return FEATURE_STATUS_TRANSITIONS;
  }
}

export type TicketRow = {
  ticket_id: string;
  ticket_number: number;
  type: TicketType;
  status: TicketStatus;
  parent_id: string | null;
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
  // Hierarchy fields
  module_requirements: string | null;
  story_details: string | null;
  planning_qa: string | null;
  plan_content: string | null;
  registry_module_id: string | null;
  created_at: string;
  updated_at: string;
};

export type Attachment = {
  filename: string;
  mimeType: string;
  sizeBytes: number;
  data: string;
};

export type ModuleRequirements = {
  requirements?: string;
  data_sources?: string;
  goals?: string;
  user_workflows?: string;
  technical_requirements?: string;
  business_impact?: string;
  strategy_alignment?: string;
};

export type StoryDetails = {
  user_expects?: string;
  why?: string;
  technical_requirements?: string;
  permissions_needed?: string;
  // Claude-generated spec fields
  business_spec?: string;
  user_spec?: string;
  product_impact?: string;
  product_design?: string;
  tech_design?: string;
};

export type PlanningQA = {
  question: string;
  answer: string;
}[];

export type CreateTicketInput = {
  type: TicketType;
  title: string;
  description: string;
  module: string;
  page: string;
  parent_id?: string | null;
  attachments?: Attachment[];
  module_requirements?: ModuleRequirements;
  story_details?: StoryDetails;
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
  // Hierarchy fields
  module_requirements?: ModuleRequirements;
  story_details?: StoryDetails;
  planning_qa?: PlanningQA;
  plan_content?: string;
  registry_module_id?: string;
};

export type TicketFilters = {
  status?: string;
  type?: string;
  module?: string;
  createdBy?: string;
  parentId?: string;
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
        parent_id, attachments, module_requirements, story_details,
        created_by, created_by_email, created_at, updated_at
      )
      VALUES (
        @ticketId, @type, @title, @description, @module, @page,
        @parentId, @attachments, @moduleRequirements, @storyDetails,
        @userId, @email, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
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
      parentId: input.parent_id ?? null,
      attachments: JSON.stringify(input.attachments ?? []),
      moduleRequirements: input.module_requirements ? JSON.stringify(input.module_requirements) : null,
      storyDetails: input.story_details ? JSON.stringify(input.story_details) : null,
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
  module, page, parent_id,
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
  module_requirements, story_details, planning_qa, plan_content, registry_module_id,
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
  if (filters.parentId) {
    conditions.push("parent_id = @parentId");
    params.parentId = filters.parentId;
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
        module, page, parent_id, attachments,
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
        module_requirements, story_details, planning_qa, plan_content, registry_module_id,
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
    const current = await query<{ status: TicketStatus; type: TicketType }>(
      `SELECT status, type FROM ${table("tickets")} WHERE ticket_id = @ticketId`,
      { ticketId }
    );
    const currentStatus = current[0]?.status;
    const ticketType = current[0]?.type ?? "feature";
    if (currentStatus) {
      const transitions = getTransitionsForType(ticketType);
      const allowed = transitions[currentStatus] ?? [];
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
  // Hierarchy fields
  if (input.module_requirements !== undefined) {
    sets.push("module_requirements = @moduleRequirements");
    params.moduleRequirements = JSON.stringify(input.module_requirements);
  }
  if (input.story_details !== undefined) {
    sets.push("story_details = @storyDetails");
    params.storyDetails = JSON.stringify(input.story_details);
  }
  if (input.planning_qa !== undefined) {
    sets.push("planning_qa = @planningQa");
    params.planningQa = JSON.stringify(input.planning_qa);
  }
  if (input.plan_content !== undefined) {
    sets.push("plan_content = @planContent");
    params.planContent = input.plan_content;
  }
  if (input.registry_module_id !== undefined) {
    sets.push("registry_module_id = @registryModuleId");
    params.registryModuleId = input.registry_module_id;
  }

  await query(
    `UPDATE ${table("tickets")} SET ${sets.join(", ")} WHERE ticket_id = @ticketId`,
    params
  );
}

export async function deleteTicket(ticketId: string): Promise<void> {
  await query(`DELETE FROM ${table("tickets")} WHERE ticket_id = @ticketId`, { ticketId });
}

// ── Hierarchy queries ─────────────────────────────

export async function listChildTickets(parentId: string): Promise<TicketRow[]> {
  return query<TicketRow>(
    `SELECT ${LIST_SELECT_COLS} FROM ${table("tickets")} WHERE parent_id = @parentId ORDER BY ticket_number ASC`,
    { parentId }
  );
}

export type TicketTreeNode = TicketRow & { children: TicketTreeNode[] };

/** Fetch all tickets and build a tree: modules → user stories → features/bugs */
export async function getTicketTree(filters: TicketFilters = {}): Promise<TicketTreeNode[]> {
  // Fetch all tickets (limited but enough for tree)
  const all = await listTickets({ ...filters, limit: 500 });

  const byId = new Map<string, TicketTreeNode>();
  for (const t of all) {
    byId.set(t.ticket_id, { ...t, children: [] });
  }

  const roots: TicketTreeNode[] = [];
  for (const node of byId.values()) {
    if (node.parent_id && byId.has(node.parent_id)) {
      byId.get(node.parent_id)!.children.push(node);
    } else {
      roots.push(node);
    }
  }

  // Sort: modules first, then stories, then features/bugs
  const typeOrder: Record<string, number> = { module: 0, user_story: 1, feature: 2, bug: 3 };
  const sortNodes = (nodes: TicketTreeNode[]) => {
    nodes.sort((a, b) => (typeOrder[a.type] ?? 9) - (typeOrder[b.type] ?? 9) || a.ticket_number - b.ticket_number);
    for (const n of nodes) sortNodes(n.children);
  };
  sortNodes(roots);

  return roots;
}

/** Get a ticket plus its full ancestor chain (for breadcrumbs) */
export async function getTicketWithAncestors(ticketId: string): Promise<TicketRow[]> {
  const chain: TicketRow[] = [];
  let currentId: string | null = ticketId;
  while (currentId) {
    const ticket = await getTicket(currentId);
    if (!ticket) break;
    chain.unshift(ticket); // prepend so order is root → leaf
    currentId = ticket.parent_id;
  }
  return chain;
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
