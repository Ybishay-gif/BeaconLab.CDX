import { randomUUID } from "node:crypto";
import { query, table } from "../db/index.js";

export type TicketRow = {
  ticket_id: string;
  ticket_number: number;
  type: "bug" | "feature";
  status: "todo" | "in_progress" | "done";
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

export type UpdateTicketInput = {
  status?: "todo" | "in_progress" | "done";
  resolution_notes?: string;
  assigned_to?: string | null;
};

export type TicketFilters = {
  status?: string;
  type?: string;
  module?: string;
  createdBy?: string;
  limit?: number;
};

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
  return { ticketId, ticketNumber: rows[0]?.ticket_number ?? 0 };
}

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

  // Strip attachment data from list response (return metadata only)
  return query<TicketRow>(
    `
      SELECT
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
        resolution_notes,
        created_at::text AS created_at,
        updated_at::text AS updated_at
      FROM ${table("tickets")}
      ${whereClause}
      ORDER BY created_at DESC
      LIMIT @limit
    `,
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
        resolution_notes,
        created_at::text AS created_at,
        updated_at::text AS updated_at
      FROM ${table("tickets")}
      WHERE ticket_id = @ticketId
    `,
    { ticketId }
  );
  return rows[0] ?? null;
}

export async function updateTicket(ticketId: string, input: UpdateTicketInput): Promise<void> {
  const sets: string[] = ["updated_at = CURRENT_TIMESTAMP()"];
  const params: Record<string, unknown> = { ticketId };

  if (input.status !== undefined) {
    sets.push("status = @status");
    params.status = input.status;
    if (input.status === "done") {
      sets.push("resolved_at = CURRENT_TIMESTAMP()");
    }
  }
  if (input.resolution_notes !== undefined) {
    sets.push("resolution_notes = @resolutionNotes");
    params.resolutionNotes = input.resolution_notes;
  }
  if (input.assigned_to !== undefined) {
    sets.push("assigned_to = @assignedTo");
    params.assignedTo = input.assigned_to;
  }

  await query(
    `UPDATE ${table("tickets")} SET ${sets.join(", ")} WHERE ticket_id = @ticketId`,
    params
  );
}

export async function deleteTicket(ticketId: string): Promise<void> {
  await query(`DELETE FROM ${table("tickets")} WHERE ticket_id = @ticketId`, { ticketId });
}
