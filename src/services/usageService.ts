import { query, table } from "../db/index.js";

export interface UsageEvent {
  sessionId: string;
  eventType: "login" | "page_visit" | "view_change" | "interaction";
  page?: string;
  action?: string;
  metadata?: Record<string, unknown>;
  module?: string;
}

export interface UsageEventRow {
  id: number;
  session_id: string;
  user_id: string;
  user_email: string;
  event_type: string;
  page: string | null;
  action: string | null;
  metadata: Record<string, unknown>;
  module: string;
  created_at: string;
}

export interface UsageQueryParams {
  startDate?: string;
  endDate?: string;
  userId?: string;
  eventType?: string;
  page?: string;
  module?: string;
  limit?: number;
  offset?: number;
}

export async function recordUsageEvents(
  userId: string,
  email: string,
  events: UsageEvent[]
): Promise<void> {
  for (const event of events) {
    await query(
      `INSERT INTO ${table("usage_events")}
         (session_id, user_id, user_email, event_type, page, action, metadata, module, created_at)
       VALUES
         (@sessionId, @userId, @email, @eventType, @page, @action, @metadata::jsonb, @module, NOW())`,
      {
        sessionId: event.sessionId,
        userId,
        email,
        eventType: event.eventType,
        page: event.page || null,
        action: event.action || null,
        metadata: JSON.stringify(event.metadata || {}),
        module: event.module || "planning",
      }
    );
  }
}

export async function queryUsageEvents(params: UsageQueryParams): Promise<{
  rows: UsageEventRow[];
  total: number;
}> {
  const conditions: string[] = ["1=1"];
  const sqlParams: Record<string, unknown> = {};

  if (params.startDate) {
    conditions.push("created_at >= @startDate::date");
    sqlParams.startDate = params.startDate;
  }
  if (params.endDate) {
    conditions.push("created_at < (@endDate::date + INTERVAL '1 day')");
    sqlParams.endDate = params.endDate;
  }
  if (params.userId) {
    conditions.push("user_id = @userId");
    sqlParams.userId = params.userId;
  }
  if (params.eventType) {
    conditions.push("event_type = @eventType");
    sqlParams.eventType = params.eventType;
  }
  if (params.page) {
    conditions.push("page = @page");
    sqlParams.page = params.page;
  }
  if (params.module) {
    conditions.push("module = @module");
    sqlParams.module = params.module;
  }

  const where = conditions.join(" AND ");
  const limit = Math.min(params.limit || 500, 5000);
  const offset = params.offset || 0;

  const countResult = await query<{ count: string }>(
    `SELECT COUNT(*)::text AS count FROM ${table("usage_events")} WHERE ${where}`,
    sqlParams
  );
  const total = Number(countResult[0]?.count || 0);

  const rows = await query<UsageEventRow>(
    `SELECT id, session_id, user_id, user_email, event_type, page, action,
            metadata, module, created_at::text AS created_at
     FROM ${table("usage_events")}
     WHERE ${where}
     ORDER BY created_at DESC
     LIMIT ${limit} OFFSET ${offset}`,
    sqlParams
  );

  return { rows, total };
}
