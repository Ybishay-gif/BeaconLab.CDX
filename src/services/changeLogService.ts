import { randomUUID } from "node:crypto";
import { query, table } from "../db/bigquery.js";

export type ChangeLogRow = {
  change_id: string;
  changed_at: string;
  changed_by_user_id: string;
  changed_by_email: string;
  object_type: string;
  object_id: string | null;
  action: string;
  before_json: string | null;
  after_json: string | null;
  metadata_json: string | null;
};

export type ChangeLogEntry = {
  objectType: string;
  objectId?: string;
  action: string;
  before?: unknown;
  after?: unknown;
  metadata?: unknown;
};

let changeLogTableReady: Promise<void> | null = null;

function ensureChangeLogTableExists(): Promise<void> {
  if (!changeLogTableReady) {
    changeLogTableReady = query(
      `
        CREATE TABLE IF NOT EXISTS ${table("change_log")} (
          change_id STRING NOT NULL,
          changed_at TIMESTAMP NOT NULL,
          changed_by_user_id STRING NOT NULL,
          changed_by_email STRING NOT NULL,
          object_type STRING NOT NULL,
          object_id STRING,
          action STRING NOT NULL,
          before_json STRING,
          after_json STRING,
          metadata_json STRING
        )
      `
    ).then(() => undefined);
  }
  return changeLogTableReady;
}

export async function appendChangeLog(
  user: { userId: string; email: string },
  entry: ChangeLogEntry
): Promise<{ changeId: string }> {
  await ensureChangeLogTableExists();
  const changeId = randomUUID();
  await query(
    `
      INSERT INTO ${table("change_log")} (
        change_id,
        changed_at,
        changed_by_user_id,
        changed_by_email,
        object_type,
        object_id,
        action,
        before_json,
        after_json,
        metadata_json
      )
      VALUES (
        @changeId,
        CURRENT_TIMESTAMP(),
        @userId,
        @email,
        @objectType,
        @objectId,
        @action,
        @beforeJson,
        @afterJson,
        @metadataJson
      )
    `,
    {
      changeId,
      userId: user.userId,
      email: user.email,
      objectType: String(entry.objectType || "").trim(),
      objectId: entry.objectId ? String(entry.objectId).trim() : null,
      action: String(entry.action || "").trim(),
      beforeJson: entry.before === undefined ? null : JSON.stringify(entry.before),
      afterJson: entry.after === undefined ? null : JSON.stringify(entry.after),
      metadataJson: entry.metadata === undefined ? null : JSON.stringify(entry.metadata)
    }
  );
  return { changeId };
}

export async function listChangeLogs(limit = 200): Promise<ChangeLogRow[]> {
  await ensureChangeLogTableExists();
  const normalizedLimit = Math.max(1, Math.min(1000, Math.floor(Number(limit) || 200)));
  return query<ChangeLogRow>(
    `
      SELECT
        change_id,
        CAST(changed_at AS STRING) AS changed_at,
        changed_by_user_id,
        changed_by_email,
        object_type,
        object_id,
        action,
        before_json,
        after_json,
        metadata_json
      FROM ${table("change_log")}
      ORDER BY changed_at DESC
      LIMIT @limit
    `,
    { limit: normalizedLimit }
  );
}

