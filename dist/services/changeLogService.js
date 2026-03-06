import { randomUUID } from "node:crypto";
import { query, table } from "../db/bigquery.js";
let changeLogTableReady = null;
function ensureChangeLogTableExists() {
    if (!changeLogTableReady) {
        changeLogTableReady = query(`
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
      `).then(() => undefined);
    }
    return changeLogTableReady;
}
export async function appendChangeLog(user, entry) {
    await ensureChangeLogTableExists();
    const changeId = randomUUID();
    await query(`
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
    `, {
        changeId,
        userId: user.userId,
        email: user.email,
        objectType: String(entry.objectType || "").trim(),
        objectId: entry.objectId ? String(entry.objectId).trim() : null,
        action: String(entry.action || "").trim(),
        beforeJson: entry.before === undefined ? null : JSON.stringify(entry.before),
        afterJson: entry.after === undefined ? null : JSON.stringify(entry.after),
        metadataJson: entry.metadata === undefined ? null : JSON.stringify(entry.metadata)
    });
    return { changeId };
}
export async function listChangeLogs(limit = 200) {
    await ensureChangeLogTableExists();
    const normalizedLimit = Math.max(1, Math.min(1000, Math.floor(Number(limit) || 200)));
    return query(`
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
    `, { limit: normalizedLimit });
}
