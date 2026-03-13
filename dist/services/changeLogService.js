import { randomUUID } from "node:crypto";
import { config } from "../config.js";
import { query, table } from "../db/index.js";
let changeLogTableReady = null;
function ensureChangeLogTableExists() {
    if (config.usePg)
        return Promise.resolve();
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
          metadata_json STRING,
          module STRING
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
        metadata_json,
        module
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
        @metadataJson,
        @module
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
        metadataJson: entry.metadata === undefined ? null : JSON.stringify(entry.metadata),
        module: entry.module || "planning"
    });
    return { changeId };
}
export async function listChangeLogs(filters = {}) {
    await ensureChangeLogTableExists();
    const normalizedLimit = Math.max(1, Math.min(1000, Math.floor(Number(filters.limit) || 200)));
    const castAt = config.usePg ? "changed_at::text" : "CAST(changed_at AS STRING)";
    const conditions = [];
    const params = { limit: normalizedLimit };
    if (filters.objectType) {
        conditions.push("object_type = @objectType");
        params.objectType = filters.objectType;
    }
    if (filters.objectId) {
        conditions.push("object_id = @objectId");
        params.objectId = filters.objectId;
    }
    if (filters.userId) {
        conditions.push("changed_by_user_id = @userId");
        params.userId = filters.userId;
    }
    if (filters.module) {
        conditions.push("module = @module");
        params.module = filters.module;
    }
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    return query(`
      SELECT
        change_id,
        ${castAt} AS changed_at,
        changed_by_user_id,
        changed_by_email,
        object_type,
        object_id,
        action,
        before_json,
        after_json,
        metadata_json,
        module
      FROM ${table("change_log")}
      ${whereClause}
      ORDER BY changed_at DESC
      LIMIT @limit
    `, params);
}
