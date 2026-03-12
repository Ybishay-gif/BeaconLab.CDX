import { randomUUID } from "node:crypto";
import { query, table } from "../db/index.js";
import { config } from "../config.js";
const WRITE_BATCH_SIZE = 10;
async function runBatched(rows, worker, batchSize = WRITE_BATCH_SIZE) {
    for (let offset = 0; offset < rows.length; offset += batchSize) {
        const batch = rows.slice(offset, offset + batchSize);
        await Promise.all(batch.map((row) => worker(row)));
    }
}
export async function listPlans() {
    return query(`
      SELECT
        p.plan_id,
        p.plan_name,
        p.description,
        p.status,
        p.created_by,
        p.created_at,
        p.updated_at,
        pc.plan_context_json,
        ps.plan_strategy_json
      FROM ${table("plans")} p
      LEFT JOIN (
        SELECT plan_id, MAX(param_value) AS plan_context_json
        FROM ${table("plan_parameters")}
        WHERE param_key = 'plan_context_config'
        GROUP BY plan_id
      ) pc
        ON pc.plan_id = p.plan_id
      LEFT JOIN (
        SELECT plan_id, MAX(param_value) AS plan_strategy_json
        FROM ${table("plan_parameters")}
        WHERE param_key = 'plan_strategy_config'
        GROUP BY plan_id
      ) ps
        ON ps.plan_id = p.plan_id
      ORDER BY p.created_at DESC
    `);
}
export async function createPlan(input) {
    const planId = randomUUID();
    await query(`
      INSERT INTO ${table("plans")}
      (plan_id, plan_name, description, status, created_by, created_at)
      VALUES (@planId, @planName, @description, 'draft', @createdBy, CURRENT_TIMESTAMP())
    `, {
        planId,
        planName: input.planName,
        description: input.description ?? null,
        createdBy: input.createdBy
    });
    return { planId };
}
export async function clonePlan(sourcePlanId, userId, input = {}) {
    const source = await getPlan(sourcePlanId);
    if (!source) {
        throw new Error("Plan not found");
    }
    const planId = randomUUID();
    const planName = String(input.planName || "").trim() || `${source.plan_name} (Clone)`;
    const description = input.description !== undefined ? input.description : source.description;
    await query(`
      INSERT INTO ${table("plans")}
      (plan_id, plan_name, description, status, created_by, created_at)
      VALUES (@planId, @planName, @description, 'draft', @createdBy, CURRENT_TIMESTAMP())
    `, {
        planId,
        planName,
        description: description ?? null,
        createdBy: userId
    });
    await query(`
      INSERT INTO ${table("plan_parameters")}
      (plan_id, param_key, param_value, value_type, updated_by, updated_at)
      SELECT
        @newPlanId,
        param_key,
        param_value,
        value_type,
        @updatedBy,
        CURRENT_TIMESTAMP()
      FROM ${table("plan_parameters")}
      WHERE plan_id = @sourcePlanId
    `, { newPlanId: planId, sourcePlanId, updatedBy: userId });
    await query(`
      INSERT INTO ${table("plan_decisions")}
      (decision_id, plan_id, decision_type, state, channel, decision_value, reason, created_by, created_at)
      SELECT
        ${config.usePg ? "gen_random_uuid()" : "GENERATE_UUID()"},
        @newPlanId,
        decision_type,
        state,
        channel,
        decision_value,
        reason,
        @createdBy,
        CURRENT_TIMESTAMP()
      FROM ${table("plan_decisions")}
      WHERE plan_id = @sourcePlanId
    `, { newPlanId: planId, sourcePlanId, createdBy: userId });
    return { planId };
}
export async function getPlan(planId) {
    const rows = await query(`
      SELECT plan_id, plan_name, description, status, created_by, created_at, updated_at
      FROM ${table("plans")}
      WHERE plan_id = @planId
      LIMIT 1
    `, { planId });
    return rows[0] ?? null;
}
export async function listPlanParameters(planId) {
    return query(`
      SELECT param_key, param_value, value_type, updated_by, updated_at
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
      ORDER BY param_key
    `, { planId });
}
export async function getParameterValues(planId, keys) {
    if (keys.length === 0)
        return {};
    const placeholders = keys.map((_, i) => `@key${i}`).join(", ");
    const params = { planId };
    keys.forEach((k, i) => { params[`key${i}`] = k; });
    const rows = await query(`SELECT param_key, param_value FROM ${table("plan_parameters")} WHERE plan_id = @planId AND param_key IN (${placeholders})`, params);
    const result = {};
    for (const r of rows)
        result[r.param_key] = r.param_value;
    return result;
}
export async function updatePlan(planId, input) {
    const updates = [];
    const params = { planId };
    if (typeof input.planName === "string") {
        updates.push("plan_name = @planName");
        params.planName = input.planName.trim();
    }
    if (input.description !== undefined) {
        updates.push("description = @description");
        params.description = input.description?.trim() || null;
    }
    if (!updates.length) {
        return;
    }
    updates.push("updated_at = CURRENT_TIMESTAMP()");
    await query(`
      UPDATE ${table("plans")}
      SET ${updates.join(",\n          ")}
      WHERE plan_id = @planId
    `, params);
}
export async function deletePlan(planId) {
    await query(`DELETE FROM ${table("plan_results")} WHERE plan_id = @planId`, { planId });
    await query(`DELETE FROM ${table("plan_runs")} WHERE plan_id = @planId`, { planId });
    await query(`DELETE FROM ${table("plan_decisions")} WHERE plan_id = @planId`, { planId });
    await query(`DELETE FROM ${table("plan_parameters")} WHERE plan_id = @planId`, { planId });
    await query(`DELETE FROM ${table("plans")} WHERE plan_id = @planId`, { planId });
}
export async function upsertParameters(planId, userId, parameters) {
    if (parameters.length === 0) {
        return;
    }
    await runBatched(parameters, async (parameter) => {
        const params = {
            planId,
            paramKey: parameter.key,
            paramValue: parameter.value,
            valueType: parameter.valueType,
            updatedBy: userId
        };
        if (config.usePg) {
            await query(`
          INSERT INTO ${table("plan_parameters")}
          (plan_id, param_key, param_value, value_type, updated_by, updated_at)
          VALUES (@planId, @paramKey, @paramValue, @valueType, @updatedBy, NOW())
          ON CONFLICT (plan_id, param_key) DO UPDATE SET
            param_value = EXCLUDED.param_value,
            value_type = EXCLUDED.value_type,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW()
        `, params);
        }
        else {
            await query(`
          MERGE ${table("plan_parameters")} T
          USING (
            SELECT @planId AS plan_id, @paramKey AS param_key
          ) S
          ON T.plan_id = S.plan_id AND T.param_key = S.param_key
          WHEN MATCHED THEN
            UPDATE SET
              param_value = @paramValue,
              value_type = @valueType,
              updated_by = @updatedBy,
              updated_at = CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN
            INSERT (plan_id, param_key, param_value, value_type, updated_by, updated_at)
            VALUES (@planId, @paramKey, @paramValue, @valueType, @updatedBy, CURRENT_TIMESTAMP())
        `, params);
        }
    });
}
export async function upsertPlanContext(planId, userId, context) {
    const contextJson = JSON.stringify(context);
    await upsertParameters(planId, userId, [
        { key: "plan_context_config", value: contextJson, valueType: "json" }
    ]);
}
export async function appendDecisions(planId, userId, decisions) {
    const decisionsWithIds = decisions.map((decision) => ({
        decisionId: randomUUID(),
        decision
    }));
    await runBatched(decisionsWithIds, async (entry) => {
        await query(`
        INSERT INTO ${table("plan_decisions")}
        (decision_id, plan_id, decision_type, state, channel, decision_value, reason, created_by, created_at)
        VALUES (
          @decisionId,
          @planId,
          @decisionType,
          NULLIF(@state, ''),
          NULLIF(@channel, ''),
          @decisionValue,
          NULLIF(@reason, ''),
          @createdBy,
          CURRENT_TIMESTAMP()
        )
      `, {
            decisionId: entry.decisionId,
            planId,
            decisionType: entry.decision.decisionType,
            state: entry.decision.state ?? "",
            channel: entry.decision.channel ?? "",
            decisionValue: entry.decision.decisionValue,
            reason: entry.decision.reason ?? "",
            createdBy: userId
        });
    });
    return { decisionIds: decisionsWithIds.map((entry) => entry.decisionId) };
}
export async function createRun(planId, userId) {
    const runId = randomUUID();
    await query(`
      INSERT INTO ${table("plan_runs")}
      (run_id, plan_id, triggered_by, status, created_at)
      VALUES (@runId, @planId, @triggeredBy, 'queued', CURRENT_TIMESTAMP())
    `, { runId, planId, triggeredBy: userId });
    return { runId };
}
export async function getRun(planId, runId) {
    const rows = await query(`
      SELECT run_id, plan_id, triggered_by, status, started_at, finished_at, error_message, created_at
      FROM ${table("plan_runs")}
      WHERE plan_id = @planId AND run_id = @runId
      LIMIT 1
    `, { planId, runId });
    return rows[0] ?? null;
}
export async function getRunResults(planId, runId) {
    return query(`
      SELECT run_id, plan_id, state, channel, metric_name, baseline_value, simulated_value, delta_value, delta_pct, created_at
      FROM ${table("plan_results")}
      WHERE plan_id = @planId AND run_id = @runId
      ORDER BY state, channel, metric_name
    `, { planId, runId });
}
