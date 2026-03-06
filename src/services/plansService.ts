import { randomUUID } from "node:crypto";
import { query, table } from "../db/bigquery.js";

type PlanStatus = "draft" | "ready" | "archived";

type CreatePlanInput = {
  planName: string;
  description?: string;
  createdBy: string;
};

type ClonePlanInput = {
  planName?: string;
  description?: string;
};

type UpdatePlanInput = {
  planName?: string;
  description?: string;
};

type PlanRow = {
  plan_id: string;
  plan_name: string;
  description: string | null;
  status: PlanStatus;
  created_by: string;
  created_at: { value: string } | string;
  updated_at: { value: string } | string | null;
  plan_context_json?: string | null;
  plan_strategy_json?: string | null;
};

type PlanParameterRow = {
  param_key: string;
  param_value: string;
  value_type: "int" | "float" | "bool" | "string" | "json";
  updated_by: string | null;
  updated_at: { value: string } | string | null;
};

const WRITE_BATCH_SIZE = 10;

async function runBatched<T>(rows: T[], worker: (row: T) => Promise<void>, batchSize = WRITE_BATCH_SIZE): Promise<void> {
  for (let offset = 0; offset < rows.length; offset += batchSize) {
    const batch = rows.slice(offset, offset + batchSize);
    await Promise.all(batch.map((row) => worker(row)));
  }
}

export async function listPlans(): Promise<PlanRow[]> {
  return query<PlanRow>(
    `
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
        SELECT plan_id, ANY_VALUE(param_value) AS plan_context_json
        FROM ${table("plan_parameters")}
        WHERE param_key = 'plan_context_config'
        GROUP BY plan_id
      ) pc
        ON pc.plan_id = p.plan_id
      LEFT JOIN (
        SELECT plan_id, ANY_VALUE(param_value) AS plan_strategy_json
        FROM ${table("plan_parameters")}
        WHERE param_key = 'plan_strategy_config'
        GROUP BY plan_id
      ) ps
        ON ps.plan_id = p.plan_id
      ORDER BY p.created_at DESC
    `
  );
}

export async function createPlan(input: CreatePlanInput): Promise<{ planId: string }> {
  const planId = randomUUID();

  await query(
    `
      INSERT INTO ${table("plans")}
      (plan_id, plan_name, description, status, created_by, created_at)
      VALUES (@planId, @planName, @description, 'draft', @createdBy, CURRENT_TIMESTAMP())
    `,
    {
      planId,
      planName: input.planName,
      description: input.description ?? null,
      createdBy: input.createdBy
    }
  );

  return { planId };
}

export async function clonePlan(
  sourcePlanId: string,
  userId: string,
  input: ClonePlanInput = {}
): Promise<{ planId: string }> {
  const source = await getPlan(sourcePlanId);
  if (!source) {
    throw new Error("Plan not found");
  }

  const planId = randomUUID();
  const planName = String(input.planName || "").trim() || `${source.plan_name} (Clone)`;
  const description = input.description !== undefined ? input.description : source.description;

  await query(
    `
      INSERT INTO ${table("plans")}
      (plan_id, plan_name, description, status, created_by, created_at)
      VALUES (@planId, @planName, @description, 'draft', @createdBy, CURRENT_TIMESTAMP())
    `,
    {
      planId,
      planName,
      description: description ?? null,
      createdBy: userId
    }
  );

  await query(
    `
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
    `,
    { newPlanId: planId, sourcePlanId, updatedBy: userId }
  );

  await query(
    `
      INSERT INTO ${table("plan_decisions")}
      (decision_id, plan_id, decision_type, state, channel, decision_value, reason, created_by, created_at)
      SELECT
        GENERATE_UUID(),
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
    `,
    { newPlanId: planId, sourcePlanId, createdBy: userId }
  );

  return { planId };
}

export async function getPlan(planId: string): Promise<PlanRow | null> {
  const rows = await query<PlanRow>(
    `
      SELECT plan_id, plan_name, description, status, created_by, created_at, updated_at
      FROM ${table("plans")}
      WHERE plan_id = @planId
      LIMIT 1
    `,
    { planId }
  );

  return rows[0] ?? null;
}

export async function listPlanParameters(planId: string): Promise<PlanParameterRow[]> {
  return query<PlanParameterRow>(
    `
      SELECT param_key, param_value, value_type, updated_by, updated_at
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
      ORDER BY param_key
    `,
    { planId }
  );
}

export async function updatePlan(planId: string, input: UpdatePlanInput): Promise<void> {
  const updates: string[] = [];
  const params: Record<string, unknown> = { planId };

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

  await query(
    `
      UPDATE ${table("plans")}
      SET ${updates.join(",\n          ")}
      WHERE plan_id = @planId
    `,
    params
  );
}

export async function deletePlan(planId: string): Promise<void> {
  await query(`DELETE FROM ${table("plan_results")} WHERE plan_id = @planId`, { planId });
  await query(`DELETE FROM ${table("plan_runs")} WHERE plan_id = @planId`, { planId });
  await query(`DELETE FROM ${table("plan_decisions")} WHERE plan_id = @planId`, { planId });
  await query(`DELETE FROM ${table("plan_parameters")} WHERE plan_id = @planId`, { planId });
  await query(`DELETE FROM ${table("plans")} WHERE plan_id = @planId`, { planId });
}

export async function upsertParameters(
  planId: string,
  userId: string,
  parameters: Array<{ key: string; value: string; valueType: "int" | "float" | "bool" | "string" | "json" }>
): Promise<void> {
  if (parameters.length === 0) {
    return;
  }

  await runBatched(parameters, async (parameter) => {
    await query(
      `
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
      `,
      {
        planId,
        paramKey: parameter.key,
        paramValue: parameter.value,
        valueType: parameter.valueType,
        updatedBy: userId
      }
    );
  });
}

export async function appendDecisions(
  planId: string,
  userId: string,
  decisions: Array<{
    decisionType: string;
    decisionValue: string;
    state?: string;
    channel?: string;
    reason?: string;
  }>
): Promise<{ decisionIds: string[] }> {
  const decisionsWithIds = decisions.map((decision) => ({
    decisionId: randomUUID(),
    decision
  }));

  await runBatched(decisionsWithIds, async (entry) => {
    await query(
      `
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
      `,
      {
        decisionId: entry.decisionId,
        planId,
        decisionType: entry.decision.decisionType,
        state: entry.decision.state ?? "",
        channel: entry.decision.channel ?? "",
        decisionValue: entry.decision.decisionValue,
        reason: entry.decision.reason ?? "",
        createdBy: userId
      }
    );
  });

  return { decisionIds: decisionsWithIds.map((entry) => entry.decisionId) };
}

export async function createRun(planId: string, userId: string): Promise<{ runId: string }> {
  const runId = randomUUID();

  await query(
    `
      INSERT INTO ${table("plan_runs")}
      (run_id, plan_id, triggered_by, status, created_at)
      VALUES (@runId, @planId, @triggeredBy, 'queued', CURRENT_TIMESTAMP())
    `,
    { runId, planId, triggeredBy: userId }
  );

  return { runId };
}

export async function getRun(planId: string, runId: string) {
  const rows = await query(
    `
      SELECT run_id, plan_id, triggered_by, status, started_at, finished_at, error_message, created_at
      FROM ${table("plan_runs")}
      WHERE plan_id = @planId AND run_id = @runId
      LIMIT 1
    `,
    { planId, runId }
  );

  return rows[0] ?? null;
}

export async function getRunResults(planId: string, runId: string) {
  return query(
    `
      SELECT run_id, plan_id, state, channel, metric_name, baseline_value, simulated_value, delta_value, delta_pct, created_at
      FROM ${table("plan_results")}
      WHERE plan_id = @planId AND run_id = @runId
      ORDER BY state, channel, metric_name
    `,
    { planId, runId }
  );
}
