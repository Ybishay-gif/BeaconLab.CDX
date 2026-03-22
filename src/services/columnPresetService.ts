import { query, table } from "../db/index.js";

// ── Types ──────────────────────────────────────────────────────────

export type PresetColumn = {
  column_name: string;
  display_name: string;
};

export type ColumnPresetRow = {
  preset_id: string;
  preset_name: string;
  user_id: string;
  columns: string; // JSON string of PresetColumn[]
  created_at: string;
  updated_at: string;
};

export type CreatePresetInput = {
  presetName: string;
  columns: PresetColumn[];
};

export type UpdatePresetInput = Partial<CreatePresetInput>;

// ── CRUD ───────────────────────────────────────────────────────────

export async function listPresets(userId: string): Promise<ColumnPresetRow[]> {
  return query<ColumnPresetRow>(
    `SELECT preset_id, preset_name, user_id,
            columns::text AS columns,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("column_presets")}
     WHERE user_id = @userId
     ORDER BY created_at DESC
     LIMIT 50`,
    { userId }
  );
}

export async function getPreset(presetId: string): Promise<ColumnPresetRow | null> {
  const rows = await query<ColumnPresetRow>(
    `SELECT preset_id, preset_name, user_id,
            columns::text AS columns,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("column_presets")}
     WHERE preset_id = @presetId`,
    { presetId }
  );
  return rows[0] ?? null;
}

export async function createPreset(
  userId: string,
  input: CreatePresetInput
): Promise<{ presetId: string }> {
  const rows = await query<{ preset_id: string }>(
    `INSERT INTO ${table("column_presets")} (
       preset_name, user_id, columns
     ) VALUES (
       @presetName, @userId, @columns
     ) RETURNING preset_id`,
    {
      presetName: input.presetName,
      userId,
      columns: JSON.stringify(input.columns),
    }
  );
  return { presetId: rows[0].preset_id };
}

export async function updatePreset(
  presetId: string,
  input: UpdatePresetInput
): Promise<void> {
  const sets: string[] = ["updated_at = CURRENT_TIMESTAMP()"];
  const params: Record<string, unknown> = { presetId };

  if (input.presetName !== undefined) {
    sets.push("preset_name = @presetName");
    params.presetName = input.presetName;
  }
  if (input.columns !== undefined) {
    sets.push("columns = @columns");
    params.columns = JSON.stringify(input.columns);
  }

  await query(
    `UPDATE ${table("column_presets")} SET ${sets.join(", ")} WHERE preset_id = @presetId`,
    params
  );
}

export async function deletePreset(presetId: string): Promise<void> {
  await query(
    `DELETE FROM ${table("column_presets")} WHERE preset_id = @presetId`,
    { presetId }
  );
}
