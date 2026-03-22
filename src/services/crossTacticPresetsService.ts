/**
 * Cross Tactic Explorer presets — save/load named configurations.
 * Stores dimensions, measures, filters, custom filters, dates in JSONB.
 */

import { query, table } from "../db/index.js";

export type PresetConfig = {
  dimensions: string[];
  measures: string[];
  filters: Record<string, string[]>;
  dynamicFilters: Array<{ column: string; operator: string; value: unknown }>;
  startDate: string;
  endDate: string;
  compareEnabled: boolean;
  compareStartDate?: string;
  compareEndDate?: string;
};

export type PresetRow = {
  preset_id: string;
  user_id: string;
  preset_name: string;
  config: string; // JSONB serialized
  created_at: string;
  updated_at: string;
};

export async function listPresets(userId: string): Promise<PresetRow[]> {
  return query<PresetRow>(
    `SELECT preset_id, user_id, preset_name,
            config::text AS config,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("cross_tactic_presets")}
     WHERE user_id = @userId
     ORDER BY created_at DESC
     LIMIT 50`,
    { userId }
  );
}

export async function createPreset(
  userId: string,
  presetName: string,
  config: PresetConfig
): Promise<{ presetId: string }> {
  const rows = await query<{ preset_id: string }>(
    `INSERT INTO ${table("cross_tactic_presets")} (user_id, preset_name, config)
     VALUES (@userId, @presetName, @config)
     RETURNING preset_id`,
    { userId, presetName, config: JSON.stringify(config) }
  );
  return { presetId: rows[0].preset_id };
}

export async function deletePreset(presetId: string): Promise<void> {
  await query(
    `DELETE FROM ${table("cross_tactic_presets")} WHERE preset_id = @presetId`,
    { presetId }
  );
}
