import { query, table } from "../db/index.js";

// ── Types ──────────────────────────────────────────────────────────

export type ReportTemplateRow = {
  template_id: string;
  template_name: string;
  user_id: string;
  fixed_filters: string;
  dynamic_filters: string;
  selected_columns: string;
  include_opps: boolean;
  created_at: string;
  updated_at: string;
};

export type CreateTemplateInput = {
  templateName: string;
  fixedFilters: Record<string, unknown>;
  dynamicFilters: unknown[];
  selectedColumns: string[];
  includeOpps?: boolean;
};

export type UpdateTemplateInput = Partial<CreateTemplateInput>;

// ── CRUD ───────────────────────────────────────────────────────────

export async function listTemplates(userId: string): Promise<ReportTemplateRow[]> {
  return query<ReportTemplateRow>(
    `SELECT template_id, template_name, user_id,
            fixed_filters::text AS fixed_filters,
            dynamic_filters::text AS dynamic_filters,
            selected_columns::text AS selected_columns,
            COALESCE(include_opps, false) AS include_opps,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("report_templates")}
     WHERE user_id = @userId
     ORDER BY created_at DESC
     LIMIT 50`,
    { userId }
  );
}

export async function getTemplate(templateId: string): Promise<ReportTemplateRow | null> {
  const rows = await query<ReportTemplateRow>(
    `SELECT template_id, template_name, user_id,
            fixed_filters::text AS fixed_filters,
            dynamic_filters::text AS dynamic_filters,
            selected_columns::text AS selected_columns,
            COALESCE(include_opps, false) AS include_opps,
            created_at::text AS created_at,
            updated_at::text AS updated_at
     FROM ${table("report_templates")}
     WHERE template_id = @templateId`,
    { templateId }
  );
  return rows[0] ?? null;
}

export async function createTemplate(
  userId: string,
  input: CreateTemplateInput
): Promise<{ templateId: string }> {
  const rows = await query<{ template_id: string }>(
    `INSERT INTO ${table("report_templates")} (
       template_name, user_id, fixed_filters, dynamic_filters,
       selected_columns, include_opps
     ) VALUES (
       @templateName, @userId, @fixedFilters, @dynamicFilters,
       @selectedColumns, @includeOpps
     ) RETURNING template_id`,
    {
      templateName: input.templateName,
      userId,
      fixedFilters: JSON.stringify(input.fixedFilters),
      dynamicFilters: JSON.stringify(input.dynamicFilters),
      selectedColumns: JSON.stringify(input.selectedColumns),
      includeOpps: input.includeOpps ?? false,
    }
  );
  return { templateId: rows[0].template_id };
}

export async function updateTemplate(
  templateId: string,
  input: UpdateTemplateInput
): Promise<void> {
  const sets: string[] = ["updated_at = CURRENT_TIMESTAMP()"];
  const params: Record<string, unknown> = { templateId };

  if (input.templateName !== undefined) {
    sets.push("template_name = @templateName");
    params.templateName = input.templateName;
  }
  if (input.fixedFilters !== undefined) {
    sets.push("fixed_filters = @fixedFilters");
    params.fixedFilters = JSON.stringify(input.fixedFilters);
  }
  if (input.dynamicFilters !== undefined) {
    sets.push("dynamic_filters = @dynamicFilters");
    params.dynamicFilters = JSON.stringify(input.dynamicFilters);
  }
  if (input.selectedColumns !== undefined) {
    sets.push("selected_columns = @selectedColumns");
    params.selectedColumns = JSON.stringify(input.selectedColumns);
  }
  if (input.includeOpps !== undefined) {
    sets.push("include_opps = @includeOpps");
    params.includeOpps = input.includeOpps;
  }

  await query(
    `UPDATE ${table("report_templates")} SET ${sets.join(", ")} WHERE template_id = @templateId`,
    params
  );
}

export async function deleteTemplate(templateId: string): Promise<void> {
  await query(
    `DELETE FROM ${table("report_templates")} WHERE template_id = @templateId`,
    { templateId }
  );
}
