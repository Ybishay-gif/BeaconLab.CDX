# Design System (MVP)

This is the single source of truth for UI consistency in this app.
Any new page, table, button, filter row, or action control must follow these rules.

## Core Principle
- Reuse before create.
- If a needed style exists, use the existing class and component pattern.
- Do not add one-off visual variants unless approved and documented here.

## Tokens

Design tokens live in `public/styles.css` under `:root`:
- Colors: `--bg`, `--surface`, `--text`, `--muted`, `--border`, `--primary`, `--primary-2`
- Radii: `--radius-sm`, `--radius-md`, `--radius-lg`
- Spacing: `--space-1` to `--space-4`
- Controls: `--control-height`
- Date presets: `--date-preset-*`

Rules:
- Use token variables, not hardcoded new colors/radii/spacing.
- If a new token is required, add it in `:root` and document it here.

## Buttons

Use only these button patterns:

1. Primary text button
- Class: default `button` or `compact-btn`
- Use for page-level actions like Apply, Clear, Refresh, Export.

2. Secondary text button
- Class: `compact-btn secondary-btn`
- Use for non-destructive secondary actions.

3. Row action icon button
- Classes: `icon-btn`, `icon-btn-secondary`, `icon-btn-danger`
- Use for table row actions: save/edit/delete.
- Preferred in dense tables instead of wide text buttons.

4. Add button (square plus)
- Class: `icon-square-btn`
- Use for row creation actions (for example, add strategy rule, add plan row).

Do not:
- Mix text action buttons in one row with icon actions in equivalent tables.
- Introduce new row-action button dimensions when `icon-btn` exists.

## Tables

All data tables must follow shared table system:
- Wrapper: `.table-wrap`
- Base table styles: `table`, `th`, `td`, sticky headers and filter row behavior as already implemented.

Table action column:
- Keep width compact.
- Prefer icon actions (`icon-btn`) for save/edit/delete.
- Keep action control order stable: edit/save -> clone (if applicable) -> delete.

Settings tables:
- Use the same `.table-wrap` + base table styles.
- Keep Settings focused on operational tables (for example, global filters reference) and avoid custom card-style lists.

## Page Header / Toolbar

Use the same toolbar structure:
- Container: `.analytics-toolbar`
- Right action group: `.analytics-date-inline`

Rules:
- Avoid multiline toolbar controls unless viewport forces wrapping.
- Page-level add actions should use `icon-square-btn` where equivalent sections already do.

## Date Controls

Date controls must use shared component mounting:
- Source: `public/modules/ui.js`
- Called from `main.js` via `mountDateRangeComponents()`

Do not handcraft new date range markup when shared control already exists.

## Reuse Map Requirement (Before UI Edits)

Before implementing UI changes, define:
- Which existing page is the visual reference.
- Which existing classes/components are reused.
- Which elements (if any) are new and why reuse is not possible.

This must be part of the implementation plan before code edits.

## UI QA Requirement (Before Deploy)

For any UI change:
1. `npm run check` must pass.
2. Validate affected flow in browser (Playwright/manual).
3. Confirm no layout break in target screen.
4. Confirm control consistency against this design system.
5. Document outcome in the task response.

Do not hand off UI work without completing this QA.

## Update Protocol

When a new approved UI pattern is introduced:
1. Add/adjust shared class(es) in `public/styles.css`.
2. Refactor existing usage where needed to keep parity.
3. Update this file in the same change.
