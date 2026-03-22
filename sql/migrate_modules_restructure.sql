-- Module Restructuring Migration
-- Expands old module IDs into new ones for existing users.
-- Old rows are NOT deleted (kept for rollback safety).
--
-- Mapping:
--   planning → lm_tools, beacon_lite_tactic, beacon_lite_cross_tactic, kissterra_tools
--   cross_tactic → beacon_lite_cross_tactic
--   channel_recommendations → lm_tools

BEGIN;

-- Users with 'planning' get all 4 new modules
INSERT INTO user_modules (user_id, module_id)
SELECT user_id, unnest(ARRAY['lm_tools','beacon_lite_tactic','beacon_lite_cross_tactic','kissterra_tools'])
FROM user_modules WHERE module_id = 'planning'
ON CONFLICT DO NOTHING;

-- Users with 'cross_tactic' get beacon_lite_cross_tactic
INSERT INTO user_modules (user_id, module_id)
SELECT user_id, 'beacon_lite_cross_tactic'
FROM user_modules WHERE module_id = 'cross_tactic'
ON CONFLICT DO NOTHING;

-- Users with 'channel_recommendations' get lm_tools
INSERT INTO user_modules (user_id, module_id)
SELECT user_id, 'lm_tools'
FROM user_modules WHERE module_id = 'channel_recommendations'
ON CONFLICT DO NOTHING;

COMMIT;
