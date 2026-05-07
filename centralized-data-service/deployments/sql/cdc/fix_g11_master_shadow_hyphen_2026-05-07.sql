-- G-11 backfill: normalize hyphen → underscore in master_binding.master_table and shadow_binding.shadow_table.
-- Idempotent: chỉ update row có hyphen.
BEGIN;

-- 1. Fix shadow_binding
UPDATE cdc_system.shadow_binding
SET shadow_table = REPLACE(shadow_table, '-', '_'),
    physical_table_fqn = REPLACE(physical_table_fqn, '-', '_'),
    updated_at = NOW()
WHERE shadow_table LIKE '%-%';

-- 2. Fix master_binding
UPDATE cdc_system.master_binding
SET master_table = REPLACE(master_table, '-', '_'),
    physical_table_fqn = REPLACE(physical_table_fqn, '-', '_'),
    updated_at = NOW()
WHERE master_table LIKE '%-%';

-- 3. Reset src 44 state để re-trigger master_bind step
UPDATE cdc_system.source_object_registry
SET provisioning_state = 'master_pending',
    last_step_error = NULL,
    updated_at = NOW()
WHERE id = 44 AND provisioning_state = 'failed';

COMMIT;

-- Verify
SELECT id, shadow_table FROM cdc_system.shadow_binding WHERE source_object_id = 44;
SELECT id, master_table FROM cdc_system.master_binding WHERE source_object_id = 44;
SELECT id, provisioning_state FROM cdc_system.source_object_registry WHERE id = 44;
