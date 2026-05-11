-- Track E B3 fan-out: backfill source_locator_json for addtest sources.
-- Declares logical-clone relationships so the Worker fan-out dispatcher routes
-- 1 source event to N shadow tables without touching Debezium table.include.list.
--
-- Mapping:
--   src id=29  orders_addtest         → logical_clone_of id=11  (orders, pg)
--   src id=30  legacy_orders_addtest  → logical_clone_of id=27  (legacy_orders, maria)
--   src id=31  payment_bills_addtest  → logical_clone_of id=28  (payment_bills, mongo)
--
-- Note: src 27/28 may be is_active=false. The clone route (29/30/31) must
-- itself be active; the master's is_active state does NOT block fan-out routing.

BEGIN;

UPDATE cdc_system.source_object_registry
   SET source_locator_json = source_locator_json || jsonb_build_object('logical_clone_of', 11, 'fan_out_role', 'clone')
 WHERE id = 29;

UPDATE cdc_system.source_object_registry
   SET source_locator_json = source_locator_json || jsonb_build_object('logical_clone_of', 27, 'fan_out_role', 'clone')
 WHERE id = 30;

UPDATE cdc_system.source_object_registry
   SET source_locator_json = source_locator_json || jsonb_build_object('logical_clone_of', 28, 'fan_out_role', 'clone')
 WHERE id = 31;

COMMIT;
