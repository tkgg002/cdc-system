-- v1.11: Bridge columns + Airbyte sync metadata
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS airbyte_raw_table VARCHAR;
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS last_bridge_at TIMESTAMP;
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS airbyte_sync_mode VARCHAR DEFAULT 'incremental';
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS airbyte_destination_sync_mode VARCHAR DEFAULT 'append';
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS airbyte_cursor_field VARCHAR;
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS airbyte_namespace VARCHAR;
