export interface PendingField {
  id: number;
  table_name: string;
  source_db: string | null;
  field_name: string;
  sample_value: string | null;
  suggested_type: string;
  final_type: string | null;
  status: 'pending' | 'approved' | 'rejected';
  detected_at: string;
  detection_count: number;
  target_column_name: string | null;
  reviewed_by: string | null;
  approval_notes: string | null;
  rejection_reason: string | null;
}

export interface TableRegistry {
  id: number;
  source_db: string;
  source_type: 'mongodb' | 'mysql' | 'postgresql';
  source_table: string;
  target_table: string;
  sync_engine: 'debezium';
  sync_interval: string;
  priority: 'critical' | 'high' | 'normal' | 'low';
  primary_key_field: string;
  primary_key_type: string;
  is_active: boolean;
  is_table_created: boolean;
  created_at: string;
  updated_at: string;
  notes: string | null;
}

export type ProvisioningMode = 'auto' | 'manual';

// State machine — migration 047_source_provisioning_state.sql.
// Keep in sync with cdc_system.source_object_registry.provisioning_state CHECK.
export type ProvisioningState =
  | 'draft'
  | 'shadow_pending'
  | 'shadow_active'
  | 'master_pending'
  | 'master_active'
  | 'mapping_pending'
  | 'mapping_ready'
  | 'schedule_pending'
  | 'running'
  | 'paused'
  | 'failed'
  | 'archived';

export interface SourceObjectRow {
  id: number;
  registry_id?: number | null;
  shadow_binding_id?: number | null;
  object_code: string;
  source_db: string;
  source_type: 'mongodb' | 'mysql' | 'postgresql';
  source_table: string;
  target_table: string;
  shadow_schema?: string | null;
  physical_table_fqn?: string | null;
  sync_engine: 'debezium';
  sync_interval: string;
  priority: 'critical' | 'high' | 'normal' | 'low';
  primary_key_field: string;
  primary_key_type: string;
  timestamp_field?: string | null;
  is_active: boolean;
  is_table_created: boolean;
  profile_status?: string;
  ddl_status?: string | null;
  sync_status?: string;
  bridge_status?: 'bridged' | 'v2_only';
  metadata_status?: 'v2_ready' | 'v2_shadow_only' | 'v2_source_only';
  recon_drift?: number;
  created_at: string;
  updated_at: string;
  notes: string | null;
  // Phase multi_engine_unified — Toggle Auto/Manual surface (L3).
  // Backend exposes these from cdc_system.source_object_registry.
  provisioning_mode?: ProvisioningMode;
  provisioning_state?: ProvisioningState;
  source_engine_type?: 'postgresql' | 'mongodb' | 'mysql' | 'mariadb';
}

export interface SourceObjectMappingContext extends SourceObjectRow {
  registry_id: number;
}

export interface ShadowBindingRow {
  id: number;
  binding_code: string;
  source_object_id: number;
  object_code: string;
  registry_id?: number | null;
  source_db: string;
  source_type: 'mongodb' | 'mysql' | 'postgresql';
  source_table: string;
  shadow_schema: string;
  shadow_table: string;
  physical_table_fqn: string;
  write_mode: 'upsert' | 'append' | 'replace';
  ddl_status: 'pending' | 'created' | 'failed' | 'drifted';
  is_active: boolean;
  recon_drift: number;
  last_recon_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface MappingRule {
  id: number;
  source_object_id?: number;
  master_binding_id?: number | null;
  source_database?: string | null;
  source_schema?: string | null;
  source_namespace?: string | null;
  source_table: string;
  shadow_schema?: string | null;
  shadow_table?: string | null;
  source_field: string;
  source_path?: string | null;
  target_column: string;
  data_type: string;
  status: 'pending' | 'approved' | 'rejected';
  rule_type: 'system' | 'discovered' | 'mapping';
  is_active: boolean;
  is_enriched: boolean;
  source_format?: string;
  transform_fn?: string | null;
  is_nullable?: boolean;
  notes?: string | null;
  created_by: string | null;
  created_at: string;
  updated_at?: string;
}

export interface SchemaChangeLog {
  id: number;
  table_name: string;
  source_db: string | null;
  change_type: string;
  field_name: string | null;
  sql_executed: string;
  status: string;
  executed_by: string;
  executed_at: string;
}

export interface RegistryStats {
  total: number;
  by_source_db: Record<string, number>;
  by_sync_engine: Record<string, number>;
  by_priority: Record<string, number>;
  tables_created: number;
}

export interface SourceObjectStats {
  total: number;
  by_source_db: Record<string, number>;
  by_sync_engine: Record<string, number>;
  by_priority: Record<string, number>;
  tables_created: number;
}

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  token_type: string;
  user: {
    id: number;
    username: string;
    email: string;
    full_name: string;
    role: string;
  };
}
