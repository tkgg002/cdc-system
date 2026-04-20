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
  sync_engine: 'airbyte' | 'debezium' | 'both';
  sync_interval: string;
  priority: 'critical' | 'high' | 'normal' | 'low';
  primary_key_field: string;
  primary_key_type: string;
  is_active: boolean;
  is_table_created: boolean;
  airbyte_connection_id: string | null;
  airbyte_source_id: string | null;
  airbyte_destination_id: string | null;
  airbyte_destination_name: string | null;
  created_at: string;
  updated_at: string;
  notes: string | null;
}

export interface MappingRule {
  id: number;
  source_table: string;
  source_field: string;
  target_column: string;
  data_type: string;
  status: 'pending' | 'approved' | 'rejected';
  rule_type: 'system' | 'discovered' | 'mapping';
  is_active: boolean;
  is_enriched: boolean;
  created_by: string | null;
  created_at: string;
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
