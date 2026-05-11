package admin

// RegisterSourceRequest — body cho POST /v2/sources/register.
// source_engine_type phải match check constraint DB: postgresql|mariadb|mysql|mongodb|clickhouse.
// source_object_type: table|collection|view.
type RegisterSourceRequest struct {
	ObjectCode        string                 `json:"object_code"          binding:"required"`
	SourceEngineType  string                 `json:"source_engine_type"   binding:"required,oneof=postgresql mongodb mariadb mysql"`
	SyncEngine        string                 `json:"sync_engine"          binding:"required,oneof=debezium"`
	SourceObjectName  string                 `json:"source_object_name"   binding:"required"`
	SourceObjectType  string                 `json:"source_object_type"   binding:"omitempty,oneof=table collection view"`
	PrimaryKeyField   string                 `json:"primary_key_field"`
	SourceLocator     map[string]interface{} `json:"source_locator"       binding:"required"`
	TargetMasterTable string                 `json:"target_master_table"  binding:"required"`
	Notes             string                 `json:"notes"`
}

// RegisterSourceResponse — response của POST /v2/sources/register.
// ProvisioningState: "active" (200) | "step2_failed" | "step3_failed" (207).
type RegisterSourceResponse struct {
	SourceObjectID    int64    `json:"source_object_id"`
	ProvisioningState string   `json:"provisioning_state"`
	StepsCompleted    []string `json:"steps_completed"`
	LastStepError     string   `json:"last_step_error,omitempty"`
	Warnings          []string `json:"warnings,omitempty"`
}
