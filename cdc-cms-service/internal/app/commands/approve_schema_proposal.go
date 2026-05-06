package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// ApproveSchemaProposalCommand applies a pending schema proposal:
// shadow layer → ALTER TABLE on resolved shadow_schema; master layer
// → ALTER TABLE public + INSERT cdc_mapping_rules. Final UPDATE
// stamps the proposal approved. Failure path best-effort marks the
// proposal failed and returns ErrSchemaProposalApplyFailed.
type ApproveSchemaProposalCommand struct {
	ports.SyncCommandMixin
	ProposalID          int64   `json:"proposal_id"`
	OverrideDataType    *string `json:"override_data_type,omitempty"`
	OverrideJSONPath    *string `json:"override_jsonpath,omitempty"`
	OverrideTransformFn *string `json:"override_transform_fn,omitempty"`
	Reason              string  `json:"reason"`
	ReviewedBy          string  `json:"reviewed_by"`
}

func (ApproveSchemaProposalCommand) Type() string { return "schema-proposal.approve" }

var (
	ErrSchemaProposalNotFound        = errors.New("schema_proposal_not_found")
	ErrSchemaProposalNotPending      = errors.New("schema_proposal_not_pending")
	ErrSchemaProposalInvalidDataType = errors.New("schema_proposal_invalid_data_type")
	ErrSchemaProposalInvalidIdent    = errors.New("schema_proposal_invalid_identifiers")
	ErrSchemaProposalApplyFailed     = errors.New("schema_proposal_apply_failed")
)

var (
	approveIdentRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	approveColumnRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
	approveTypeRe   = regexp.MustCompile(`^(SMALLINT|INTEGER|BIGINT|REAL|DOUBLE PRECISION|BOOLEAN|DATE|TIME|TIMESTAMP|TIMESTAMPTZ|INTERVAL|JSON|JSONB|UUID|INET|CIDR|MACADDR|BYTEA|TEXT|CHAR\([1-9][0-9]{0,7}\)|VARCHAR\([1-9][0-9]{0,7}\)|NUMERIC\([1-9][0-9]?,[0-9][0-9]?\)|DECIMAL\([1-9][0-9]?,[0-9][0-9]?\))$`)
)

func (c ApproveSchemaProposalCommand) Validate() error {
	if c.ProposalID <= 0 {
		return errors.New("proposal_id required")
	}
	if c.ReviewedBy == "" {
		return errors.New("reviewed_by required")
	}
	return nil
}

type ApproveSchemaProposalHandler struct {
	db *gorm.DB
}

func NewApproveSchemaProposalHandler(db *gorm.DB) *ApproveSchemaProposalHandler {
	return &ApproveSchemaProposalHandler{db: db}
}

type approveProposalRow struct {
	ID                  int64   `gorm:"column:id"`
	TableName           string  `gorm:"column:table_name"`
	TableLayer          string  `gorm:"column:table_layer"`
	ColumnName          string  `gorm:"column:column_name"`
	ProposedDataType    string  `gorm:"column:proposed_data_type"`
	ProposedJSONPath    *string `gorm:"column:proposed_jsonpath"`
	ProposedTransformFn *string `gorm:"column:proposed_transform_fn"`
	Status              string  `gorm:"column:status"`
}

func (h *ApproveSchemaProposalHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(ApproveSchemaProposalCommand)
	if !ok {
		return nil, errors.New("schema-proposal.approve: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("schema proposal store not ready")
	}

	var row approveProposalRow
	if err := h.db.WithContext(ctx).Table("cdc_system.schema_proposal").
		Select("id, table_name, table_layer, column_name, proposed_data_type, proposed_jsonpath, proposed_transform_fn, status").
		Where("id = ?", cmd.ProposalID).Scan(&row).Error; err != nil {
		return nil, err
	}
	if row.ID == 0 {
		return nil, ErrSchemaProposalNotFound
	}
	if row.Status != "pending" {
		return nil, ErrSchemaProposalNotPending
	}

	finalType := row.ProposedDataType
	if cmd.OverrideDataType != nil && *cmd.OverrideDataType != "" {
		finalType = *cmd.OverrideDataType
	}
	if !approveTypeRe.MatchString(finalType) {
		return nil, ErrSchemaProposalInvalidDataType
	}
	if !approveIdentRe.MatchString(row.TableName) || !approveColumnRe.MatchString(row.ColumnName) {
		return nil, ErrSchemaProposalInvalidIdent
	}

	txErr := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		switch row.TableLayer {
		case "shadow":
			var shadowSchema string
			if err := tx.Raw(
				`SELECT shadow_schema FROM cdc_system.shadow_binding
				  WHERE shadow_table = ? AND is_active = true LIMIT 1`,
				row.TableName,
			).Scan(&shadowSchema).Error; err != nil {
				return fmt.Errorf("binding lookup: %w", err)
			}
			if shadowSchema == "" {
				return fmt.Errorf("binding_not_found for shadow table %q", row.TableName)
			}
			if !approveIdentRe.MatchString(shadowSchema) {
				return fmt.Errorf("invalid_shadow_schema: %q", shadowSchema)
			}
			stmt := fmt.Sprintf(
				`ALTER TABLE %q.%q ADD COLUMN IF NOT EXISTS %q %s`,
				shadowSchema, row.TableName, row.ColumnName, finalType,
			)
			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("alter shadow: %w", err)
			}
		case "master":
			stmt := fmt.Sprintf(
				`ALTER TABLE public.%q ADD COLUMN IF NOT EXISTS %q %s`,
				row.TableName, row.ColumnName, finalType,
			)
			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("alter master: %w", err)
			}
			finalPath := ""
			if cmd.OverrideJSONPath != nil {
				finalPath = *cmd.OverrideJSONPath
			} else if row.ProposedJSONPath != nil {
				finalPath = *row.ProposedJSONPath
			}
			finalFn := ""
			if cmd.OverrideTransformFn != nil {
				finalFn = *cmd.OverrideTransformFn
			} else if row.ProposedTransformFn != nil {
				finalFn = *row.ProposedTransformFn
			}
			insertSQL := `INSERT INTO cdc_mapping_rules
			  (source_table, master_table, source_field, target_column, data_type,
			   source_format, jsonpath, transform_fn, is_active, status,
			   approved_by_admin, approved_at, created_by, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, 'debezium_after', NULLIF(?, ''), NULLIF(?, ''),
			         true, 'approved', true, NOW(), ?, NOW(), NOW())
			 ON CONFLICT DO NOTHING`
			if err := tx.Exec(insertSQL,
				row.TableName, row.TableName,
				row.ColumnName, row.ColumnName, finalType,
				finalPath, finalFn, cmd.ReviewedBy,
			).Error; err != nil {
				return fmt.Errorf("insert mapping_rule: %w", err)
			}
		default:
			return fmt.Errorf("invalid table_layer: %s", row.TableLayer)
		}

		return tx.Exec(
			`UPDATE cdc_system.schema_proposal
			    SET status = 'approved',
			        reviewed_by = ?,
			        reviewed_at = NOW(),
			        applied_at = NOW(),
			        override_data_type = ?,
			        override_jsonpath = ?,
			        override_transform_fn = ?,
			        error_message = NULL,
			        updated_at = NOW()
			  WHERE id = ?`,
			cmd.ReviewedBy, cmd.OverrideDataType, cmd.OverrideJSONPath,
			cmd.OverrideTransformFn, cmd.ProposalID,
		).Error
	})
	if txErr != nil {
		_ = h.db.WithContext(ctx).Exec(
			`UPDATE cdc_system.schema_proposal
			    SET status='failed', error_message=?, updated_at=NOW()
			  WHERE id=?`,
			txErr.Error(), cmd.ProposalID,
		).Error
		return nil, errors.Join(ErrSchemaProposalApplyFailed, txErr)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"status":     "approved",
		"id":         cmd.ProposalID,
		"table":      row.TableName,
		"column":     row.ColumnName,
		"final_type": finalType,
	})
	return body, nil
}
