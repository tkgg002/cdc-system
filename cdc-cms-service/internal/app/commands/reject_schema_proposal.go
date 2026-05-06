package commands

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// RejectSchemaProposalCommand stamps a schema_proposal row as rejected
// with CAS guard (status='pending'). Idempotent: replay returns
// ErrSchemaProposalNotPendingOrNotFound if the row was already moved
// off pending by an earlier reviewer.
type RejectSchemaProposalCommand struct {
	ports.SyncCommandMixin
	ProposalID      string `json:"proposal_id"`
	RejectionReason string `json:"rejection_reason"`
	ReviewedBy      string `json:"reviewed_by"`
}

func (RejectSchemaProposalCommand) Type() string { return "schema-proposal.reject" }

var ErrSchemaProposalNotPendingOrNotFound = errors.New("schema_proposal_not_pending_or_not_found")

func (c RejectSchemaProposalCommand) Validate() error {
	if c.ProposalID == "" {
		return errors.New("proposal_id required")
	}
	if c.RejectionReason == "" {
		return errors.New("rejection_reason required")
	}
	return nil
}

type RejectSchemaProposalHandler struct {
	db *gorm.DB
}

func NewRejectSchemaProposalHandler(db *gorm.DB) *RejectSchemaProposalHandler {
	return &RejectSchemaProposalHandler{db: db}
}

func (h *RejectSchemaProposalHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(RejectSchemaProposalCommand)
	if !ok {
		return nil, errors.New("schema-proposal.reject: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("schema proposal store not ready")
	}
	res := h.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.schema_proposal
		    SET status = 'rejected',
		        reviewed_by = ?,
		        reviewed_at = NOW(),
		        rejection_reason = ?,
		        updated_at = NOW()
		  WHERE id = ? AND status = 'pending'`,
		cmd.ReviewedBy, cmd.RejectionReason, cmd.ProposalID,
	)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrSchemaProposalNotPendingOrNotFound
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status": "rejected",
		"id":     cmd.ProposalID,
	})
	return body, nil
}
