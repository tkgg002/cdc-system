package service

import (
	"context"
	"fmt"
	"time"

	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/pkgs/natsconn"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ApprovalService struct {
	db            *gorm.DB
	pendingRepo   *repository.PendingFieldRepo
	mappingRepo   *repository.MappingRuleRepo
	schemaLogRepo *repository.SchemaLogRepo
	registryRepo  *repository.RegistryRepo
	natsClient    *natsconn.NatsClient
	logger        *zap.Logger
}

func NewApprovalService(
	db *gorm.DB,
	pendingRepo *repository.PendingFieldRepo,
	mappingRepo *repository.MappingRuleRepo,
	schemaLogRepo *repository.SchemaLogRepo,
	registryRepo *repository.RegistryRepo,
	nats *natsconn.NatsClient,
	logger *zap.Logger,
) *ApprovalService {
	return &ApprovalService{
		db: db, pendingRepo: pendingRepo, mappingRepo: mappingRepo,
		schemaLogRepo: schemaLogRepo, registryRepo: registryRepo,
		natsClient: nats, logger: logger,
	}
}

// ApproveRequest is the request body for approving a schema change
type ApproveRequest struct {
	TargetColumnName string `json:"target_column_name" example:"loyalty_points"`
	FinalType        string `json:"final_type" example:"INTEGER"`
	ApprovalNotes    string `json:"approval_notes" example:"Approved for phase 1"`
}

// RejectRequest is the request body for rejecting a schema change
type RejectRequest struct {
	RejectionReason string `json:"rejection_reason" example:"Not needed for MVP"`
}

func (s *ApprovalService) Approve(ctx context.Context, id uint, req ApproveRequest, username string) (*model.PendingField, error) {
	pf, err := s.pendingRepo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("pending field not found: %w", err)
	}
	if pf.Status != "pending" {
		return nil, fmt.Errorf("field is already %s", pf.Status)
	}

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
			pf.TblName, req.TargetColumnName, req.FinalType)

		startTime := time.Now()
		alterErr := tx.Exec(alterSQL).Error
		duration := time.Since(startTime)

		newDef := fmt.Sprintf("%s %s", req.TargetColumnName, req.FinalType)
		durationMS := int(duration.Milliseconds())
		schemaLog := &model.SchemaChangeLog{
			TblName:             pf.TblName,
			SourceDB:            pf.SourceDB,
			ChangeType:          "ADD_COLUMN",
			FieldName:           &pf.FieldName,
			NewDefinition:       &newDef,
			SQLExecuted:         alterSQL,
			ExecutionDurationMS: &durationMS,
			PendingFieldID:      &pf.ID,
			ExecutedBy:          username,
			ExecutedAt:          time.Now(),
			RollbackSQL:         strPtr(fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", pf.TblName, req.TargetColumnName)),
		}

		if alterErr != nil {
			schemaLog.Status = "failed"
			errMsg := alterErr.Error()
			schemaLog.ErrorMessage = &errMsg
			s.schemaLogRepo.Create(ctx, schemaLog)
			return fmt.Errorf("ALTER TABLE failed: %w", alterErr)
		}
		schemaLog.Status = "success"
		s.schemaLogRepo.Create(ctx, schemaLog)

		rule := &model.MappingRule{
			SourceTable:  pf.TblName,
			SourceField:  pf.FieldName,
			TargetColumn: req.TargetColumnName,
			DataType:     req.FinalType,
			IsActive:     true,
			CreatedBy:    &username,
			UpdatedBy:    &username,
		}
		if err := tx.Create(rule).Error; err != nil {
			return fmt.Errorf("create mapping rule: %w", err)
		}

		now := time.Now()
		pf.Status = "approved"
		pf.ReviewedBy = &username
		pf.ApprovedAt = &now
		pf.TargetColumnName = &req.TargetColumnName
		pf.FinalType = &req.FinalType
		if req.ApprovalNotes != "" {
			pf.ApprovalNotes = &req.ApprovalNotes
		}
		return tx.Save(pf).Error
	})
	if err != nil {
		return nil, err
	}

	// Context propagation: gửi user_id + metadata cho Worker audit log
	s.natsClient.PublishReload(pf.TblName, username, "approve", pf.FieldName)
	go s.triggerAirbyteRefresh(pf.TblName)

	return pf, nil
}

func (s *ApprovalService) Reject(ctx context.Context, id uint, req RejectRequest, username string) (*model.PendingField, error) {
	pf, err := s.pendingRepo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("pending field not found: %w", err)
	}
	if pf.Status != "pending" {
		return nil, fmt.Errorf("field is already %s", pf.Status)
	}

	now := time.Now()
	pf.Status = "rejected"
	pf.ReviewedBy = &username
	pf.ReviewedAt = &now
	pf.RejectionReason = &req.RejectionReason

	if err := s.pendingRepo.Update(ctx, pf); err != nil {
		return nil, err
	}
	return pf, nil
}

func (s *ApprovalService) triggerAirbyteRefresh(tableName string) {
	ctx := context.Background()
	// schema is refreshed via signal (cdc.cmd.debezium-signal) or on connector
	// restart — see Command Center UI.
	_, _ = s.registryRepo.GetByTargetTable(ctx, tableName)
}

func strPtr(s string) *string { return &s }
