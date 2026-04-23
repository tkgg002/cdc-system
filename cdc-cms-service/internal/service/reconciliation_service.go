package service

import (
	"context"

	"cdc-cms-service/internal/repository"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// pipeline was retired (plan v2 §R3). The CDC v1.25 architecture uses
// cdc_internal.table_registry + Debezium Connect REST for source-side
// reconciliation — see Command Center at /api/v1/system/connectors.
//
// Future work: reimplement reconcile() against Debezium connector status
// + Kafka consumer-group lag (separate initiative).
type ReconciliationService struct {
	repo        *repository.RegistryRepo
	mappingRepo *repository.MappingRuleRepo
	db          *gorm.DB
	logger      *zap.Logger
	stopCh      chan struct{}
}

func NewReconciliationService(
	repo *repository.RegistryRepo,
	mappingRepo *repository.MappingRuleRepo,
	db *gorm.DB,
	logger *zap.Logger,
) *ReconciliationService {
	return &ReconciliationService{
		repo:        repo,
		mappingRepo: mappingRepo,
		db:          db,
		logger:      logger,
		stopCh:      make(chan struct{}),
	}
}

// Start() callers in server.go + cmd/server/main.go compile unchanged.
func (s *ReconciliationService) Start(ctx context.Context) {
	s.logger.Info("ReconciliationService is a no-op after Airbyte retirement (plan v2 §R3)")
	<-ctx.Done()
}

// Stop is a no-op.
func (s *ReconciliationService) Stop() {
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}
}
