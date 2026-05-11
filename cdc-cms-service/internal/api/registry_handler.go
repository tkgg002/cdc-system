package api

import (
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/pkgs/natsconn"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RegistryHandler struct {
	repo           ports.RegistryRepo
	db             *gorm.DB
	natsClient     *natsconn.NatsClient
	bus            ports.CommandBus
	automator      *persistence.ShadowAutomator
	v2sync         *persistence.SourceObjectV2SyncService
	activityLogger *persistence.ActivityLogger
	logger         *zap.Logger
	syncHealthQ    *queries.GetSyncHealthHandler
	bridgeReader   queries.BridgeStatusReader
}

func NewRegistryHandler(
	repo ports.RegistryRepo,
	db *gorm.DB,
	nats *natsconn.NatsClient,
	bus ports.CommandBus,
	automator *persistence.ShadowAutomator,
	v2sync *persistence.SourceObjectV2SyncService,
	activityLogger *persistence.ActivityLogger,
	logger *zap.Logger,
	syncHealthQ *queries.GetSyncHealthHandler,
	bridgeReader queries.BridgeStatusReader,
) *RegistryHandler {
	return &RegistryHandler{
		repo:           repo,
		db:             db,
		natsClient:     nats,
		bus:            bus,
		automator:      automator,
		v2sync:         v2sync,
		activityLogger: activityLogger,
		logger:         logger,
		syncHealthQ:    syncHealthQ,
		bridgeReader:   bridgeReader,
	}
}
