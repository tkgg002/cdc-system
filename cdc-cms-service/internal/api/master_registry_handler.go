package api

import (
	"encoding/json"
	"regexp"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/pkgs/natsconn"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type MasterRegistryHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
	listQ  *queries.ListMastersHandler
	bus    ports.CommandBus
}

func NewMasterRegistryHandler(db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger, listQ *queries.ListMastersHandler, bus ports.CommandBus) *MasterRegistryHandler {
	return &MasterRegistryHandler{db: db, nats: nats, logger: logger, listQ: listQ, bus: bus}
}

var (
	masterNameRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	namespaceName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

type CreateRequest struct {
	MasterName           string          `json:"master_name"`
	MasterSchema         string          `json:"master_schema"`
	MasterConnectionCode string          `json:"master_connection_code"`
	SourceShadow         string          `json:"source_shadow"`
	SourceDatabase       string          `json:"source_database"`
	SourceSchema         string          `json:"source_schema"`
	SourceNamespace      string          `json:"source_namespace"`
	SourceTable          string          `json:"source_table"`
	ShadowSchema         string          `json:"shadow_schema"`
	ShadowTable          string          `json:"shadow_table"`
	TransformType        string          `json:"transform_type"`
	Spec                 json.RawMessage `json:"spec"`
	Reason               string          `json:"reason"`
}

type ApproveRequest struct {
	Reason string `json:"reason"`
}

type SwapRequest struct {
	NewTableName string `json:"new_table_name"`
	Reason       string `json:"reason"`
}
