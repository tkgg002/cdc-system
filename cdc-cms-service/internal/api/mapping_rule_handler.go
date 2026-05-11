package api

import (
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/pkgs/natsconn"
)

type MappingRuleHandler struct {
	natsClient   *natsconn.NatsClient
	bus          ports.CommandBus
	listQuery    *queries.ListMappingRulesHandler
	resolveQuery *queries.ResolveMappingScopeHandler
	ruleRepo     ports.MappingRuleRepo
}

func NewMappingRuleHandler(nats *natsconn.NatsClient, bus ports.CommandBus, listQuery *queries.ListMappingRulesHandler, resolveQuery *queries.ResolveMappingScopeHandler, ruleRepo ports.MappingRuleRepo) *MappingRuleHandler {
	return &MappingRuleHandler{natsClient: nats, bus: bus, listQuery: listQuery, resolveQuery: resolveQuery, ruleRepo: ruleRepo}
}
