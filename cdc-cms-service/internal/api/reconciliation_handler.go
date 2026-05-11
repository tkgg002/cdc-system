package api

import (
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
)

type ReconciliationHandler struct {
	reader         queries.ReconReader
	nats           *natsconn.NatsClient
	bus            ports.CommandBus
	listLatestQ    *queries.ListLatestReportsHandler
	getHistoryQ    *queries.GetTableHistoryHandler
	listFailedQ    *queries.ListFailedLogsHandler
	activityLogger *persistence.ActivityLogger
}

func NewReconciliationHandler(
	reader queries.ReconReader,
	nats *natsconn.NatsClient,
	bus ports.CommandBus,
	listLatestQ *queries.ListLatestReportsHandler,
	getHistoryQ *queries.GetTableHistoryHandler,
	listFailedQ *queries.ListFailedLogsHandler,
	activityLogger *persistence.ActivityLogger,
) *ReconciliationHandler {
	return &ReconciliationHandler{
		reader:         reader,
		nats:           nats,
		bus:            bus,
		listLatestQ:    listLatestQ,
		getHistoryQ:    getHistoryQ,
		listFailedQ:    listFailedQ,
		activityLogger: activityLogger,
	}
}

// ReportRow + FailedLogRow are type aliases so external callers /
// Swagger docs continue to see the legacy names; the canonical
// definitions now live in `internal/app/queries/recon_read_models.go`.
type ReportRow = queries.LatestReportRow
type FailedLogRow = queries.FailedLogRow

type reconScopeRequest struct {
	Table           string `json:"table"`
	SourceDatabase  string `json:"source_database"`
	SourceSchema    string `json:"source_schema"`
	SourceNamespace string `json:"source_namespace"`
	SourceTable     string `json:"source_table"`
	ShadowSchema    string `json:"shadow_schema"`
	ShadowTable     string `json:"shadow_table"`
}

func (h *ReconciliationHandler) resolveTargetTable(c *fiber.Ctx, scope reconScopeRequest) (string, error) {
	if t := queries.TrimReconValue(scope.Table); t != "" {
		return t, nil
	}
	return h.reader.ResolveTargetTableByScope(c.UserContext(), queries.ReconScopeFilter{
		SourceDatabase:  scope.SourceDatabase,
		SourceSchema:    scope.SourceSchema,
		SourceNamespace: scope.SourceNamespace,
		SourceTable:     scope.SourceTable,
		ShadowSchema:    scope.ShadowSchema,
		ShadowTable:     scope.ShadowTable,
	})
}
