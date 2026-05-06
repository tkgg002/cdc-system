// Package queries — unit tests for the read-side use cases.
//
// Strategy: each query handler is exercised against a hand-rolled stub
// implementing its reader port. Tests cover (a) the happy path,
// (b) error propagation from the reader, and (c) the paging clamp logic
// inlined in the four paginated handlers (page<1, size<1, size>cap).
package queries

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"cdc-cms-service/internal/domain/job"
	"cdc-cms-service/internal/domain/mapping"
	infrahttp "cdc-cms-service/internal/infra/http"
	"cdc-cms-service/internal/model"
)

var errBoom = errors.New("boom")

// ---------------------------------------------------------------------
// Query.Type() smokescreen — every Query is an empty/struct shape; calling
// Type() on a zero value covers the trivial getters in one shot.
// ---------------------------------------------------------------------
func TestQueryTypes(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"mapping.list", ListMappingRulesQuery{}.Type(), "mapping.list"},
		{"source.list", ListSourceObjectsQuery{}.Type(), "source.list"},
		{"source.mapping_context", GetSourceObjectMappingContextQuery{}.Type(), "source.mapping_context"},
		{"master.list", ListMastersQuery{}.Type(), "master.list"},
		{"recon.list_latest", ListLatestReportsQuery{}.Type(), "recon.list_latest"},
		{"recon.history", GetTableHistoryQuery{}.Type(), "recon.history"},
		{"recon.failed_logs", ListFailedLogsQuery{}.Type(), "recon.failed_logs"},
		{"sync.health", GetSyncHealthQuery{}.Type(), "sync.health"},
		{"connectors.list", ListConnectorsQuery{}.Type(), "connectors.list"},
		{"connectors.get", GetConnectorQuery{}.Type(), "connectors.get"},
		{"connectors.plugins", ListConnectorPluginsQuery{}.Type(), "connectors.plugins"},
		{"activitylog.list", ListActivityLogsQuery{}.Type(), "activitylog.list"},
		{"activitylog.stats", GetActivityStatsQuery{}.Type(), "activitylog.stats"},
		{"transmute_schedules.list", ListTransmuteSchedulesQuery{}.Type(), "transmute_schedules.list"},
		{"sources.list", ListSourcesQuery{}.Type(), "sources.list"},
		{"sources.get", GetSourceQuery{}.Type(), "sources.get"},
		{"wizard.session.get", GetWizardSessionQuery{}.Type(), "wizard.session.get"},
		{"wizard.progress.get", GetWizardProgressQuery{}.Type(), "wizard.progress.get"},
		{"worker_schedules.list", ListWorkerSchedulesQuery{}.Type(), "worker_schedules.list"},
		{"job.get", GetJobQuery{}.Type(), "job.get"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %q want %q", c.name, c.got, c.want)
		}
	}
}

// ---------------------------------------------------------------------
// MappingRuleRepo — full ports interface. Only ListPaginated is exercised
// here; the remaining methods are stubbed to satisfy the contract.
// ---------------------------------------------------------------------

type stubMappingRuleRepo struct {
	rules []mapping.Rule
	total int64
	err   error
}

func (s *stubMappingRuleRepo) List(context.Context, mapping.Filter) ([]mapping.Rule, error) {
	return s.rules, s.err
}
func (s *stubMappingRuleRepo) ListPaginated(_ context.Context, _ mapping.Filter, _ int, _ int) ([]mapping.Rule, int64, error) {
	return s.rules, s.total, s.err
}
func (s *stubMappingRuleRepo) GetByID(context.Context, int64) (*mapping.Rule, error) {
	return nil, s.err
}
func (s *stubMappingRuleRepo) Save(context.Context, *mapping.Rule) error { return s.err }
func (s *stubMappingRuleRepo) UpdateStatus(context.Context, int64, mapping.Status) error {
	return s.err
}
func (s *stubMappingRuleRepo) BatchUpdateStatus(context.Context, []int64, mapping.Status) (int64, error) {
	return 0, s.err
}

func TestListMappingRulesHandler_Happy(t *testing.T) {
	repo := &stubMappingRuleRepo{
		rules: []mapping.Rule{{ID: 1}, {ID: 2}},
		total: 7,
	}
	h := NewListMappingRulesHandler(repo)

	res, err := h.Handle(context.Background(), ListMappingRulesQuery{Page: 2, PageSize: 25})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Data) != 2 || res.Total != 7 || res.Page != 2 || res.PageSize != 25 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestListMappingRulesHandler_Error(t *testing.T) {
	repo := &stubMappingRuleRepo{err: errBoom}
	h := NewListMappingRulesHandler(repo)

	if _, err := h.Handle(context.Background(), ListMappingRulesQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("want errBoom, got %v", err)
	}
}

func TestListMappingRulesHandler_PageClamp(t *testing.T) {
	repo := &stubMappingRuleRepo{}
	h := NewListMappingRulesHandler(repo)

	// page<1 clamps to 1; size<1 clamps to default 50.
	res, _ := h.Handle(context.Background(), ListMappingRulesQuery{Page: 0, PageSize: 0})
	if res.Page != 1 || res.PageSize != 50 {
		t.Fatalf("low clamp: got page=%d size=%d", res.Page, res.PageSize)
	}
	// size>200 clamps to default 50.
	res, _ = h.Handle(context.Background(), ListMappingRulesQuery{Page: -5, PageSize: 999})
	if res.Page != 1 || res.PageSize != 50 {
		t.Fatalf("high clamp: got page=%d size=%d", res.Page, res.PageSize)
	}
}

// ---------------------------------------------------------------------
// SourceObjectReader stub.
// ---------------------------------------------------------------------

type stubSourceObjectReader struct {
	listRows []SourceObjectListItem
	listN    int64
	listErr  error

	mapCtx    *SourceObjectMappingContextReadModel
	mapCtxErr error

	gotPage, gotSize int
	gotRegistryID   uint64
}

func (s *stubSourceObjectReader) ListEnriched(_ context.Context, _ SourceObjectListFilter, page, size int) ([]SourceObjectListItem, int64, error) {
	s.gotPage, s.gotSize = page, size
	return s.listRows, s.listN, s.listErr
}
func (s *stubSourceObjectReader) GetMappingContextByRegistryID(_ context.Context, rid uint64) (*SourceObjectMappingContextReadModel, error) {
	s.gotRegistryID = rid
	return s.mapCtx, s.mapCtxErr
}

func TestListSourceObjectsHandler_HappyAndClamp(t *testing.T) {
	r := &stubSourceObjectReader{
		listRows: []SourceObjectListItem{{ID: 1}, {ID: 2}},
		listN:    9,
	}
	h := NewListSourceObjectsHandler(r)

	// happy
	res, err := h.Handle(context.Background(), ListSourceObjectsQuery{Page: 3, PageSize: 50})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Total != 9 || res.Page != 3 || len(res.Data) != 2 {
		t.Fatalf("happy: got %+v", res)
	}
	if r.gotPage != 3 || r.gotSize != 50 {
		t.Fatalf("reader paging: page=%d size=%d", r.gotPage, r.gotSize)
	}

	// clamp low → page=1, size=20 default
	res, _ = h.Handle(context.Background(), ListSourceObjectsQuery{Page: 0, PageSize: 0})
	if res.Page != 1 || r.gotSize != 20 {
		t.Fatalf("low clamp: page=%d size=%d", res.Page, r.gotSize)
	}

	// clamp high → size capped at 500
	_, _ = h.Handle(context.Background(), ListSourceObjectsQuery{Page: -1, PageSize: 9999})
	if r.gotSize != 500 {
		t.Fatalf("high clamp: size=%d (want 500)", r.gotSize)
	}
}

func TestListSourceObjectsHandler_Error(t *testing.T) {
	r := &stubSourceObjectReader{listErr: errBoom}
	h := NewListSourceObjectsHandler(r)

	if _, err := h.Handle(context.Background(), ListSourceObjectsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("want errBoom, got %v", err)
	}
}

func TestGetSourceObjectMappingContextHandler(t *testing.T) {
	want := &SourceObjectMappingContextReadModel{ID: 42, RegistryID: 17}
	r := &stubSourceObjectReader{mapCtx: want}
	h := NewGetSourceObjectMappingContextHandler(r)

	got, err := h.Handle(context.Background(), GetSourceObjectMappingContextQuery{RegistryID: 17})
	if err != nil || got == nil || got.ID != 42 {
		t.Fatalf("happy: got=%+v err=%v", got, err)
	}
	if r.gotRegistryID != 17 {
		t.Fatalf("registry id passthrough: got %d", r.gotRegistryID)
	}

	// nil-pass-through (legacy 404 contract)
	r2 := &stubSourceObjectReader{}
	h2 := NewGetSourceObjectMappingContextHandler(r2)
	got, err = h2.Handle(context.Background(), GetSourceObjectMappingContextQuery{})
	if err != nil || got != nil {
		t.Fatalf("nil pass: got=%v err=%v", got, err)
	}

	// error
	r3 := &stubSourceObjectReader{mapCtxErr: errBoom}
	h3 := NewGetSourceObjectMappingContextHandler(r3)
	if _, err := h3.Handle(context.Background(), GetSourceObjectMappingContextQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("want errBoom, got %v", err)
	}
}

// ---------------------------------------------------------------------
// MasterReader stub.
// ---------------------------------------------------------------------

type stubMasterReader struct {
	rows []MasterListItem
	err  error
}

func (s *stubMasterReader) ListEnriched(context.Context) ([]MasterListItem, error) {
	return s.rows, s.err
}

func TestListMastersHandler(t *testing.T) {
	r := &stubMasterReader{rows: []MasterListItem{{ID: 1}, {ID: 2}, {ID: 3}}}
	h := NewListMastersHandler(r)

	res, err := h.Handle(context.Background(), ListMastersQuery{})
	if err != nil || res.Count != 3 || len(res.Data) != 3 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubMasterReader{err: errBoom}
	h2 := NewListMastersHandler(r2)
	if _, err := h2.Handle(context.Background(), ListMastersQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// ReconReader stub — covers ListLatest, GetTableHistory, ListFailedLogs.
// ---------------------------------------------------------------------

type stubReconReader struct {
	latest    []LatestReportRow
	latestErr error

	hist     []model.ReconciliationReport
	histN    int64
	histErr  error
	histPage int
	histSize int
	histTbl  string

	failed     []FailedLogRow
	failedN    int64
	failedErr  error
	failedPage int
	failedSize int
	failedF    FailedLogFilter
}

func (s *stubReconReader) ListLatest(context.Context) ([]LatestReportRow, error) {
	return s.latest, s.latestErr
}
func (s *stubReconReader) GetTableHistory(_ context.Context, table string, page, size int) ([]model.ReconciliationReport, int64, error) {
	s.histTbl, s.histPage, s.histSize = table, page, size
	return s.hist, s.histN, s.histErr
}
func (s *stubReconReader) ListFailedLogs(_ context.Context, f FailedLogFilter, page, size int) ([]FailedLogRow, int64, error) {
	s.failedF, s.failedPage, s.failedSize = f, page, size
	return s.failed, s.failedN, s.failedErr
}

func TestListLatestReportsHandler(t *testing.T) {
	r := &stubReconReader{latest: []LatestReportRow{{}, {}}}
	h := NewListLatestReportsHandler(r)
	res, err := h.Handle(context.Background(), ListLatestReportsQuery{})
	if err != nil || res.Count != 2 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubReconReader{latestErr: errBoom}
	h2 := NewListLatestReportsHandler(r2)
	if _, err := h2.Handle(context.Background(), ListLatestReportsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

func TestGetTableHistoryHandler_HappyAndClamp(t *testing.T) {
	r := &stubReconReader{hist: []model.ReconciliationReport{{}}, histN: 1}
	h := NewGetTableHistoryHandler(r)

	// happy
	res, err := h.Handle(context.Background(), GetTableHistoryQuery{Table: "orders", Page: 2, PageSize: 50})
	if err != nil || res.Page != 2 || r.histTbl != "orders" || r.histPage != 2 || r.histSize != 50 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	// page<1 clamps to 1; size<1 clamps to default 20
	_, _ = h.Handle(context.Background(), GetTableHistoryQuery{Page: 0, PageSize: 0})
	if r.histPage != 1 || r.histSize != 20 {
		t.Fatalf("low clamp: page=%d size=%d", r.histPage, r.histSize)
	}

	// size>100 clamps to default 20
	_, _ = h.Handle(context.Background(), GetTableHistoryQuery{Page: 1, PageSize: 999})
	if r.histSize != 20 {
		t.Fatalf("high clamp: size=%d", r.histSize)
	}
}

func TestGetTableHistoryHandler_Error(t *testing.T) {
	r := &stubReconReader{histErr: errBoom}
	h := NewGetTableHistoryHandler(r)
	if _, err := h.Handle(context.Background(), GetTableHistoryQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

func TestListFailedLogsHandler_HappyAndClamp(t *testing.T) {
	r := &stubReconReader{failed: []FailedLogRow{{}}, failedN: 1}
	h := NewListFailedLogsHandler(r)

	// happy + filter passthrough
	res, err := h.Handle(context.Background(), ListFailedLogsQuery{
		Filter:   FailedLogFilter{TargetTable: "x"},
		Page:     2,
		PageSize: 100,
	})
	if err != nil || res.Page != 2 || r.failedF.TargetTable != "x" || r.failedSize != 100 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	// page<1 → 1; size<1 → default 30
	_, _ = h.Handle(context.Background(), ListFailedLogsQuery{Page: 0, PageSize: 0})
	if r.failedPage != 1 || r.failedSize != 30 {
		t.Fatalf("low clamp: page=%d size=%d", r.failedPage, r.failedSize)
	}

	// size>200 → default 30
	_, _ = h.Handle(context.Background(), ListFailedLogsQuery{Page: 1, PageSize: 9999})
	if r.failedSize != 30 {
		t.Fatalf("high clamp: size=%d", r.failedSize)
	}
}

func TestListFailedLogsHandler_Error(t *testing.T) {
	r := &stubReconReader{failedErr: errBoom}
	h := NewListFailedLogsHandler(r)
	if _, err := h.Handle(context.Background(), ListFailedLogsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// SyncHealthReader stub.
// ---------------------------------------------------------------------

type stubSyncHealthReader struct {
	snap SyncHealthSnapshot
	err  error
}

func (s *stubSyncHealthReader) GetSyncHealth(context.Context) (SyncHealthSnapshot, error) {
	return s.snap, s.err
}

func TestGetSyncHealthHandler(t *testing.T) {
	r := &stubSyncHealthReader{snap: SyncHealthSnapshot{ActiveTables: 5, TotalRegistryCMS: 9}}
	h := NewGetSyncHealthHandler(r)

	res, err := h.Handle(context.Background(), GetSyncHealthQuery{})
	if err != nil || res.Snapshot.ActiveTables != 5 || res.Snapshot.TotalRegistryCMS != 9 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubSyncHealthReader{err: errBoom}
	h2 := NewGetSyncHealthHandler(r2)
	if _, err := h2.Handle(context.Background(), GetSyncHealthQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// ConnectorReader stub — most branchy handler in the package.
// ---------------------------------------------------------------------

type stubConnectorReader struct {
	names    []string
	namesErr error

	statuses    map[string]infrahttp.ConnectorStatusResp
	statusErrs  map[string]error

	configs    map[string]map[string]string
	configErrs map[string]error

	plugins    []map[string]any
	pluginsErr error
}

func (s *stubConnectorReader) ListNames(context.Context) ([]string, error) {
	return s.names, s.namesErr
}
func (s *stubConnectorReader) GetStatus(_ context.Context, name string) (infrahttp.ConnectorStatusResp, error) {
	if e, ok := s.statusErrs[name]; ok {
		return infrahttp.ConnectorStatusResp{}, e
	}
	return s.statuses[name], nil
}
func (s *stubConnectorReader) GetConfig(_ context.Context, name string) (map[string]string, error) {
	if e, ok := s.configErrs[name]; ok {
		return nil, e
	}
	return s.configs[name], nil
}
func (s *stubConnectorReader) ListPlugins(context.Context) ([]map[string]any, error) {
	return s.plugins, s.pluginsErr
}

func TestListConnectorsHandler_AllBranches(t *testing.T) {
	r := &stubConnectorReader{
		names: []string{"good", "no-status", "no-config", "neither"},
		statuses: map[string]infrahttp.ConnectorStatusResp{
			"good":      {Type: "source", Connector: infrahttp.ConnectorState{State: "RUNNING"}},
			"no-config": {Type: "sink", Connector: infrahttp.ConnectorState{State: "PAUSED"}},
		},
		statusErrs: map[string]error{
			"no-status": errBoom,
			"neither":   errBoom,
		},
		configs: map[string]map[string]string{
			"good":      {"connector.class": "io.debezium.MySql", "database.password": "secret"},
			"no-status": {"connector.class": "io.debezium.Pg"},
		},
		configErrs: map[string]error{
			"no-config": errBoom,
			"neither":   errBoom,
		},
	}
	h := NewListConnectorsHandler(r)
	res, err := h.Handle(context.Background(), ListConnectorsQuery{})
	if err != nil || res.Count != 4 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	// "good" — both succeed, FilterSafeConfig drops `database.password`.
	good := res.Data[0]
	if good.State != "RUNNING" || good.Type != "source" || good.Connector != "io.debezium.MySql" {
		t.Fatalf("good: %+v", good)
	}
	if good.Config["database.password"] != "***" {
		t.Fatalf("FilterSafeConfig should mask database.password as ***, got %v", good.Config)
	}

	// "no-status" — status block empty, config block populated.
	noStatus := res.Data[1]
	if noStatus.State != "" || noStatus.Connector != "io.debezium.Pg" {
		t.Fatalf("no-status: %+v", noStatus)
	}

	// "no-config" — status populated, config empty.
	noConfig := res.Data[2]
	if noConfig.State != "PAUSED" || noConfig.Connector != "" || noConfig.Config != nil {
		t.Fatalf("no-config: %+v", noConfig)
	}

	// "neither" — only Name set.
	neither := res.Data[3]
	if neither.State != "" || neither.Connector != "" {
		t.Fatalf("neither: %+v", neither)
	}
}

func TestListConnectorsHandler_NamesError(t *testing.T) {
	r := &stubConnectorReader{namesErr: errBoom}
	h := NewListConnectorsHandler(r)
	if _, err := h.Handle(context.Background(), ListConnectorsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

func TestGetConnectorHandler(t *testing.T) {
	r := &stubConnectorReader{
		statuses: map[string]infrahttp.ConnectorStatusResp{
			"alpha": {Type: "source", Connector: infrahttp.ConnectorState{State: "RUNNING"}},
		},
		configs: map[string]map[string]string{
			"alpha": {"connector.class": "x", "database.password": "p"},
		},
	}
	h := NewGetConnectorHandler(r)
	res, err := h.Handle(context.Background(), GetConnectorQuery{Name: "alpha"})
	if err != nil || res.Status.Type != "source" || res.Config["connector.class"] != "x" {
		t.Fatalf("happy: %+v err=%v", res, err)
	}
	if res.Config["database.password"] != "***" {
		t.Fatalf("FilterSafeConfig should mask password as ***, got %v", res.Config)
	}

	// Status error → propagate
	r2 := &stubConnectorReader{
		statusErrs: map[string]error{"alpha": errBoom},
	}
	h2 := NewGetConnectorHandler(r2)
	if _, err := h2.Handle(context.Background(), GetConnectorQuery{Name: "alpha"}); !errors.Is(err, errBoom) {
		t.Fatalf("status err: %v", err)
	}

	// Status OK + config error → swallowed (legacy contract)
	r3 := &stubConnectorReader{
		statuses:   map[string]infrahttp.ConnectorStatusResp{"alpha": {Type: "sink"}},
		configErrs: map[string]error{"alpha": errBoom},
	}
	h3 := NewGetConnectorHandler(r3)
	res3, err := h3.Handle(context.Background(), GetConnectorQuery{Name: "alpha"})
	if err != nil || res3.Status.Type != "sink" {
		t.Fatalf("config-err swallow: %+v err=%v", res3, err)
	}
}

func TestListConnectorPluginsHandler(t *testing.T) {
	r := &stubConnectorReader{plugins: []map[string]any{{"class": "x"}, {"class": "y"}}}
	h := NewListConnectorPluginsHandler(r)
	res, err := h.Handle(context.Background(), ListConnectorPluginsQuery{})
	if err != nil || res.Count != 2 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubConnectorReader{pluginsErr: errBoom}
	h2 := NewListConnectorPluginsHandler(r2)
	if _, err := h2.Handle(context.Background(), ListConnectorPluginsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// ActivityLogReader stub.
// ---------------------------------------------------------------------

type stubActivityLogReader struct {
	rows []ActivityLogRow
	n    int64
	err  error

	gotPage, gotSize int
	gotFilter       ActivityLogFilter

	stats     []OpStat
	recentErr []ActivityLogRow
	statsErr  error
}

func (s *stubActivityLogReader) ListActivity(_ context.Context, f ActivityLogFilter, page, size int) ([]ActivityLogRow, int64, error) {
	s.gotFilter, s.gotPage, s.gotSize = f, page, size
	return s.rows, s.n, s.err
}
func (s *stubActivityLogReader) Stats24h(context.Context) ([]OpStat, []ActivityLogRow, error) {
	return s.stats, s.recentErr, s.statsErr
}

func TestListActivityLogsHandler_HappyAndClamp(t *testing.T) {
	r := &stubActivityLogReader{rows: []ActivityLogRow{{ID: 1}}, n: 1}
	h := NewListActivityLogsHandler(r)

	res, err := h.Handle(context.Background(), ListActivityLogsQuery{
		Filter:   ActivityLogFilter{Operation: "shadow-sync"},
		Page:     2,
		PageSize: 100,
	})
	if err != nil || res.Page != 2 || res.PageSize != 100 || r.gotFilter.Operation != "shadow-sync" {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	// page<1 → 1; size<1 → default 50
	_, _ = h.Handle(context.Background(), ListActivityLogsQuery{Page: -1, PageSize: 0})
	if r.gotPage != 1 || r.gotSize != 50 {
		t.Fatalf("low clamp: page=%d size=%d", r.gotPage, r.gotSize)
	}

	// size>200 → default 50
	_, _ = h.Handle(context.Background(), ListActivityLogsQuery{Page: 1, PageSize: 999})
	if r.gotSize != 50 {
		t.Fatalf("high clamp: size=%d", r.gotSize)
	}
}

func TestListActivityLogsHandler_Error(t *testing.T) {
	r := &stubActivityLogReader{err: errBoom}
	h := NewListActivityLogsHandler(r)
	if _, err := h.Handle(context.Background(), ListActivityLogsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

func TestGetActivityStatsHandler(t *testing.T) {
	r := &stubActivityLogReader{
		stats:     []OpStat{{Operation: "shadow-sync", Total: 10}},
		recentErr: []ActivityLogRow{{ID: 99}},
	}
	h := NewGetActivityStatsHandler(r)

	res, err := h.Handle(context.Background(), GetActivityStatsQuery{})
	if err != nil || len(res.Stats24h) != 1 || len(res.RecentErrors) != 1 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubActivityLogReader{statsErr: errBoom}
	h2 := NewGetActivityStatsHandler(r2)
	if _, err := h2.Handle(context.Background(), GetActivityStatsQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// TransmuteScheduleReader stub.
// ---------------------------------------------------------------------

type stubTransmuteScheduleReader struct {
	rows []TransmuteScheduleRow
	err  error
}

func (s *stubTransmuteScheduleReader) ListSchedules(context.Context) ([]TransmuteScheduleRow, error) {
	return s.rows, s.err
}

func TestListTransmuteSchedulesHandler(t *testing.T) {
	r := &stubTransmuteScheduleReader{rows: []TransmuteScheduleRow{{ID: 1}, {ID: 2}}}
	h := NewListTransmuteSchedulesHandler(r)

	res, err := h.Handle(context.Background(), ListTransmuteSchedulesQuery{})
	if err != nil || res.Count != 2 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubTransmuteScheduleReader{err: errBoom}
	h2 := NewListTransmuteSchedulesHandler(r2)
	if _, err := h2.Handle(context.Background(), ListTransmuteSchedulesQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// SourceReader stub.
// ---------------------------------------------------------------------

type stubSourceReader struct {
	list    []model.Source
	listErr error

	one    *model.Source
	oneErr error
	gotID  int64
}

func (s *stubSourceReader) List(context.Context) ([]model.Source, error) { return s.list, s.listErr }
func (s *stubSourceReader) GetByID(_ context.Context, id int64) (*model.Source, error) {
	s.gotID = id
	return s.one, s.oneErr
}

func TestListSourcesHandler(t *testing.T) {
	r := &stubSourceReader{list: []model.Source{{ID: 1}, {ID: 2}, {ID: 3}}}
	h := NewListSourcesHandler(r)

	res, err := h.Handle(context.Background(), ListSourcesQuery{})
	if err != nil || res.Count != 3 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubSourceReader{listErr: errBoom}
	h2 := NewListSourcesHandler(r2)
	if _, err := h2.Handle(context.Background(), ListSourcesQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

func TestGetSourceHandler(t *testing.T) {
	r := &stubSourceReader{one: &model.Source{ID: 42}}
	h := NewGetSourceHandler(r)

	res, err := h.Handle(context.Background(), GetSourceQuery{ID: 42})
	if err != nil || res.Source == nil || res.Source.ID != 42 || r.gotID != 42 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubSourceReader{oneErr: errBoom}
	h2 := NewGetSourceHandler(r2)
	if _, err := h2.Handle(context.Background(), GetSourceQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// WizardReader stub.
// ---------------------------------------------------------------------

type stubWizardReader struct {
	sess  *model.WizardSession
	err   error
	gotID string
}

func (s *stubWizardReader) Get(_ context.Context, id string) (*model.WizardSession, error) {
	s.gotID = id
	return s.sess, s.err
}

func TestGetWizardSessionHandler(t *testing.T) {
	r := &stubWizardReader{sess: &model.WizardSession{ID: "sess-1"}}
	h := NewGetWizardSessionHandler(r)

	res, err := h.Handle(context.Background(), GetWizardSessionQuery{ID: "sess-1"})
	if err != nil || res.Session == nil || res.Session.ID != "sess-1" || r.gotID != "sess-1" {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubWizardReader{err: errBoom}
	h2 := NewGetWizardSessionHandler(r2)
	if _, err := h2.Handle(context.Background(), GetWizardSessionQuery{ID: "x"}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

func TestGetWizardProgressHandler(t *testing.T) {
	now := time.Now().UTC()
	r := &stubWizardReader{sess: &model.WizardSession{
		ID:          "sess-2",
		CurrentStep: 4,
		Status:      "running",
		ProgressLog: []byte(`[{"step":1,"event":"started"}]`),
		UpdatedAt:   now,
	}}
	h := NewGetWizardProgressHandler(r)

	res, err := h.Handle(context.Background(), GetWizardProgressQuery{ID: "sess-2"})
	if err != nil {
		t.Fatalf("happy err: %v", err)
	}
	if res.Progress.SessionID != "sess-2" || res.Progress.CurrentStep != 4 || res.Progress.Status != "running" {
		t.Fatalf("projection: %+v", res.Progress)
	}
	if !res.Progress.UpdatedAt.Equal(now) {
		t.Fatalf("updated_at mismatch: got %v want %v", res.Progress.UpdatedAt, now)
	}
	// progress_log is RawMessage — round-trip via json should yield the
	// same array (FE wire contract).
	var arr []map[string]any
	if err := json.Unmarshal(res.Progress.ProgressLog, &arr); err != nil || len(arr) != 1 {
		t.Fatalf("progress_log unmarshal: arr=%v err=%v", arr, err)
	}

	r2 := &stubWizardReader{err: errBoom}
	h2 := NewGetWizardProgressHandler(r2)
	if _, err := h2.Handle(context.Background(), GetWizardProgressQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// WorkerScheduleReader stub.
// ---------------------------------------------------------------------

type stubWorkerScheduleReader struct {
	list    []WorkerScheduleResponse
	listErr error

	one    *WorkerScheduleResponse
	oneErr error
	gotID  uint
}

func (s *stubWorkerScheduleReader) ListResponses(context.Context) ([]WorkerScheduleResponse, error) {
	return s.list, s.listErr
}
func (s *stubWorkerScheduleReader) GetResponseByID(_ context.Context, id uint) (*WorkerScheduleResponse, error) {
	s.gotID = id
	return s.one, s.oneErr
}

func TestListWorkerSchedulesHandler(t *testing.T) {
	r := &stubWorkerScheduleReader{list: []WorkerScheduleResponse{{ID: 1}, {ID: 2}}}
	h := NewListWorkerSchedulesHandler(r)

	res, err := h.Handle(context.Background(), ListWorkerSchedulesQuery{})
	if err != nil || len(res.Data) != 2 {
		t.Fatalf("happy: %+v err=%v", res, err)
	}

	r2 := &stubWorkerScheduleReader{listErr: errBoom}
	h2 := NewListWorkerSchedulesHandler(r2)
	if _, err := h2.Handle(context.Background(), ListWorkerSchedulesQuery{}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}

// ---------------------------------------------------------------------
// JobReader — Phase 2 v2 / P3.T3.10. GET /api/jobs/:id projects the
// CommandBus tracker row.
// ---------------------------------------------------------------------

type stubJobReader struct {
	job *job.Job
	err error
}

func (s *stubJobReader) GetByID(context.Context, string) (*job.Job, error) {
	return s.job, s.err
}

func TestGetJobHandler(t *testing.T) {
	now := time.Now().UTC()
	finished := now.Add(2 * time.Second)
	r := &stubJobReader{job: &job.Job{
		ID:             "11111111-1111-1111-1111-111111111111",
		Type:           "master.swap",
		Status:         job.StatusSuccess,
		Payload:        json.RawMessage(`{"master":"orders"}`),
		Result:         json.RawMessage(`{"swapped":true}`),
		CreatedBy:      "alice",
		CorrelationID:  "trace-1",
		IdempotencyKey: "idem-1",
		CreatedAt:      now,
		FinishedAt:     &finished,
	}}
	h := NewGetJobHandler(r)
	res, err := h.Handle(context.Background(), GetJobQuery{ID: "x"})
	if err != nil {
		t.Fatalf("happy err=%v", err)
	}
	if res.Job.Status != "success" || res.Job.Type != "master.swap" || res.Job.CreatedBy != "alice" {
		t.Fatalf("projection: %+v", res.Job)
	}
	if string(res.Job.Result) != `{"swapped":true}` {
		t.Fatalf("result: %s", res.Job.Result)
	}

	r2 := &stubJobReader{err: errBoom}
	h2 := NewGetJobHandler(r2)
	if _, err := h2.Handle(context.Background(), GetJobQuery{ID: "x"}); !errors.Is(err, errBoom) {
		t.Fatalf("err: %v", err)
	}
}
