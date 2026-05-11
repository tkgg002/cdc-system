package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_events_processed_total",
			Help: "Total CDC events processed",
		},
		[]string{"operation", "source_db", "table", "status"},
	)

	ProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cdc_processing_duration_seconds",
			Help:    "Processing duration per event",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
		},
		[]string{"operation", "source_db", "table"},
	)

	SchemaDriftDetected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "schema_drift_detected_total",
			Help: "Schema drifts detected",
		},
		[]string{"source_db", "table"},
	)

	MappingRulesLoaded = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mapping_rules_loaded",
			Help: "Current loaded mapping rules count",
		},
	)

	PendingFieldsCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pending_fields_count",
			Help: "Pending fields by status",
		},
		[]string{"status"},
	)

	RegisteredTables = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "registered_tables_total",
			Help: "Registered CDC tables",
		},
		[]string{"source_db", "sync_engine"},
	)

	SyncSuccess = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_sync_success_total",
			Help: "Total successful CDC syncs",
		},
		[]string{"table", "operation", "source"},
	)

	SyncFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_sync_failed_total",
			Help: "Total failed CDC syncs",
		},
		[]string{"table", "operation", "source"},
	)

	ConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cdc_kafka_consumer_lag",
			Help: "Kafka consumer group lag per topic partition",
		},
		[]string{"topic", "partition"},
	)

	ReconDrift = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cdc_recon_drift_count",
			Help: "Data drift count per table",
		},
		[]string{"table", "tier"},
	)

	// ReconRunDuration records wall-time for a single recon run,
	// bucketed by `table_group` (prefix before first underscore of
	// the target table name; `other` when no prefix exists) and
	// `tier` (1/2/3). Kept low-cardinality to protect Prometheus TSDB.
	ReconRunDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cdc_recon_run_duration_seconds",
			Help:    "Reconciliation run duration per table group and tier",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800},
		},
		[]string{"table_group", "tier"},
	)

	// ReconMismatchCount carries per-table mismatches for the latest
	// run. High-cardinality intentionally (one series per table) so
	// the SRE can alert on "table X has N drifted records for >1h".
	// Operators with >500 tables should whitelist or drop this metric
	// at scrape time via relabel_configs.
	ReconMismatchCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cdc_recon_mismatch_count",
			Help: "Mismatches found by the most recent recon run per table + tier",
		},
		[]string{"table", "tier"},
	)

	// ReconHealActions counts heal outcomes. `action` ∈ upsert|skip|error.
	ReconHealActions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_recon_heal_actions_total",
			Help: "Total recon heal actions by outcome",
		},
		[]string{"table", "action"},
	)

	// ReconLastSuccessTs is the Unix timestamp of the most recent
	// successful recon run. Fuel for a "ReconStale" alert
	// (time() - value > threshold).
	ReconLastSuccessTs = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cdc_recon_last_success_timestamp",
			Help: "Unix epoch seconds of the most recent successful recon run per table + tier",
		},
		[]string{"table", "tier"},
	)

	E2ELatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cdc_e2e_latency_seconds",
		Help:    "End-to-end latency: Kafka message timestamp → Postgres insert",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	})

	// ReconBackfillProgress — gauge 0..100 (%) for the _source_ts backfill
	// job. Emitted by BackfillSourceTsService per table. Cardinality is
	// bounded by the active table set (tens at most) so a per-table label
	// is safe here.
	ReconBackfillProgress = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cdc_recon_backfill_progress",
			Help: "Progress percent (0..100) of the _source_ts backfill job per table",
		},
		[]string{"table"},
	)
)
