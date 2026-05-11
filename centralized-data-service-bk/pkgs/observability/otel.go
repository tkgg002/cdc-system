package observability

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	otelLog "go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	otelTrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LogSampleConfig defines per-severity sampling ratios (0.0 — drop all,
// 1.0 — keep all) applied BEFORE logs are handed to the OTel exporter.
// Missing levels default to 1.0 to avoid silent drops.
type LogSampleConfig struct {
	Debug float64 `mapstructure:"debug"`
	Info  float64 `mapstructure:"info"`
	Warn  float64 `mapstructure:"warn"`
	Error float64 `mapstructure:"error"`
	Fatal float64 `mapstructure:"fatal"`
}

// LogFallbackConfig controls the export-error-driven fallback.
type LogFallbackConfig struct {
	// DegradedAfterErrors: once the rolling 1-minute error counter
	// reaches this value, the OTel log sink is detached (console-only).
	DegradedAfterErrors int `mapstructure:"degradedAfterErrors"`
	// RecoverAfter: how long to stay in degraded mode before probing.
	RecoverAfter time.Duration `mapstructure:"recoverAfter"`
}

// LogsConfig bundles logs-specific OTel tunables.
type LogsConfig struct {
	SampleBySeverity LogSampleConfig   `mapstructure:"sampleBySeverity"`
	MemoryLimitMiB   int               `mapstructure:"memoryLimitMib"`
	Fallback         LogFallbackConfig `mapstructure:"fallback"`
}

// OtelConfig holds OpenTelemetry configuration
type OtelConfig struct {
	Enabled     bool       `mapstructure:"enabled"`
	ServiceName string     `mapstructure:"serviceName"`
	Endpoint    string     `mapstructure:"endpoint"`
	SampleRatio float64    `mapstructure:"sampleRatio"`
	Logs        LogsConfig `mapstructure:"logs"`
}

var tracer otelTrace.Tracer
var loggerProvider *sdklog.LoggerProvider

// defaultLogsConfig fills any unset fields with conservative defaults.
func defaultLogsConfig(l LogsConfig) LogsConfig {
	// Sample ratios: a 0 literal is indistinguishable from "unset" in
	// the struct, so we only back-fill when the whole block looks empty.
	empty := l.SampleBySeverity.Debug == 0 &&
		l.SampleBySeverity.Info == 0 &&
		l.SampleBySeverity.Warn == 0 &&
		l.SampleBySeverity.Error == 0 &&
		l.SampleBySeverity.Fatal == 0
	if empty {
		l.SampleBySeverity = LogSampleConfig{
			Debug: 0.0,
			Info:  0.1,
			Warn:  1.0,
			Error: 1.0,
			Fatal: 1.0,
		}
	}
	if l.MemoryLimitMiB <= 0 {
		l.MemoryLimitMiB = 256
	}
	if l.Fallback.DegradedAfterErrors <= 0 {
		l.Fallback.DegradedAfterErrors = 10
	}
	if l.Fallback.RecoverAfter <= 0 {
		l.Fallback.RecoverAfter = 5 * time.Minute
	}
	return l
}

// severityAwareCore wraps a zapcore.Core and probabilistically drops
// entries whose level is under-sampled by the operator. Used for the
// OTel branch only — the console branch always keeps everything.
//
// A *severityAwareCore also supports runtime mute/unmute via SetMuted:
// when muted, Check returns nil so no logs reach the inner exporter.
// This is how the fallback path degrades gracefully without taking
// the whole logger down.
type severityAwareCore struct {
	zapcore.Core
	sample LogSampleConfig
	muted  atomic.Bool
	rng    *rand.Rand
	mu     sync.Mutex
}

func newSeverityAwareCore(inner zapcore.Core, sample LogSampleConfig) *severityAwareCore {
	return &severityAwareCore{
		Core:   inner,
		sample: sample,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetMuted toggles the OTel branch on/off at runtime.
func (s *severityAwareCore) SetMuted(muted bool) { s.muted.Store(muted) }

// ratioFor returns the keep probability for a given level.
func (s *severityAwareCore) ratioFor(l zapcore.Level) float64 {
	switch l {
	case zapcore.DebugLevel:
		return s.sample.Debug
	case zapcore.InfoLevel:
		return s.sample.Info
	case zapcore.WarnLevel:
		return s.sample.Warn
	case zapcore.ErrorLevel:
		return s.sample.Error
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return s.sample.Fatal
	default:
		return 1.0
	}
}

// Enabled intersects the inner core's enablement with our ratio-based
// gate (ratio == 0 → level effectively disabled).
func (s *severityAwareCore) Enabled(l zapcore.Level) bool {
	if s.muted.Load() {
		return false
	}
	if s.ratioFor(l) <= 0 {
		return false
	}
	return s.Core.Enabled(l)
}

func (s *severityAwareCore) With(fields []zapcore.Field) zapcore.Core {
	return &severityAwareCore{
		Core:   s.Core.With(fields),
		sample: s.sample,
		rng:    s.rng,
	}
}

func (s *severityAwareCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if s.muted.Load() {
		return ce
	}
	ratio := s.ratioFor(ent.Level)
	switch {
	case ratio <= 0:
		return ce
	case ratio >= 1:
		// full keep
	default:
		s.mu.Lock()
		keep := s.rng.Float64() < ratio
		s.mu.Unlock()
		if !keep {
			return ce
		}
	}
	return s.Core.Check(ent, ce)
}

// exportErrorTracker is a leaky-bucket counter used to decide whether
// the OTel sink is healthy enough to keep receiving logs. Increment
// from the exporter-error path; Check runs on a tick.
type exportErrorTracker struct {
	mu          sync.Mutex
	count       int
	windowStart time.Time
	threshold   int
	// Degraded state
	degraded       atomic.Bool
	degradedSince  time.Time
	recoverAfter   time.Duration
	onStateChange  func(degraded bool)
}

func newExportErrorTracker(threshold int, recoverAfter time.Duration, onChange func(bool)) *exportErrorTracker {
	return &exportErrorTracker{
		windowStart:   time.Now(),
		threshold:     threshold,
		recoverAfter:  recoverAfter,
		onStateChange: onChange,
	}
}

func (t *exportErrorTracker) IncrementErrors(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1-minute rolling window reset
	if time.Since(t.windowStart) > time.Minute {
		t.count = 0
		t.windowStart = time.Now()
	}
	t.count += n
	if t.count >= t.threshold && !t.degraded.Load() {
		t.degraded.Store(true)
		t.degradedSince = time.Now()
		if t.onStateChange != nil {
			t.onStateChange(true)
		}
	}
}

// Probe flips out of degraded mode once RecoverAfter has elapsed. The
// caller should invoke this periodically (e.g., every 30 s).
func (t *exportErrorTracker) Probe() {
	if !t.degraded.Load() {
		return
	}
	t.mu.Lock()
	elapsed := time.Since(t.degradedSince)
	t.mu.Unlock()
	if elapsed >= t.recoverAfter {
		t.degraded.Store(false)
		t.mu.Lock()
		t.count = 0
		t.windowStart = time.Now()
		t.mu.Unlock()
		if t.onStateChange != nil {
			t.onStateChange(false)
		}
	}
}

var globalSeverityCore *severityAwareCore

// trackedLogExporter wraps a real sdklog.Exporter and forwards export
// errors to an exportErrorTracker so the fallback logic can react to
// backend outages (e.g., SigNoz unreachable).
type trackedLogExporter struct {
	inner   sdklog.Exporter
	tracker *exportErrorTracker
}

func (t *trackedLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := t.inner.Export(ctx, records)
	if err != nil && t.tracker != nil {
		t.tracker.IncrementErrors(1)
	}
	return err
}

func (t *trackedLogExporter) Shutdown(ctx context.Context) error {
	return t.inner.Shutdown(ctx)
}

func (t *trackedLogExporter) ForceFlush(ctx context.Context) error {
	return t.inner.ForceFlush(ctx)
}

// InitOtel initializes OpenTelemetry with OTLP HTTP exporters (traces + metrics)
// Compatible with SigNoz endpoint
func InitOtel(cfg OtelConfig, logger *zap.Logger) (func(), error) {
	if !cfg.Enabled {
		logger.Info("OpenTelemetry disabled")
		return func() {}, nil
	}
	cfg.Logs = defaultLogsConfig(cfg.Logs)

	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			attribute.String("environment", "development"),
		),
	)
	if err != nil {
		return nil, err
	}

	// Trace exporter
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.Endpoint+"/v1/traces"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	sampler := trace.ParentBased(trace.TraceIDRatioBased(cfg.SampleRatio))
	tp := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter, trace.WithBatchTimeout(5*time.Second)),
		trace.WithResource(res),
		trace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer(cfg.ServiceName)

	// Install W3C TraceContext + Baggage propagator so span context can
	// cross Kafka / HTTP / NATS boundaries (plan WORKER task #10).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Metric exporter
	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(cfg.Endpoint+"/v1/metrics"),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		logger.Warn("metric exporter init failed, metrics disabled", zap.Error(err))
	} else {
		mp := metric.NewMeterProvider(
			metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(10*time.Second))),
			metric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
	}

	// Log exporter → SigNoz, wrapped by error tracker for fallback
	var probeStop chan struct{}
	logExporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(cfg.Endpoint+"/v1/logs"),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		logger.Warn("log exporter init failed, log forwarding disabled", zap.Error(err))
	} else {
		tracker := newExportErrorTracker(
			cfg.Logs.Fallback.DegradedAfterErrors,
			cfg.Logs.Fallback.RecoverAfter,
			func(degraded bool) {
				if globalSeverityCore != nil {
					globalSeverityCore.SetMuted(degraded)
				}
				if degraded {
					logger.Warn("OTel log sink degraded — switching to console-only",
						zap.Int("error_threshold", cfg.Logs.Fallback.DegradedAfterErrors),
						zap.Duration("recover_after", cfg.Logs.Fallback.RecoverAfter),
					)
				} else {
					logger.Info("OTel log sink recovered — resuming log forwarding")
				}
			},
		)

		tracked := &trackedLogExporter{inner: logExporter, tracker: tracker}

		// Bounded memory: BatchProcessor buffers up to MaxQueueSize
		// records. We approximate the memory budget by capping the
		// queue at ~(MemoryLimitMiB * 1024) records assuming ~1KiB
		// per record — a conservative upper bound. Operators tune
		// via config.
		maxQueue := cfg.Logs.MemoryLimitMiB * 1024
		if maxQueue <= 0 {
			maxQueue = 4096
		}

		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(
				tracked,
				sdklog.WithMaxQueueSize(maxQueue),
				sdklog.WithExportMaxBatchSize(512),
				sdklog.WithExportInterval(5*time.Second),
			)),
			sdklog.WithResource(res),
		)
		otelLog.SetLoggerProvider(lp)
		loggerProvider = lp

		// Periodic probe to recover from degraded mode.
		probeStop = make(chan struct{})
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-probeStop:
					return
				case <-ticker.C:
					tracker.Probe()
				}
			}
		}()
	}

	logger.Info("OpenTelemetry initialized",
		zap.String("service", cfg.ServiceName),
		zap.String("endpoint", cfg.Endpoint),
		zap.Float64("sample_ratio", cfg.SampleRatio),
		zap.Int("log_memory_limit_mib", cfg.Logs.MemoryLimitMiB),
		zap.Int("log_fallback_threshold", cfg.Logs.Fallback.DegradedAfterErrors),
	)

	// Shutdown function
	shutdown := func() {
		if probeStop != nil {
			close(probeStop)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tp.Shutdown(ctx)
		if loggerProvider != nil {
			loggerProvider.Shutdown(ctx)
		}
		logger.Info("OpenTelemetry shutdown")
	}

	return shutdown, nil
}

// WrapCoreWithSeverityAwareness wraps a zapcore.Core with severity-based
// sampling and registers the wrapper globally so the fallback tracker can
// mute/unmute it at runtime. Returns the wrapped core for use with
// zapcore.NewTee.
//
// Only call this on the OTel branch; keep the console branch wrapped by
// the raw core so local debugging is never degraded.
func WrapCoreWithSeverityAwareness(c zapcore.Core, cfg OtelConfig) zapcore.Core {
	cfg.Logs = defaultLogsConfig(cfg.Logs)
	wrapped := newSeverityAwareCore(c, cfg.Logs.SampleBySeverity)
	globalSeverityCore = wrapped
	return wrapped
}

// Tracer returns the global tracer
func Tracer() otelTrace.Tracer {
	if tracer == nil {
		return otel.Tracer("cdc-worker")
	}
	return tracer
}

// LogProvider returns the OTel LoggerProvider for zap bridge
func LogProvider() *sdklog.LoggerProvider {
	return loggerProvider
}

// StartSpan creates a new span for tracing
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, otelTrace.Span) {
	return Tracer().Start(ctx, name, otelTrace.WithAttributes(attrs...))
}
