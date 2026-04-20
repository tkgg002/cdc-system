package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelLog "go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	otelTrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// OtelConfig holds OpenTelemetry configuration
type OtelConfig struct {
	Enabled     bool    `mapstructure:"enabled"`
	ServiceName string  `mapstructure:"serviceName"`
	Endpoint    string  `mapstructure:"endpoint"`
	SampleRatio float64 `mapstructure:"sampleRatio"`
}

var tracer otelTrace.Tracer
var loggerProvider *sdklog.LoggerProvider

// InitOtel initializes OpenTelemetry with OTLP HTTP exporters (traces + logs)
// Compatible with SigNoz endpoint
func InitOtel(cfg OtelConfig, logger *zap.Logger) (func(), error) {
	if !cfg.Enabled {
		logger.Info("OpenTelemetry disabled")
		return func() {}, nil
	}

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

	// Log exporter
	logExporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(cfg.Endpoint+"/v1/logs"),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		logger.Warn("log exporter init failed, log forwarding disabled", zap.Error(err))
	} else {
		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
			sdklog.WithResource(res),
		)
		otelLog.SetLoggerProvider(lp)
		loggerProvider = lp
	}

	logger.Info("OpenTelemetry initialized",
		zap.String("service", cfg.ServiceName),
		zap.String("endpoint", cfg.Endpoint),
		zap.Float64("sample_ratio", cfg.SampleRatio),
	)

	// Shutdown function
	shutdown := func() {
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

// Tracer returns the global tracer
func Tracer() otelTrace.Tracer {
	if tracer == nil {
		return otel.Tracer("cdc-cms")
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
