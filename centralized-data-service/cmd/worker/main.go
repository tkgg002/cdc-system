package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"centralized-data-service/config"
	"centralized-data-service/internal/server"
	"centralized-data-service/pkgs/idgen"
	"centralized-data-service/pkgs/metrics"
	"centralized-data-service/pkgs/observability"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger, _ := zap.NewProduction()
	if cfg.Server.Mode == "debug" {
		logger, _ = zap.NewDevelopment()
	}
	defer logger.Sync()

	logger.Info("starting CDC Worker",
		zap.String("service", cfg.Server.Name),
		zap.String("port", cfg.Server.Port),
	)

	if err := idgen.Init(logger); err != nil {
		logger.Fatal("failed to initialize sonyflake ID generator", zap.Error(err))
	}

	otelCfg := observability.OtelConfig{
		Enabled:     cfg.Otel.Enabled,
		ServiceName: cfg.Otel.ServiceName,
		Endpoint:    cfg.Otel.Endpoint,
		SampleRatio: cfg.Otel.SampleRatio,
		Logs: observability.LogsConfig{
			SampleBySeverity: observability.LogSampleConfig{
				Debug: cfg.Otel.Logs.SampleBySeverity.Debug,
				Info:  cfg.Otel.Logs.SampleBySeverity.Info,
				Warn:  cfg.Otel.Logs.SampleBySeverity.Warn,
				Error: cfg.Otel.Logs.SampleBySeverity.Error,
				Fatal: cfg.Otel.Logs.SampleBySeverity.Fatal,
			},
			MemoryLimitMiB: cfg.Otel.Logs.MemoryLimitMiB,
			Fallback: observability.LogFallbackConfig{
				DegradedAfterErrors: cfg.Otel.Logs.Fallback.DegradedAfterErrors,
				RecoverAfter:        cfg.Otel.Logs.Fallback.RecoverAfter,
			},
		},
	}
	otelShutdown, err := observability.InitOtel(otelCfg, logger)
	if err != nil {
		logger.Warn("OpenTelemetry init failed, tracing disabled", zap.Error(err))
	}
	defer otelShutdown()

	// Wire OTel zap bridge: logs go to both console AND SigNoz. The
	// OTel branch is wrapped in a severity-aware sampler so that
	// low-value Debug/Info floods never saturate the backend, and
	// can be muted at runtime by the fallback tracker when SigNoz
	// is unreachable.
	if lp := observability.LogProvider(); lp != nil {
		otelCore := otelzap.NewCore("cdc-worker", otelzap.WithLoggerProvider(lp))
		wrapped := observability.WrapCoreWithSeverityAwareness(otelCore, otelCfg)
		logger = zap.New(zapcore.NewTee(logger.Core(), wrapped))
		logger.Info("OTel zap bridge active — logs forwarding to SigNoz (severity-aware)")
	}

	srv, err := server.NewWorkerServer(cfg, logger)
	if err != nil {
		logger.Fatal("failed to initialize worker server", zap.Error(err))
	}

	// Standalone Prometheus /metrics endpoint on 9090 (separate from
	// the fiber app so that the metrics surface is isolated from the
	// public API port and stays reachable even if fiber is busy).
	metricsCtx, metricsCancel := context.WithCancel(context.Background())
	defer metricsCancel()
	go metrics.StartMetricsServer(metricsCtx, 9090, logger)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		metricsCancel()
		srv.Shutdown()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		logger.Fatal("worker server failed", zap.Error(err))
	}
}
