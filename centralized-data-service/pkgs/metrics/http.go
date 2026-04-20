package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// StartMetricsServer spins up a standalone HTTP server exposing
// Prometheus metrics on /metrics and a tiny liveness probe on /health.
//
// Runs until ctx is cancelled; on shutdown it performs a graceful
// close with a 5 s deadline. Errors are logged but never returned
// to the caller — failing to bind the port must not crash the
// Worker (metrics are a side-channel).
//
// Usage:
//
//	go metrics.StartMetricsServer(ctx, 9090, logger)
func StartMetricsServer(ctx context.Context, port int, logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","component":"metrics"}`))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// ListenAndServe in a goroutine; main goroutine waits on ctx.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("metrics HTTP server listening",
			zap.String("addr", srv.Addr),
			zap.Strings("paths", []string{"/metrics", "/health"}),
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("metrics server shutdown error", zap.Error(err))
			return
		}
		logger.Info("metrics HTTP server stopped")
	case err := <-errCh:
		if err != nil {
			logger.Error("metrics HTTP server failed", zap.Error(err))
		}
	}
}
