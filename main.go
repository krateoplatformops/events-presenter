package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/krateoplatformops/events-presenter/internal/config"
	"github.com/krateoplatformops/events-presenter/internal/handlers"
	"github.com/krateoplatformops/events-presenter/internal/queue"
	"github.com/krateoplatformops/events-presenter/internal/telemetry"
	"github.com/krateoplatformops/plumbing/pgutil"
	"github.com/krateoplatformops/plumbing/server/probes"
	"github.com/krateoplatformops/plumbing/server/use"
	"github.com/krateoplatformops/plumbing/server/use/cors"
)

func main() {
	cfg := config.Setup()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics, shutdownMetrics, err := telemetry.Setup(rootCtx, cfg.Log, telemetry.Config{
		Enabled:        cfg.OTelEnabled,
		ServiceName:    "events-presenter",
		ExportInterval: cfg.OTelInterval,
	})
	if err != nil {
		cfg.Log.Error("OpenTelemetry setup failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownMetrics(ctx); err != nil {
			cfg.Log.Warn("OpenTelemetry shutdown failed", slog.Any("err", err))
		}
	}()

	pgCtx, cancel := context.WithTimeout(rootCtx, cfg.DbReadyTimeout)
	defer cancel()

	dbConnectStarted := time.Now()
	pool, err := pgutil.WaitForPostgres(pgCtx, cfg.Log, cfg.DbURL)
	metrics.RecordDBConnectDuration(rootCtx, time.Since(dbConnectStarted))
	if err != nil {
		metrics.IncStartupFailure(rootCtx)
		cfg.Log.Error("cannot connect to PostgreSQL", slog.Any("err", err))
		os.Exit(1)
	}
	defer pool.Close()
	cfg.Log.Info("PostgreSQL is ready.")

	// Queue
	q := queue.NewQueue(1000, 8)
	q.Run()
	defer q.Terminate()

	// SSE hub
	hub := handlers.NewEventHub(metrics)

	// Listener Postgres
	lo := handlers.PgListenerConfig{
		DSN:           cfg.DbURL,
		Channel:       "events",
		ReconnectWait: 5 * time.Second,
		Log:           cfg.Log,
	}

	listenerCtx, cancelListener := context.WithCancel(rootCtx)
	listenerDone := make(chan struct{})

	go func() {
		defer close(listenerDone)
		handlers.RunPgListener(listenerCtx, lo, q, pool, hub, metrics)
	}()

	// HTTP server
	mux := http.NewServeMux()

	// Register /livez and /readyz without auth — Kubernetes probes must not require JWT.
	probes.Register(mux, cfg.Log, pool, time.Second)

	chain := use.NewChain(
		use.TraceId(),
		use.Access(cfg.Log),
		use.CORS(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowedMethods: []string{"GET", "POST", "OPTIONS"},
			AllowedHeaders: []string{
				"Accept",
				"Authorization",
				"Content-Type",
				"X-Auth-Code",
				"X-Krateo-TraceId",
			},
			ExposedHeaders:   []string{"Link"},
			AllowCredentials: false,
			MaxAge:           300, // Maximum value not ignored by any of major browsers
		}),
	)

	authChain := chain.Append(use.UserConfig(cfg.SigningKey, cfg.AuthnNS))

	mux.Handle("/notifications", authChain.Then(handlers.EventsSSEHandler(hub, cfg.Log)))
	mux.Handle("/events", authChain.Then(handlers.ResourcesHandler(pool, metrics)))

	server := &http.Server{
		Addr:         ":" + strconv.Itoa(cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // SSE connections can stay open indefinitely.
		IdleTimeout:  60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("Starting HTTP server on port %d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	log.Println("Application is ready")
	metrics.IncStartupSuccess(rootCtx)

	// --- WAIT FOR SHUTDOWN SIGNAL OR SERVER ERROR ---
	select {
	case <-rootCtx.Done():
		log.Println("Shutdown signal received")
	case err := <-serverErr:
		log.Printf("Server error: %v", err)
	}

	// --- GRACEFUL SHUTDOWN ---
	log.Println("Starting graceful shutdown...")

	var wg sync.WaitGroup

	// Shutdown HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelShutdown()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		} else {
			log.Println("HTTP server stopped gracefully")
		}
	}()

	// Shutdown listener Postgres
	wg.Add(1)
	go func() {
		defer wg.Done()
		cancelListener()
		select {
		case <-listenerDone:
			log.Println("Postgres listener stopped")
		case <-time.After(5 * time.Second):
			log.Println("Postgres listener shutdown timeout")
		}
	}()

	wg.Wait()
	log.Println("Graceful shutdown complete")
}
