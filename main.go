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
	"github.com/krateoplatformops/events-presenter/internal/probes"
	"github.com/krateoplatformops/events-presenter/internal/queue"
	pgutil "github.com/krateoplatformops/events-presenter/internal/util/pg"
)

func main() {
	cfg := config.Setup()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgCtx, cancel := context.WithTimeout(rootCtx, cfg.DbReadyTimeout)
	defer cancel()

	pool, err := pgutil.WaitForPostgres(pgCtx, cfg.Log, cfg.DbURL)
	if err != nil {
		cfg.Log.Error("cannot connect to PostgreSQL", slog.Any("err", err))
		os.Exit(1)
	}
	defer pool.Close()
	cfg.Log.Info("PostgreSQL is ready.")

	health := probes.New(pool)

	// Queue
	q := queue.NewQueue(1000, 8)
	q.Run()
	defer q.Terminate()

	// SSE hub
	hub := handlers.NewEventHub()

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
		handlers.RunPgListener(listenerCtx, lo, q, pool, hub)
	}()

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/notifications", handlers.EventsSSEHandler(hub))
	mux.HandleFunc("/events", handlers.ResourcesHandler(pool))
	mux.HandleFunc("/livez", health.LivenessHandler())
	mux.HandleFunc("/readyz", health.ReadinessHandler())

	server := &http.Server{
		Addr:         ":" + strconv.Itoa(cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("Starting HTTP server on port %d", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	health.SetReady(true)
	log.Println("Application is ready")

	// --- WAIT FOR SHUTDOWN SIGNAL OR SERVER ERROR ---
	select {
	case <-rootCtx.Done():
		log.Println("Shutdown signal received")
	case err := <-serverErr:
		log.Printf("Server error: %v", err)
	}

	// --- GRACEFUL SHUTDOWN ---
	health.SetShutdownStarted()
	health.SetReady(false)
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
