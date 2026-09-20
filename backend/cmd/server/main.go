// Command taskforged is the TaskForge standalone service.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskforge/internal/clock"
	"taskforge/internal/config"
	"taskforge/internal/demo"
	"taskforge/internal/engine"
	"taskforge/internal/httpapi"
	"taskforge/internal/processor"
	"taskforge/internal/queue"
	"taskforge/internal/store"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- persistence ----------------------------------------------------
	var repo store.Repository
	pg, err := store.NewPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	if err := waitForPostgres(ctx, pg); err != nil {
		log.Fatalf("postgres not ready: %v", err)
	}
	if err := pg.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	repo = pg
	log.Print("postgresql ready and migrated")

	// ---- redis queue ----------------------------------------------------
	rq := queue.NewRedis(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err := waitForRedis(ctx, rq); err != nil {
		log.Fatalf("redis not ready: %v", err)
	}
	var q queue.Queue = rq

	// ---- engine ---------------------------------------------------------
	reg := processor.NewRegistry()
	demo.Register(reg)

	eng := engine.New(clock.NewReal(), repo, q, reg, engine.Options{
		WorkerCount:       cfg.WorkerCount,
		SlotsPerWorker:    cfg.SlotsPerWorker,
		FairnessThreshold: cfg.FairnessThreshold,
		DispatchInterval:  cfg.DispatchInterval,
		DelayScanInterval: cfg.DelayScanInterval,
		HeartbeatInterval: cfg.HeartbeatInterval,
		WorkerTimeout:     cfg.WorkerTimeout,
		LeaseTTL:          cfg.LeaseTTL,
	})
	if err := eng.Start(ctx); err != nil {
		log.Fatalf("engine start: %v", err)
	}
	log.Printf("engine started: %d workers x %d slots, fairness threshold %d",
		cfg.WorkerCount, cfg.SlotsPerWorker, cfg.FairnessThreshold)

	// ---- http -----------------------------------------------------------
	srv := httpapi.New(eng, repo)
	app := srv.Handler()
	go func() {
		log.Printf("listening on %s", cfg.HTTPAddr)
		if err := app.Listen(cfg.HTTPAddr); err != nil {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("shutdown signal received, draining…")
	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = app.ShutdownWithContext(shutCtx)
	eng.Shutdown(shutCtx)
	pg.Close()
	log.Print("shutdown complete")
}

func waitForPostgres(ctx context.Context, pg *store.PgStore) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := pg.Ping(ctx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return os.ErrDeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func waitForRedis(ctx context.Context, q *queue.RedisQueue) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := q.Ping(ctx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return os.ErrDeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
