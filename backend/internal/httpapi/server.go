package httpapi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/valyala/fasthttp"

	"taskforge/internal/domain"
)

// Handler wires routes onto a Fiber app.
func (s *Server) Handler() *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:               "TaskForge",
		DisableStartupMessage: true,
		JSONEncoder:           json.Marshal,
	})
	app.Use(cors.New())

	app.Get("/api/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"ok": true, "time": time.Now()})
	})

	api := app.Group("/api")

	// metrics / overview
	api.Get("/metrics", s.metrics)
	api.Get("/events", s.eventsSSE)

	// tasks
	api.Post("/tasks", s.submitTask)
	api.Get("/tasks", s.listTasks)
	api.Get("/tasks/types", s.taskTypes)
	api.Get("/tasks/:id", s.getTask)
	api.Post("/tasks/:id/cancel", s.cancelTask)

	// dead letter
	api.Get("/dead", s.listDead)
	api.Get("/dead/stats", s.deadStats)
	api.Post("/dead/retry", s.retryDead)
	api.Post("/dead/discard", s.discardDead)

	// workers
	api.Get("/workers", s.listWorkers)
	api.Post("/workers/:id/drain", s.drainWorker)
	api.Post("/workers/:id/simulate-loss", s.simulateLoss)

	// DAG
	api.Get("/dags", s.listDAGs)
	api.Post("/dags", s.createDAG)
	api.Post("/dags/validate", s.validateDAG)
	api.Get("/dags/:id", s.getDAG)

	// audit
	api.Get("/audit", s.audit)

	return app
}

func (s *Server) metrics(c *fiber.Ctx) error {
	snap, err := s.eng.SnapshotMetrics(c.Context())
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(snap)
}

// eventsSSE pushes the metrics snapshot once per second for live dashboard
// updates without page reloads.
func (s *Server) eventsSSE(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		ctx := c.Context()
		// send one immediately, then once per second
		ping := func() bool {
			snap, err := s.eng.SnapshotMetrics(ctx)
			if err != nil {
				return true
			}
			b, _ := json.Marshal(snap)
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return false
			}
			return w.Flush() == nil
		}
		if !ping() {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !ping() {
					return
				}
			}
		}
	}))
	return nil
}

var _ = domain.PriorityNormal
