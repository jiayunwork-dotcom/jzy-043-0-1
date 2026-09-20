package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"

	"taskforge/internal/clock"
	"taskforge/internal/demo"
	"taskforge/internal/engine"
	"taskforge/internal/processor"
	"taskforge/internal/queue"
	"taskforge/internal/store"
)

func newTestApp(t *testing.T) (*fiber.App, *engine.Engine) {
	t.Helper()
	clk := clock.NewReal()
	repo := store.NewMem()
	q := queue.NewMemory()
	reg := processor.NewRegistry()
	demo.Register(reg)
	eng := engine.New(clk, repo, q, reg, engine.Options{
		WorkerCount: 2, SlotsPerWorker: 4, FairnessThreshold: 5,
		DispatchInterval: 20 * time.Millisecond, DelayScanInterval: 50 * time.Millisecond,
		HeartbeatInterval: time.Hour, WorkerTimeout: time.Hour, LeaseTTL: time.Hour,
	})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		eng.Shutdown(ctx)
	})
	srv := New(eng, repo)
	return srv.Handler(), eng
}

func doJSON(t *testing.T, app *fiber.App, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 20000)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestTaskSubmitSucceedsAndAppears(t *testing.T) {
	app, eng := newTestApp(t)
	_ = eng

	// submit a trivial compute task
	status, body := doJSON(t, app, "POST", "/api/tasks", map[string]any{
		"type": "compute", "priority": "high",
		"payload": map[string]any{"a": 3, "b": 4, "op": "mul"},
	})
	if status != 201 {
		t.Fatalf("submit status=%d body=%v", status, body)
	}
	taskMap, _ := body["task"].(map[string]any)
	id, _ := taskMap["id"].(string)
	if id == "" {
		t.Fatal("missing task id")
	}

	// wait until succeeded
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, detail := doJSON(t, app, "GET", "/api/tasks/"+id, nil)
		tm, _ := detail["task"].(map[string]any)
		if tm["state"] == "succeeded" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("task did not succeed in time")
}

func TestDeadLetterBatchRetryOverHTTP(t *testing.T) {
	app, _ := newTestApp(t)
	var id string
	{
		status, body := doJSON(t, app, "POST", "/api/tasks", map[string]any{
			"type": "always-fail", "priority": "bulk",
			"retry_policy": map[string]any{"kind": "fixed", "base_interval_seconds": 0.01, "max_retries": 1},
		})
		if status != 201 {
			t.Fatalf("submit status=%d body=%v", status, body)
		}
		tm, _ := body["task"].(map[string]any)
		id, _ = tm["id"].(string)
	}

	// wait for it to land in dead
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		_, dl := doJSON(t, app, "GET", "/api/dead", nil)
		if n, _ := dl["total"].(float64); n >= 1 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	_, stats := doJSON(t, app, "GET", "/api/dead/stats", nil)
	if arr, _ := stats["stats"].([]any); len(arr) == 0 {
		t.Fatal("expected dead error stats")
	}

	// batch retry
	status, body := doJSON(t, app, "POST", "/api/dead/retry", map[string]any{"ids": []string{id}})
	if status != 200 || body["requeued"].(float64) != 1 {
		t.Fatalf("retry status=%d body=%v", status, body)
	}
}

func TestDAGSubmitAndCycleRejection(t *testing.T) {
	app, _ := newTestApp(t)

	// cyclic DAG -> 422
	status, body := doJSON(t, app, "POST", "/api/dags", map[string]any{
		"name": "cyc",
		"nodes": []map[string]any{
			{"id": "A", "task_type": "echo", "depends_on": []string{"B"}},
			{"id": "B", "task_type": "echo", "depends_on": []string{"A"}},
		},
	})
	if status != 422 {
		t.Fatalf("cycle status=%d body=%v", status, body)
	}

	// valid DAG A -> B
	status, body = doJSON(t, app, "POST", "/api/dags", map[string]any{
		"name": "ab", "failure_policy": "abort",
		"nodes": []map[string]any{
			{"id": "A", "task_type": "echo", "priority": "normal"},
			{"id": "B", "task_type": "echo", "priority": "normal", "depends_on": []string{"A"}},
		},
	})
	if status != 201 {
		t.Fatalf("dag submit status=%d body=%v", status, body)
	}
	dm, _ := body["dag"].(map[string]any)
	dagID, _ := dm["id"].(string)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, detail := doJSON(t, app, "GET", "/api/dags/"+dagID, nil)
		d, _ := detail["dag"].(map[string]any)
		if d["status"] == "succeeded" {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("DAG did not succeed in time")
}

func TestMetricsAndWorkersEndpoints(t *testing.T) {
	app, _ := newTestApp(t)

	status, body := doJSON(t, app, "GET", "/api/metrics", nil)
	if status != 200 {
		t.Fatalf("metrics status=%d", status)
	}
	if _, ok := body["queue_depth"]; !ok {
		t.Fatal("metrics missing queue_depth")
	}

	status, body = doJSON(t, app, "GET", "/api/workers", nil)
	if status != 200 {
		t.Fatalf("workers status=%d", status)
	}
	ws, _ := body["workers"].([]any)
	if len(ws) != 2 {
		t.Fatalf("workers=%d want 2", len(ws))
	}
}
