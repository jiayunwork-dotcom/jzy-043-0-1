package engine

import (
	"context"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/metrics"
	"taskforge/internal/store"
)

// RetryDead re-enqueues dead tasks to the head of their ready queues.
func (e *Engine) RetryDead(ctx context.Context, ids []string) (int64, error) {
	now := e.clk.Now()
	n, err := e.repo.RequeueDead(ctx, ids, now)
	if err != nil {
		return 0, err
	}
	// Re-enqueue only the ids that actually transitioned; re-read priorities.
	tasks, _ := e.repo.GetTasks(ctx, ids)
	for _, t := range tasks {
		if t.State == domain.StateReady {
			_ = e.q.EnqueueReady(ctx, t.ID, int(t.Priority), true)
		}
	}
	return n, nil
}

// DiscardDead marks dead tasks canceled (they disappear from the dead zone).
func (e *Engine) DiscardDead(ctx context.Context, ids []string) (int64, error) {
	return e.repo.DiscardDead(ctx, ids, e.clk.Now())
}

// DrainWorker starts graceful shutdown of one worker: no new tasks, existing
// executions run to completion.
func (e *Engine) DrainWorker(ctx context.Context, id string) error {
	return e.markWorkerDraining(ctx, id)
}

// SnapshotMetrics assembles the overview dashboard payload.
func (e *Engine) SnapshotMetrics(ctx context.Context) (metrics.Snapshot, error) {
	now := e.clk.Now()
	depth, err := e.q.ReadyDepth(ctx)
	if err != nil {
		return metrics.Snapshot{}, err
	}
	delayDepth, _ := e.q.DelaySize(ctx)
	dead, _ := e.repo.CountDead(ctx)
	active, total := e.pool.ActiveSlots()
	on, draining, off, _ := e.repo.WorkerStats(ctx)
	return e.mc.Snapshot(now, metrics.LiveInputs{
		Depth:        depth,
		DelayDepth:   delayDepth,
		DeadBacklog:  dead,
		TrendSeconds: 300,
		Workers: metrics.WorkerSummary{
			Online:      int(on),
			Draining:    int(draining),
			Offline:     int(off),
			ActiveSlots: active,
			TotalSlots:  total,
		},
	}), nil
}

// SimulateWorkerLoss moves a worker's heartbeat into the past so the next
// reaper tick considers it lost. Test/debug only.
func (e *Engine) SimulateWorkerLoss(ctx context.Context, workerID string) error {
	stale := e.clk.Now().Add(-e.opts.WorkerTimeout - time.Second)
	return e.repo.HeartbeatWorker(ctx, workerID, 1, 0, stale)
}

// WorkerSnapshot is one row on the workers page.
type WorkerSnapshot struct {
	Worker  *domain.Worker `json:"worker"`
	Running []*domain.Task `json:"running"`
}

// WorkerDetails returns worker state plus its in-flight tasks.
func (e *Engine) WorkerDetails(ctx context.Context) ([]WorkerSnapshot, error) {
	ws, err := e.repo.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]WorkerSnapshot, 0, len(ws))
	for _, w := range ws {
		list, err := e.repo.ListTasks(ctx, store.TaskFilter{
			States:   []domain.TaskState{domain.StateRunning},
			WorkerID: w.ID,
			Limit:    100,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, WorkerSnapshot{Worker: w, Running: list.Tasks})
	}
	return out, nil
}
