package engine

import (
	"context"
	"log"
	"time"

	"taskforge/internal/domain"
)

// delayTick promotes due delayed tasks (pending -> ready). Runs at
// DelayScanInterval (sub-second default) so promotions are visible within
// about a second.
func (e *Engine) delayTick(ctx context.Context) {
	now := e.clk.Now()
	claims, err := e.q.PopDue(ctx, now)
	if err != nil {
		log.Printf("delay pop: %v", err)
		return
	}
	for _, c := range claims {
		if err := e.repo.MarkReady(ctx, c.TaskID, now); err != nil {
			// Already ready/canceled: make sure it is not lost in Redis.
			t, gerr := e.repo.GetTask(ctx, c.TaskID)
			if gerr == nil && t.State == domain.StateReady {
				_ = e.q.EnqueueReady(ctx, c.TaskID, c.Priority, false)
			}
			continue
		}
		if err := e.q.EnqueueReady(ctx, c.TaskID, c.Priority, false); err != nil {
			log.Printf("delay enqueue: %v", err)
		}
	}
}

// heartbeatTick refreshes DB heartbeat rows for every live worker.
func (e *Engine) heartbeatTick(ctx context.Context) {
	now := e.clk.Now()
	for _, w := range e.pool.List() {
		active := e.pool.ActiveCount(w)
		if err := e.repo.HeartbeatWorker(ctx, w.ID, w.TotalSlots, active, now); err != nil {
			log.Printf("heartbeat %s: %v", w.ID, err)
		}
	}
}

// reapWorkersTick marks workers dead after WorkerTimeout without heartbeat
// and requeues everything they were running. (External workers are detected
// through stale heartbeat rows; the SimulateLoss test helper directly
// advances a worker's heartbeat into the past.)
func (e *Engine) reapWorkersTick(ctx context.Context) {
	cutoff := e.clk.Now().Add(-e.opts.WorkerTimeout)
	ids, err := e.repo.MarkWorkersOfflineBefore(ctx, cutoff)
	if err != nil {
		log.Printf("mark offline: %v", err)
		return
	}
	for _, id := range ids {
		e.reapWorker(ctx, id)
	}
}

func (e *Engine) reapWorker(ctx context.Context, id string) {
	now := e.clk.Now()
	tasks, err := e.repo.ReapRunningOnWorker(ctx, id, now, true)
	if err != nil {
		log.Printf("reap worker %s: %v", id, err)
		return
	}
	e.pool.MarkOffline(id)
	// Requeue recovered tasks at the heads of their priority queues. This is
	// authoritative from PostgreSQL and does not rely on Redis inflight data.
	for _, t := range tasks {
		_ = e.q.RemoveInflight(ctx, id, t.ID)
		e.unprotect(t.ID)
		if err := e.q.EnqueueReady(ctx, t.ID, int(t.Priority), true); err != nil {
			log.Printf("reap requeue %s: %v", t.ID, err)
		}
		if w := e.pool.Get(id); w != nil {
			e.pool.Preempt(w, t.ID)
		}
	}
}

// reapLeasesTick recovers tasks whose lease was not renewed (process crash
// or stuck handler) even if the worker still heartbeats.
func (e *Engine) reapLeasesTick(ctx context.Context) {
	now := e.clk.Now()
	expired, err := e.q.ExpiredInflights(ctx, now)
	if err != nil {
		return
	}
	for _, in := range expired {
		t, err := e.repo.GetTask(ctx, in.TaskID)
		if err != nil {
			continue
		}
		if t.State != domain.StateRunning {
			_ = e.q.RemoveInflight(ctx, in.WorkerID, in.TaskID)
			continue
		}
		if err := e.repo.RequeueRunning(ctx, t.ID, now, "lease expired"); err != nil {
			continue
		}
		e.unprotect(t.ID)
		_ = e.q.RequeueInflight(ctx, in.WorkerID, t.ID, int(t.Priority))
		// Interrupt any local execution.
		if w := e.pool.Get(in.WorkerID); w != nil {
			e.pool.Preempt(w, t.ID)
		}
	}
}

// reconcile rebuilds scheduling state from PostgreSQL on startup:
// running tasks of previous instance are requeued, ready/pending tasks are
// re-inserted into Redis. The single-instance deployment makes this safe.
func (e *Engine) reconcile(ctx context.Context) error {
	if err := e.q.Reset(ctx); err != nil {
		return err
	}
	now := e.clk.Now()
	tasks, err := e.repo.RecoverableTasks(ctx)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		switch t.State {
		case domain.StateReady:
			_ = e.q.EnqueueReady(ctx, t.ID, int(t.Priority), false)
		case domain.StatePending:
			runAt := t.RunAt
			if runAt == nil {
				_ = e.repo.MarkReady(ctx, t.ID, now)
				_ = e.q.EnqueueReady(ctx, t.ID, int(t.Priority), false)
				continue
			}
			_ = e.q.AddDelay(ctx, t.ID, int(t.Priority), *runAt)
		case domain.StateRunning:
			// Previous instance died mid-execution: recover at queue head.
			if err := e.repo.RequeueRunning(ctx, t.ID, now, "startup recovery"); err == nil {
				_ = e.q.EnqueueReady(ctx, t.ID, int(t.Priority), true)
			}
		}
	}
	return nil
}

var _ = time.Second
