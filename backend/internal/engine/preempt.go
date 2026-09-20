package engine

import (
	"context"
	"errors"
	"time"

	"taskforge/internal/domain"
)

// tryPreempt implements Critical/High preemption and reports whether it
// freed a slot. It:
//   - runs only when Critical/High work is waiting and every slot is busy,
//   - interrupts only a Normal/Low/Bulk task (Critical/High are never preempted),
//   - moves the interrupted task to the HEAD of its ready queue,
//   - cancels the worker's execution context.
func (e *Engine) tryPreempt(ctx context.Context, now time.Time) bool {
	depth, err := e.q.ReadyDepth(ctx)
	if err != nil {
		return false
	}
	// Both Critical and High may preempt tasks at Normal and below.
	if depth[0] == 0 && depth[1] == 0 {
		return false
	}
	minPri := domain.PriorityNormal

	w := e.pool.PreemptTarget()
	if w == nil {
		return false
	}
	e.mu.Lock()
	protect := make([]string, 0, len(e.protected))
	for id := range e.protected {
		protect = append(protect, id)
	}
	e.mu.Unlock()
	t, err := e.repo.PreemptRunning(ctx, w.ID, minPri, protect, now)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return false
		}
		return false
	}
	// Move lease -> head of ready queue (atomic Redis operation).
	if err := e.q.RequeueInflight(ctx, w.ID, t.ID, int(t.Priority)); err != nil {
		// Redis failed but the DB already says ready; enqueue directly so the
		// task remains visible to schedulers.
		_ = e.q.EnqueueReady(ctx, t.ID, int(t.Priority), true)
	}
	// Interrupt the handler; the late execution result is discarded because
	// its DB row is no longer running.
	e.pool.Preempt(w, t.ID)
	_ = e.repo.BumpWorkerCounters(ctx, w.ID, 0, 0, 1)
	return true
}

// markWorkerDraining is used by the graceful-shutdown API.
func (e *Engine) markWorkerDraining(ctx context.Context, id string) error {
	if err := e.repo.DrainWorker(ctx, id, e.clk.Now()); err != nil {
		return err
	}
	e.pool.Drain(id)
	return nil
}
