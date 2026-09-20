package engine

import (
	"context"
	"errors"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/processor"
	"taskforge/internal/retry"
	"taskforge/internal/workerpool"
)

// execute runs one task on a worker slot. It owns the full attempt
// lifecycle: lease renewal, handler invocation, timeout/preemption
// observation, success/failure persistence, retry scheduling, callbacks
// and DAG notifications.
func (e *Engine) execute(w *workerpool.Worker, res *workerpool.Reservation,
	t *domain.Task, att *domain.Attempt) {
	timeout := t.Timeout.Std()
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	// Convert the reserved slot into a live, timeout-bounded execution.
	exec := res.Confirm(t.ID, timeout)
	if exec == nil {
		_ = e.q.EnqueueReady(context.Background(), t.ID, int(t.Priority), true)
		return
	}

	go func() {
		defer e.pool.Release(w, t.ID)

		ctx := exec.Ctx // canceled on timeout OR preemption

		// Lease renewal loop; ends when execution ends.
		leaseStop := make(chan struct{})
		go e.renewLoop(leaseStop, w.ID, t.ID)
		defer close(leaseStop)

		if err := e.q.AddInflight(ctx, w.ID, t.ID, int(t.Priority), e.leaseDeadline()); err != nil {
			// best effort: the DB is the authoritative lease source.
		}

		h, err := e.reg.Get(t.Type)
		if err != nil {
			e.finishFailed(w, t, att, err, "handler-missing")
			_ = e.q.RemoveInflight(context.Background(), w.ID, t.ID)
			return
		}

		start := e.clk.Now()
		result, herr := h.Handle(ctx, t)
		end := e.clk.Now()
		_ = e.q.RemoveInflight(context.Background(), w.ID, t.ID)

		if herr != nil {
			if errors.Is(exec.Ctx.Err(), context.DeadlineExceeded) {
				e.finishFailed(w, t, att, errors.New("task execution timeout"), "timeout")
				return
			}
			if errors.Is(exec.Ctx.Err(), context.Canceled) {
				// Preemption: PreemptRunning already moved the DB row back to
				// ready and requeued it. This late execution must be dropped.
				e.unprotect(t.ID)
				return
			}
			errType := "handler-error"
			var te processor.TypedError
			if errors.As(herr, &te) {
				errType = te.ErrorType()
			}
			e.finishFailed(w, t, att, herr, errType)
			return
		}
		// Handler returned nil but the context was canceled concurrently.
		if errors.Is(exec.Ctx.Err(), context.Canceled) {
			e.unprotect(t.ID)
			return
		}

		e.finishSuccess(w, t, att, result, start, end)
	}()
}

// renewLoop keeps both the Redis lease and the DB heartbeat fresh.
func (e *Engine) renewLoop(stop chan struct{}, workerID, taskID string) {
	interval := e.opts.LeaseTTL / 3
	if interval <= 0 {
		interval = 5 * time.Second
	}
	tk := e.clk.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-e.stopCh:
			return
		case <-tk.C():
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			expires := e.clk.Now().Add(e.opts.LeaseTTL)
			ok, err := e.q.RenewInflight(ctx, workerID, taskID, expires)
			if err == nil && ok {
				_ = e.repo.RenewLease(ctx, taskID, workerID, expires)
			}
			cancel()
		}
	}
}

func (e *Engine) finishSuccess(w *workerpool.Worker, t *domain.Task, att *domain.Attempt,
	result []byte, start, end time.Time) {

	e.unprotect(t.ID)
	att.State = domain.AttemptSucceeded
	if err := e.repo.CompleteAttempt(context.Background(), att,
		domain.StateSucceeded, result, "", "", end); err != nil {
		// ErrConflict: preempted/reaped late result — ignore.
		return
	}
	_ = e.poolBump(w.ID, 1, 0, 0)
	schedLatency := start.Sub(t.ReadyAt)
	t.State = domain.StateSucceeded
	t.Result = result
	e.mc.RecordOutcome(t, end.Sub(start), schedLatency, end)

	e.fireCallback(t, true, result, "")
	e.afterDAGNode(t, true, "")
}

func (e *Engine) finishFailed(w *workerpool.Worker, t *domain.Task, att *domain.Attempt,
	herr error, errType string) {

	end := e.clk.Now()
	errMsg := herr.Error()
	e.unprotect(t.ID)
	t.LastError = errMsg
	t.ErrorType = errType

	nextRun, willRetry := retry.NextRun(t.RetryPolicy, att.AttemptNo, end)

	if willRetry {
		// Single transaction: attempt=failed, task running -> pending(runAt).
		if err := e.repo.FailAttemptForRetry(context.Background(), att,
			errMsg, errType, nextRun); err != nil {
			// Lost ownership (preempted/reaped): late result is dropped.
			return
		}
		_ = e.poolBump(w.ID, 0, 1, 0)
		t.State = domain.StatePending
		e.mc.RecordOutcome(t, 0, 0, end)

		if err := e.q.AddDelay(context.Background(), t.ID, int(t.Priority), nextRun); err != nil {
			// Redis failed: promote to ready immediately rather than lose it.
			_ = e.repo.MarkReady(context.Background(), t.ID, end)
			_ = e.q.EnqueueReady(context.Background(), t.ID, int(t.Priority), true)
		}
		return
	}

	// Retries exhausted: attempt=failed, task running -> dead.
	att.State = domain.AttemptFailed
	if err := e.repo.CompleteAttempt(context.Background(), att,
		domain.StateDead, nil, errMsg, errType, end); err != nil {
		return
	}
	_ = e.poolBump(w.ID, 0, 1, 0)
	t.State = domain.StateDead
	e.mc.RecordOutcome(t, 0, 0, end)
	e.fireCallback(t, false, nil, errMsg)
	e.afterDAGNode(t, false, errMsg)
}

func (e *Engine) poolBump(workerID string, succeeded, failed, preempted int) error {
	return e.repo.BumpWorkerCounters(context.Background(), workerID, succeeded, failed, preempted)
}
