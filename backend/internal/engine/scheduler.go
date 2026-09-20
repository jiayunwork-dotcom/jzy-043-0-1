package engine

import (
	"context"
	"errors"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/queue"
	"taskforge/internal/workerpool"
)

// dispatchTick is one scheduler pass:
//  1. While a worker has a free slot, claim tasks. Strict priority order is
//     enforced by the queue; weighted fairness forces a low-priority slot
//     after FairnessThreshold consecutive high-priority dispatches.
//  2. If every slot is busy but Critical/High work is waiting, preempt one
//     interruptible (Normal/Low/Bulk) execution.
//
// Immediately after a preemption the freed slot must serve the urgent task
// that caused it, so fairness forcing is suppressed for that one claim.
func (e *Engine) dispatchTick(ctx context.Context) {
	now := e.clk.Now()
	suppressFairness := false

	for {
		for {
			w, _ := e.pool.FreeWorker()
			if w == nil {
				break
			}
			claim, res, forcedLow, ok := e.claimForSlot(ctx, w, now, suppressFairness)
			if !ok {
				break
			}
			suppressFairness = false

			// Fairness accounting.
			e.mu.Lock()
			if claim.Priority <= int(domain.PriorityHigh) {
				e.highStreak++
			} else {
				e.highStreak = 0
			}
			e.mu.Unlock()

			// A low-priority task pulled in through the fairness window is
			// protected from urgent preemption for this execution, otherwise
			// it could be preempted forever while highs keep arriving. We set
			// the marker strictly from the current dispatch, so a stale marker
			// left by a reaped earlier run can never make a later run
			// un-preemptable.
			if forcedLow {
				e.protect(claim.TaskID)
			} else {
				e.unprotect(claim.TaskID)
			}

			if !e.startTask(ctx, w, res, claim) {
				// CAS lost and task was put back; loop again with a fresh slot.
				continue
			}
		}

		// No free slots (or queue drained). If urgent work is waiting behind a
		// running interruptible task, preempt it and loop again so the freed
		// slot immediately picks up the urgent task in the same pass.
		if !e.tryPreempt(ctx, now) {
			return
		}
		suppressFairness = true
	}
}

// claimForSlot reserves one slot and pops a task for it. The returned claim
// is always backed by a usable slot. suppressFairness=true skips the
// weighted-fairness window (used right after a preemption). forcedLow
// reports whether the claim came through a forced fairness window.
func (e *Engine) claimForSlot(ctx context.Context, w *workerpool.Worker,
	now time.Time, suppressFairness bool) (claim *queue.Claim, res *workerpool.Reservation, forcedLow, ok bool) {

	// Reserve a slot FIRST, so a popped task always has somewhere to run
	// (prevents pop-without-slot requeue churn / lost claims).
	res, slotOK := e.pool.Reserve(w)
	if !slotOK {
		return nil, nil, false, false
	}

	e.mu.Lock()
	forceLow := !suppressFairness && e.highStreak >= e.opts.FairnessThreshold
	e.mu.Unlock()

	c, err := e.q.ClaimOne(ctx, forceLow, now)
	if err != nil {
		res.Release()
		return nil, nil, false, false
	}
	if c == nil && forceLow {
		// Nothing low-priority ready: reset streak and take a high-priority task.
		e.mu.Lock()
		e.highStreak = 0
		e.mu.Unlock()
		c, err = e.q.ClaimOne(ctx, false, now)
		forceLow = false
	}
	if err != nil || c == nil {
		res.Release()
		return nil, nil, false, false
	}
	return c, res, forceLow, true
}

// startTask performs the ready->running CAS and launches execution against
// an already-reserved slot. Returns false if the claim could not be started
// (it has been put back on the queue and the reservation released).
func (e *Engine) startTask(ctx context.Context, w *workerpool.Worker,
	res *workerpool.Reservation, claim *queue.Claim) bool {

	now := e.clk.Now()
	att, err := e.repo.StartAttempt(ctx, claim.TaskID, w.ID, e.leaseDeadline(), now)
	if err != nil {
		// Lost CAS (task canceled, reaped, duplicate pop): release the slot
		// and put the task back where it came from.
		res.Release()
		e.unprotect(claim.TaskID)
		if errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrNotFound) {
			_ = e.q.EnqueueReady(ctx, claim.TaskID, claim.Priority, claim.Head)
		}
		return false
	}
	t, err := e.repo.GetTask(ctx, claim.TaskID)
	if err != nil {
		res.Release()
		e.unprotect(claim.TaskID)
		_ = e.q.EnqueueReady(ctx, claim.TaskID, claim.Priority, true)
		return false
	}
	e.execute(w, res, t, att)
	return true
}

func (e *Engine) leaseDeadline() time.Time {
	return e.clk.Now().Add(e.opts.LeaseTTL)
}
