package engine

import (
	"context"
	"testing"
	"time"

	"taskforge/internal/domain"
)

// waitStartOf blocks until the controllable handler reports a start for the
// given task id. It relies on the buffered starts channel plus a drain, so it
// is independent of dispatch/start goroutine scheduling jitter.
func waitStartOf(c *controllable, taskID string) startEvent {
	// drain any buffered starts that arrived earlier and remember the target
	var buffered []startEvent
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case ev := <-c.starts:
			if ev.TaskID == taskID {
				return ev
			}
			buffered = append(buffered, ev)
		default:
			if time.Now().After(deadline) {
				return startEvent{}
			}
			time.Sleep(time.Millisecond)
		}
		if time.Now().After(deadline) {
			return startEvent{}
		}
	}
}

// Combined invariant: after a preemption frees a slot, the urgent task that
// triggered it runs even if the scheduler is inside a weighted-fairness
// window (highStreak >= threshold). The preempted low task must not be
// re-picked before the urgent task.
func TestPreemptionWinsOverFairnessWindow(t *testing.T) {
	// threshold 2: after two highs the next claim would be forced low.
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 1, FairnessThreshold: 2,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "hi", "bulk", "critical")
	ctx := context.Background()

	dispatched := []string{}
	// run two highs (streak -> 2, next claim would be forced low)
	var highs []*domain.Task
	for i := 0; i < 2; i++ {
		highs = append(highs, env.submit(t, SubmitInput{Type: "hi", Priority: domain.PriorityHigh}))
	}
	for _, h := range highs {
		env.eng.dispatchTick(ctx)
		env.waitState(t, h.ID, domain.StateRunning)
		ev := waitStartOf(ctrl, h.ID)
		if ev.TaskID != h.ID {
			t.Fatalf("expected hi %s to start", h.ID)
		}
		close(ev.Gate)
		env.waitState(t, h.ID, domain.StateSucceeded)
		dispatched = append(dispatched, "hi")
	}

	// only a bulk is waiting; the fairness window forces it ahead of highs
	bulk := env.submit(t, SubmitInput{Type: "bulk", Priority: domain.PriorityBulk})
	env.eng.dispatchTick(ctx)
	env.waitState(t, bulk.ID, domain.StateRunning)
	ev := waitStartOf(ctrl, bulk.ID)
	if ev.TaskID != bulk.ID {
		t.Fatal("fairness window should run bulk first")
	}

	// a critical arrives; bulk is fairness-protected, so it MUST NOT be
	// preempted even though critical is waiting.
	crit := env.submit(t, SubmitInput{Type: "critical", Priority: domain.PriorityCritical})
	env.eng.dispatchTick(ctx)
	time.Sleep(80 * time.Millisecond)
	env.waitState(t, bulk.ID, domain.StateRunning)
	env.waitState(t, crit.ID, domain.StateReady)

	// finish bulk; the slot frees and critical (suppressed fairness) runs
	close(ev.Gate)
	env.waitState(t, bulk.ID, domain.StateSucceeded)
	env.eng.dispatchTick(ctx)
	env.waitState(t, crit.ID, domain.StateRunning)
	ev2 := waitStartOf(ctrl, crit.ID)
	if ev2.TaskID != crit.ID {
		t.Fatalf("after fairness slot freed, critical must run, got %q", ev2.TaskID)
	}
	close(ev2.Gate)
	env.waitState(t, crit.ID, domain.StateSucceeded)
}

// A non-fairness running Normal task is preempted by Critical, and the freed
// slot serves Critical immediately (not the requeued normal even with a high
// streak pending).
func TestPreemptedUrgentRunsBeforeRequeuedTask(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 1, FairnessThreshold: 1,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "normal", "critical")
	ctx := context.Background()

	normal := env.submit(t, SubmitInput{Type: "normal", Priority: domain.PriorityNormal})
	env.eng.dispatchTick(ctx)
	env.waitState(t, normal.ID, domain.StateRunning)
	ev1 := waitStartOf(ctrl, normal.ID)

	crit := env.submit(t, SubmitInput{Type: "critical", Priority: domain.PriorityCritical})
	env.eng.dispatchTick(ctx)
	env.waitState(t, crit.ID, domain.StateRunning)
	ev2 := waitStartOf(ctrl, crit.ID)
	if ev2.TaskID != crit.ID {
		t.Fatalf("critical must take preempted slot, got %q", ev2.TaskID)
	}
	close(ev2.Gate)
	env.waitState(t, crit.ID, domain.StateSucceeded)
	close(ev1.Gate)

	env.eng.dispatchTick(ctx)
	env.waitState(t, normal.ID, domain.StateRunning)
	ev3 := waitStartOf(ctrl, normal.ID)
	if ev3.TaskID != normal.ID {
		t.Fatalf("requeued normal must run after critical, got %q", ev3.TaskID)
	}
	close(ev3.Gate)
	env.waitState(t, normal.ID, domain.StateSucceeded)
}
