package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/store"
)

//  1. With a continuous stream of high-priority tasks, low-priority tasks
//     are still consumed once per FairnessThreshold high dispatches.
func TestFairnessLowPriorityNotStarved(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 1, FairnessThreshold: 3,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "hi", "lo")

	// fill with far more high tasks than low; lows must still surface
	for i := 0; i < 9; i++ {
		env.submit(t, SubmitInput{Type: "hi", Priority: domain.PriorityHigh})
	}
	for i := 0; i < 3; i++ {
		env.submit(t, SubmitInput{Type: "lo", Priority: domain.PriorityBulk})
	}

	ctx := context.Background()
	var order []string
	const total = 12
	// A pump goroutine keeps calling dispatchTick so newly freed slots and
	// preemptions are picked up promptly; the test itself drains start events.
	stopPump := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPump:
				return
			default:
				env.eng.dispatchTick(ctx)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	for dispatched := 0; dispatched < total; dispatched++ {
		select {
		case ev := <-ctrl.starts:
			order = append(order, ev.Type)
			close(ev.Gate)
			env.waitState(t, ev.TaskID, domain.StateSucceeded)
		case <-time.After(3 * time.Second):
			t.Fatalf("no dispatch at iteration %d; order=%v", dispatched, order)
		}
	}
	close(stopPump)

	// Highs between any two low dispatches must never exceed FairnessThreshold.
	highSinceLastLow := 0
	for i, ty := range order {
		if ty == "lo" {
			if highSinceLastLow > 3 {
				t.Fatalf("low starved at position %d: %d highs since previous low", i, highSinceLastLow)
			}
			highSinceLastLow = 0
		} else {
			highSinceLastLow++
		}
	}
	lows := 0
	for _, ty := range order {
		if ty == "lo" {
			lows++
		}
	}
	if lows != 3 {
		t.Fatalf("expected all 3 low tasks consumed, got %d; order=%v", lows, order)
	}
	t.Logf("dispatch order: %v", order)
}

//  2. A Critical task preempts a running Normal task: the interrupted task
//     returns to the head of its queue, is not lost, and runs again.
func TestCriticalPreemptsNormalAndRequeuesAtHead(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 1, FairnessThreshold: 100,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "normal", "critical", "other-normal")

	ctx := context.Background()

	// start a Normal task and block inside the handler
	normal := env.submit(t, SubmitInput{Type: "normal", Priority: domain.PriorityNormal})
	env.eng.dispatchTick(ctx)
	ev1 := awaitStart(ctrl)
	if ev1.TaskID != normal.ID {
		t.Fatalf("expected normal task to start first, got %s", ev1.TaskID)
	}
	env.waitState(t, normal.ID, domain.StateRunning)

	// submit Critical while Normal is running; dispatch must preempt and the
	// freed slot must immediately pick up the critical task in the same pass.
	crit := env.submit(t, SubmitInput{Type: "critical", Priority: domain.PriorityCritical})
	env.eng.dispatchTick(ctx)

	ev2 := awaitStart(ctrl)
	if ev2.TaskID != crit.ID {
		t.Fatalf("expected critical task to preempt, got %q (want %s)", ev2.TaskID, crit.ID)
	}
	env.waitState(t, crit.ID, domain.StateRunning)

	// interrupted normal task must be back to ready at the queue head
	got := env.waitState(t, normal.ID, domain.StateReady)
	if got.WorkerID != "" {
		t.Fatalf("preempted task still bound to worker %s", got.WorkerID)
	}
	depth, _ := env.q.ReadyDepth(ctx)
	if depth[int(domain.PriorityNormal)] != 1 {
		t.Fatalf("normal queue depth = %d, want 1 (preempted task at head)", depth[int(domain.PriorityNormal)])
	}
	claim, _ := env.q.ClaimOne(ctx, false, env.clk.Now())
	if claim == nil || claim.TaskID != normal.ID || !claim.Head {
		t.Fatalf("expected preempted task at head of normal queue, got %+v", claim)
	}
	// put it back so the scheduler can run it after critical completes
	_ = env.q.EnqueueReady(ctx, normal.ID, int(domain.PriorityNormal), true)

	// let critical finish
	close(ev2.Gate)
	env.waitState(t, crit.ID, domain.StateSucceeded)

	// the original (preempted) handler observes cancellation; releasing its
	// gate must not overwrite the requeued task state.
	close(ev1.Gate)

	// now schedule the recovered normal task again -> it reruns and finishes
	env.eng.dispatchTick(ctx)
	ev3 := awaitStart(ctrl)
	if ev3.TaskID != normal.ID {
		t.Fatalf("expected requeued normal task to run again, got %s", ev3.TaskID)
	}
	close(ev3.Gate)
	env.waitState(t, normal.ID, domain.StateSucceeded)
}

// 3) Delayed tasks enter their ready queue within ~one second of becoming due.
func TestDelayedTaskPromotedSecondLevel(t *testing.T) {
	env := newTestEnv(t, Options{LeaseTTL: time.Hour, WorkerTimeout: time.Hour})
	ctrl := newControllable()
	ctrl.register(env.reg, "later")

	env.submit(t, SubmitInput{Type: "later", Priority: domain.PriorityLow, DelaySeconds: 5})

	ctx := context.Background()
	// before due: still in delay zone, nothing ready
	env.clk.Advance(4 * time.Second)
	env.eng.delayTick(ctx)
	depth, _ := env.q.ReadyDepth(ctx)
	if depth[int(domain.PriorityLow)] != 0 {
		t.Fatalf("task became ready early: %d", depth[int(domain.PriorityLow)])
	}

	// cross the due time with a sub-second scan: promoted immediately
	env.clk.Advance(1050 * time.Millisecond)
	promotedAt := env.clk.Now()
	env.eng.delayTick(ctx)

	tk, _ := env.repo.GetTasks(ctx, nil) // ensure store reachable
	_ = tk
	depth, _ = env.q.ReadyDepth(ctx)
	if depth[int(domain.PriorityLow)] != 1 {
		t.Fatalf("due delayed task not promoted: depth=%d", depth[int(domain.PriorityLow)])
	}

	// and it actually runs
	env.eng.dispatchTick(ctx)
	ev := awaitStart(ctrl)
	close(ev.Gate)
	stored, _ := env.repo.GetTask(ctx, ev.TaskID)
	lag := stored.StartedAt.Sub(promotedAt)
	if lag > 250*time.Millisecond || lag < -time.Second {
		t.Fatalf("scheduling lag out of bounds: %v (started=%s promoted=%s)",
			lag, stored.StartedAt, promotedAt)
	}
}

//  4. When a worker is lost (no heartbeat), its running task is recovered and
//     re-enqueued, then executed by another worker.
func TestWorkerLossRequeuesInflightTask(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 2, SlotsPerWorker: 1, FairnessThreshold: 100,
		LeaseTTL: time.Hour, WorkerTimeout: 30 * time.Second,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "job")

	ctx := context.Background()
	task := env.submit(t, SubmitInput{Type: "job", Priority: domain.PriorityNormal})

	env.eng.dispatchTick(ctx)
	ev := awaitStart(ctrl)
	if ev.TaskID != task.ID {
		t.Fatalf("unexpected task started: %s", ev.TaskID)
	}
	running := env.waitState(t, task.ID, domain.StateRunning)
	lostWorker := running.WorkerID
	if lostWorker == "" {
		t.Fatal("task has no worker")
	}

	// simulate loss: stale heartbeat, then run the reaper
	if err := env.eng.SimulateWorkerLoss(ctx, lostWorker); err != nil {
		t.Fatal(err)
	}
	env.eng.reapWorkersTick(ctx)

	got := env.waitState(t, task.ID, domain.StateReady)
	_ = got
	depth, _ := env.q.ReadyDepth(ctx)
	if depth[int(domain.PriorityNormal)] != 1 {
		t.Fatalf("recovered task not in queue: depth=%d", depth[int(domain.PriorityNormal)])
	}

	// release the dead handler so its goroutine exits; it must not overwrite state
	close(ev.Gate)

	// it runs again and succeeds (on the surviving worker)
	env.eng.dispatchTick(ctx)
	ev2 := awaitStart(ctrl)
	if ev2.TaskID != task.ID {
		t.Fatalf("recovered task did not rerun: got %s", ev2.TaskID)
	}
	rerun := env.waitState(t, task.ID, domain.StateRunning)
	if rerun.WorkerID == "" {
		t.Fatal("rerun has no worker")
	}
	close(ev2.Gate)
	env.waitState(t, task.ID, domain.StateSucceeded)
}

// 5) Retries exhausted => task enters the dead letter zone with typed errors.
func TestRetriesExhaustedGoesDead(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 1, FairnessThreshold: 100,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "boom")
	ctrl.failAll["boom"] = true
	ctrl.failKind = "PermanentFailure"

	ctx := context.Background()
	task := env.submit(t, SubmitInput{
		Type: "boom", Priority: domain.PriorityHigh, MaxRetries: 2,
		RetryPolicy: &domain.RetryPolicy{
			Kind: domain.RetryFixed, BaseInterval: domainDur(10 * time.Millisecond), MaxRetries: 2,
		},
	})

	// pump: promote due retries and dispatch continuously (fake-clock driven)
	stopPump := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPump:
				return
			default:
				env.eng.delayTick(ctx)
				env.eng.dispatchTick(ctx)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for attempt := 0; attempt <= 2; attempt++ {
		// each attempt surfaces exactly one handler start
		var ev startEvent
		select {
		case ev = <-ctrl.starts:
		case <-time.After(2 * time.Second):
			tk, _ := env.repo.GetTask(ctx, task.ID)
			t.Fatalf("attempt %d handler never started; state=%+v", attempt, tk)
		}
		if ev.TaskID != task.ID {
			t.Fatalf("unexpected task started during retries: %s", ev.TaskID)
		}
		// confirm it is actually running on a worker before releasing
		env.waitState(t, task.ID, domain.StateRunning)
		close(ev.Gate)

		if attempt < 2 {
			// failure -> pending with a short fake-clock delay
			env.waitState(t, task.ID, domain.StatePending)
			env.clk.Advance(20 * time.Millisecond)
		}
	}
	close(stopPump)

	env.waitState(t, task.ID, domain.StateDead)
	final, _ := env.repo.GetTask(ctx, task.ID)
	if final.ErrorType != "PermanentFailure" {
		t.Fatalf("error_type=%s want PermanentFailure", final.ErrorType)
	}
	attempts, _ := env.repo.ListAttempts(ctx, task.ID)
	if len(attempts) != 3 {
		t.Fatalf("attempts=%d want 3", len(attempts))
	}
	dead, _ := env.repo.CountDead(ctx)
	if dead != 1 {
		t.Fatalf("dead count=%d want 1", dead)
	}
}

// 6) Batch retry of dead tasks puts them back in ready queues and they run.
func TestDeadBatchRetryReenqueues(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 4, FairnessThreshold: 100,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "boom2")
	ctrl.failAll["boom2"] = true

	var ids []string
	for i := 0; i < 3; i++ {
		tk := env.submit(t, SubmitInput{
			Type: "boom2", Priority: domain.PriorityNormal,
			RetryPolicy: &domain.RetryPolicy{Kind: domain.RetryFixed, MaxRetries: 0},
		})
		ids = append(ids, tk.ID)
	}
	ctx := context.Background()

	pumpOnce := func() (stop func()) {
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					env.eng.dispatchTick(ctx)
					time.Sleep(time.Millisecond)
				}
			}
		}()
		return func() { close(done); wg.Wait() }
	}
	stopPump := pumpOnce()
	for i := 0; i < 3; i++ {
		ev := awaitStart(ctrl)
		if ev.Gate == nil {
			t.Fatalf("attempt %d never started", i)
		}
		close(ev.Gate)
	}
	if !waitFor(2*time.Second, func() bool {
		n, _ := env.repo.CountDead(ctx)
		return n == 3
	}) {
		t.Fatal("not all 3 tasks went dead")
	}

	// operator fixes the handler and fully stops dispatch, then batch retries
	// so the requeued tasks are observably "ready" before dispatch resumes.
	ctrl.failAll["boom2"] = false
	stopPump()
	n, err := env.eng.RetryDead(ctx, ids)
	if err != nil || n != 3 {
		t.Fatalf("RetryDead n=%d err=%v", n, err)
	}
	for _, id := range ids {
		env.waitState(t, id, domain.StateReady)
	}

	stopPump = pumpOnce()
	for i := 0; i < 3; i++ {
		ev := awaitStart(ctrl)
		if ev.Gate == nil {
			t.Fatalf("retried attempt %d never started", i)
		}
		close(ev.Gate)
	}
	stopPump()
	for _, id := range ids {
		env.waitState(t, id, domain.StateSucceeded)
	}
}

// 7a) DAG nodes execute in dependency order: A -> (B,C) -> D.
func TestDAGDependencyOrder(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 1, SlotsPerWorker: 1, FairnessThreshold: 100,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "node")

	def := &domain.DAGDef{
		Name:          "fanout",
		FailurePolicy: domain.NodeFailAbort,
		Nodes: []domain.DAGNodeDef{
			{ID: "A", TaskType: "node", Priority: domain.PriorityNormal},
			{ID: "B", TaskType: "node", Priority: domain.PriorityNormal, DependsOn: []string{"A"}},
			{ID: "C", TaskType: "node", Priority: domain.PriorityNormal, DependsOn: []string{"A"}},
			{ID: "D", TaskType: "node", Priority: domain.PriorityNormal, DependsOn: []string{"B", "C"}},
		},
	}
	ctx := context.Background()
	d, err := env.eng.DAGs().Instantiate(ctx, def)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}

	completed := map[string]bool{}
	finishOne := func(want string) {
		env.eng.dispatchTick(ctx)
		ev := awaitStart(ctrl)
		node := taskNode(t, env, ev.TaskID)
		if node != want {
			t.Fatalf("expected node %s to run, got %s", want, node)
		}
		close(ev.Gate)
		if !waitFor(time.Second, func() bool {
			g, _ := env.repo.GetDAG(ctx, d.ID)
			n := findTestNode(g, node)
			return n != nil && n.State == domain.NodeSucceeded
		}) {
			t.Fatalf("node %s did not succeed", node)
		}
		completed[node] = true
	}

	finishOne("A")
	// B and C may run in either order; D only after both
	env.eng.dispatchTick(ctx)
	first := awaitStart(ctrl)
	node1 := taskNode(t, env, first.TaskID)
	if node1 != "B" && node1 != "C" {
		t.Fatalf("expected B or C, got %s", node1)
	}
	close(first.Gate)
	waitForSucceeded(t, env, d.ID, node1)

	env.eng.dispatchTick(ctx)
	second := awaitStart(ctrl)
	node2 := taskNode(t, env, second.TaskID)
	if node2 != "B" && node2 != "C" || node2 == node1 {
		t.Fatalf("expected the other of B/C, got %s", node2)
	}
	close(second.Gate)
	waitForSucceeded(t, env, d.ID, node2)

	finishOne("D")

	final, _ := env.repo.GetDAG(ctx, d.ID)
	if final.Status != domain.DAGSucceeded {
		t.Fatalf("dag status=%s want succeeded", final.Status)
	}
}

// 7b) A cyclic DAG is rejected before anything is enqueued.
func TestDAGCycleRejected(t *testing.T) {
	env := newTestEnv(t, Options{LeaseTTL: time.Hour, WorkerTimeout: time.Hour})
	ctrl := newControllable()
	ctrl.register(env.reg, "node")

	def := &domain.DAGDef{
		Name: "cyclic",
		Nodes: []domain.DAGNodeDef{
			{ID: "A", TaskType: "node", DependsOn: []string{"C"}},
			{ID: "B", TaskType: "node", DependsOn: []string{"A"}},
			{ID: "C", TaskType: "node", DependsOn: []string{"B"}},
		},
	}
	if _, err := env.eng.DAGs().Instantiate(context.Background(), def); err == nil {
		t.Fatal("expected cycle error, got nil")
	}
	dags, _ := env.repo.ListDAGs(context.Background(), 10)
	if len(dags) != 0 {
		t.Fatalf("cyclic dag was persisted: %d", len(dags))
	}
}

//  8. Concurrent submissions never lose or duplicate tasks: every submitted
//     id appears exactly once and eventually finishes.
func TestConcurrentSubmitNoLossNoDuplication(t *testing.T) {
	env := newTestEnv(t, Options{
		WorkerCount: 4, SlotsPerWorker: 8, FairnessThreshold: 100,
		LeaseTTL: time.Hour, WorkerTimeout: time.Hour,
	})
	ctrl := newControllable()
	ctrl.register(env.reg, "work")

	const n = 200
	var wg sync.WaitGroup
	ids := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tk := env.submit(t, SubmitInput{
				Type:     "work",
				Priority: domain.Priority(i % 5),
				Payload:  []byte(fmt.Sprintf(`{"i":%d}`, i)),
			})
			ids <- tk.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	var all []string
	seen := map[string]int{}
	for id := range ids {
		all = append(all, id)
		seen[id]++
	}
	if len(all) != n {
		t.Fatalf("submitted=%d want %d", len(all), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("task %s submitted %d times", id, count)
		}
	}

	// drain: dispatch everything and release handlers as they start
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case ev := <-ctrl.starts:
				close(ev.Gate)
			case <-done:
				return
			}
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		env.eng.dispatchTick(ctx)
		list, _ := env.repo.ListTasks(ctx, store.TaskFilter{
			States: []domain.TaskState{domain.StateReady, domain.StatePending, domain.StateRunning},
			Limit:  n + 10,
		})
		if len(list.Tasks) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(done)

	list, _ := env.repo.ListTasks(ctx, store.TaskFilter{Limit: n + 10})
	succeeded := 0
	if int64(len(list.Tasks)) != list.Total {
		t.Fatalf("list count mismatch: %d vs total %d", len(list.Tasks), list.Total)
	}
	for _, tk := range list.Tasks {
		if tk.State != domain.StateSucceeded {
			t.Fatalf("task %s ended in state %s", tk.ID, tk.State)
		}
		succeeded++
	}
	if succeeded != n {
		t.Fatalf("succeeded=%d want %d", succeeded, n)
	}
}

// ---- small helpers --------------------------------------------------------

func domainDur(d time.Duration) domain.Duration { return domain.Duration(d) }

func taskNode(t *testing.T, env *testEnv, taskID string) string {
	t.Helper()
	tk, err := env.repo.GetTask(context.Background(), taskID)
	if err != nil || tk == nil {
		t.Fatalf("get task %s: %v", taskID, err)
	}
	return tk.DAGNode
}

func waitForSucceeded(t *testing.T, env *testEnv, dagID, node string) {
	t.Helper()
	if !waitFor(time.Second, func() bool {
		g, _ := env.repo.GetDAG(context.Background(), dagID)
		n := findTestNode(g, node)
		return n != nil && n.State == domain.NodeSucceeded
	}) {
		t.Fatalf("node %s not succeeded", node)
	}
}

func findTestNode(d *domain.DAG, id string) *domain.DAGNode {
	for _, n := range d.Nodes {
		if n.NodeID == id {
			return n
		}
	}
	return nil
}
