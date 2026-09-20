package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"taskforge/internal/clock"
	"taskforge/internal/domain"
	"taskforge/internal/processor"
	"taskforge/internal/queue"
	"taskforge/internal/store"
)

// testEnv wires an engine against in-memory store/queue with no background
// loops; tests drive ticks deterministically.
type testEnv struct {
	eng  *Engine
	reg  *processor.Registry
	clk  *clock.FakeClock
	q    *queue.MemoryQueue
	repo *store.MemStore
}

func newTestEnv(t *testing.T, opts Options) *testEnv {
	t.Helper()
	start := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	repo := store.NewMem()
	q := queue.NewMemory()
	reg := processor.NewRegistry()
	if opts.WorkerCount == 0 {
		opts.WorkerCount = 2
	}
	if opts.SlotsPerWorker == 0 {
		opts.SlotsPerWorker = 2
	}
	if opts.FairnessThreshold == 0 {
		opts.FairnessThreshold = 3
	}
	if opts.LeaseTTL == 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.WorkerTimeout == 0 {
		opts.WorkerTimeout = 30 * time.Second
	}
	eng := New(clk, repo, q, reg, opts)
	ctx := context.Background()
	for i := 0; i < opts.WorkerCount; i++ {
		id := workerID(i)
		if _, err := eng.pool.Add(ctx, id, opts.SlotsPerWorker); err != nil {
			t.Fatalf("add worker: %v", err)
		}
	}
	return &testEnv{eng: eng, reg: reg, clk: clk, q: q, repo: repo}
}

func workerID(i int) string {
	return "worker-" + itoa2(i+1)
}

func itoa2(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func (e *testEnv) submit(t *testing.T, in SubmitInput) *domain.Task {
	t.Helper()
	task, _, err := e.eng.Submit(context.Background(), in)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return task
}

// waitFor blocks until cond is true or the deadline.
func waitFor(deadline time.Duration, cond func() bool) bool {
	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func (e *testEnv) taskState(id string) domain.TaskState {
	tk, err := e.repo.GetTask(context.Background(), id)
	if err != nil || tk == nil {
		return ""
	}
	return tk.State
}

func (e *testEnv) waitState(t *testing.T, id string, want domain.TaskState) *domain.Task {
	t.Helper()
	var tk *domain.Task
	if !waitFor(2*time.Second, func() bool {
		var err error
		tk, err = e.repo.GetTask(context.Background(), id)
		return err == nil && tk != nil && tk.State == want
	}) {
		got, _ := e.repo.GetTask(context.Background(), id)
		t.Fatalf("task %s state=%v, want %s", id, got, want)
	}
	return tk
}

// startEvent is delivered when a handler begins: close Gate to release it.
type startEvent struct {
	TaskID string
	Type   string
	Gate   chan struct{}
}

// controllable is a handler whose executions can be released one by one and
// which can fail a configurable number of times per task type.
type controllable struct {
	mu       sync.Mutex
	starts   chan startEvent
	failN    map[string]int // type -> remaining forced failures
	failAll  map[string]bool
	failKind string
	ran      []string
}

func newControllable() *controllable {
	return &controllable{
		starts:  make(chan startEvent, 4096),
		failN:   map[string]int{},
		failAll: map[string]bool{},
	}
}

func (c *controllable) register(reg *processor.Registry, types ...string) {
	for _, ty := range types {
		reg.Register(ty, c)
	}
}

func (c *controllable) Name() string { return "ctrl" }

func (c *controllable) Handle(ctx context.Context, task *domain.Task) ([]byte, error) {
	c.mu.Lock()
	c.ran = append(c.ran, task.ID)
	kind := c.failKind
	n := c.failN[task.Type]
	if n > 0 {
		c.failN[task.Type] = n - 1
	}
	always := c.failAll[task.Type]
	c.mu.Unlock()

	gate := make(chan struct{})
	select {
	case c.starts <- startEvent{TaskID: task.ID, Type: task.Type, Gate: gate}:
	default:
	}
	select {
	case <-gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if always {
		if kind == "" {
			kind = "PermanentFailure"
		}
		return nil, processor.WrapError(kind, errTest{})
	}
	if n > 0 {
		if kind == "" {
			kind = "TransientFailure"
		}
		return nil, processor.WrapError(kind, errTest{})
	}
	return []byte(`{"ok":true}`), nil
}

// awaitStart waits for the next handler start (with a generous real deadline).
func awaitStart(c *controllable) startEvent {
	select {
	case ev := <-c.starts:
		return ev
	case <-time.After(2 * time.Second):
		return startEvent{}
	}
}

// drainStarts returns and closes any already-signaled starts.
func drainStarts(c *controllable) int {
	n := 0
	for {
		select {
		case ev := <-c.starts:
			close(ev.Gate)
			n++
		default:
			return n
		}
	}
}

type errTest struct{}

func (errTest) Error() string { return "test failure" }
