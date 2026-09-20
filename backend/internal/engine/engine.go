// Package engine wires every subsystem together: task submission, the
// strict-priority + weighted-fairness scheduler, preemption, execution with
// timeouts, lease renewal, worker/lease reaping, delay promotion, retry and
// dead-letter handling, DAG orchestration callbacks and metrics.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"taskforge/internal/clock"
	"taskforge/internal/dag"
	"taskforge/internal/domain"
	"taskforge/internal/metrics"
	"taskforge/internal/processor"
	"taskforge/internal/queue"
	"taskforge/internal/store"
	"taskforge/internal/workerpool"
)

// Options configures an Engine.
type Options struct {
	WorkerCount       int
	SlotsPerWorker    int
	FairnessThreshold int
	DispatchInterval  time.Duration
	DelayScanInterval time.Duration
	HeartbeatInterval time.Duration
	WorkerTimeout     time.Duration
	LeaseTTL          time.Duration
}

// Engine is the standalone service kernel.
type Engine struct {
	clk  clock.Clock
	repo store.Repository
	q    queue.Queue
	pool *workerpool.Pool
	reg  *processor.Registry
	mc   *metrics.Collector
	dags *dag.Orchestrator

	opts Options

	httpClient *http.Client

	mu         sync.Mutex
	highStreak int // consecutive high-priority (critical/high) dispatches
	protected  map[string]struct{}
	started    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// protect marks a task exempt from urgent preemption for its current run.
func (e *Engine) protect(taskID string) {
	e.mu.Lock()
	if e.protected == nil {
		e.protected = map[string]struct{}{}
	}
	e.protected[taskID] = struct{}{}
	e.mu.Unlock()
}

// unprotect releases the preemption exemption (called on completion).
func (e *Engine) unprotect(taskID string) {
	e.mu.Lock()
	delete(e.protected, taskID)
	e.mu.Unlock()
}

func (e *Engine) isProtected(taskID string) bool {
	e.mu.Lock()
	_, ok := e.protected[taskID]
	e.mu.Unlock()
	return ok
}

func New(clk clock.Clock, repo store.Repository, q queue.Queue, reg *processor.Registry, opts Options) *Engine {
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = 3
	}
	if opts.SlotsPerWorker <= 0 {
		opts.SlotsPerWorker = 4
	}
	if opts.FairnessThreshold <= 0 {
		opts.FairnessThreshold = 5
	}
	if opts.DispatchInterval <= 0 {
		opts.DispatchInterval = 50 * time.Millisecond
	}
	if opts.DelayScanInterval <= 0 {
		opts.DelayScanInterval = 250 * time.Millisecond
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 3 * time.Second
	}
	if opts.WorkerTimeout <= 0 {
		opts.WorkerTimeout = 15 * time.Second
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 15 * time.Second
	}
	e := &Engine{
		clk:        clk,
		repo:       repo,
		q:          q,
		pool:       workerpool.New(clk, repo),
		reg:        reg,
		mc:         metrics.New(clk.Now()),
		opts:       opts,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		stopCh:     make(chan struct{}),
	}
	e.dags = dag.NewOrchestrator(repo, clk, e.submitDAGNodeTask)
	return e
}

// Registry exposes the handler registry (used to register demo handlers).
func (e *Engine) Registry() *processor.Registry { return e.reg }
func (e *Engine) Pool() *workerpool.Pool        { return e.pool }
func (e *Engine) Metrics() *metrics.Collector   { return e.mc }
func (e *Engine) DAGs() *dag.Orchestrator       { return e.dags }

// SubmitInput is the HTTP-facing task submission payload.
type SubmitInput struct {
	Type           string              `json:"type"`
	Payload        []byte              `json:"payload"`
	Priority       domain.Priority     `json:"priority"`
	DelaySeconds   float64             `json:"delay_seconds"`
	TimeoutSeconds float64             `json:"timeout_seconds"`
	MaxRetries     int                 `json:"max_retries"`
	RetryPolicy    *domain.RetryPolicy `json:"retry_policy"`
	CallbackURL    string              `json:"callback_url"`
	IdempotencyKey string              `json:"idempotency_key"`
}

// Submit validates and persists a task and places it in ready/delay zone.
func (e *Engine) Submit(ctx context.Context, in SubmitInput) (*domain.Task, bool, error) {
	if in.Type == "" {
		return nil, false, errors.New("task type is required")
	}
	if !e.reg.Has(in.Type) {
		return nil, false, fmt.Errorf("unknown task type %q", in.Type)
	}
	if in.Priority < domain.PriorityCritical || in.Priority > domain.PriorityBulk {
		return nil, false, errors.New("invalid priority")
	}
	now := e.clk.Now()

	rp := domain.RetryPolicy{
		Kind:         domain.RetryExponential,
		MaxRetries:   in.MaxRetries,
		BaseInterval: domain.Duration(time.Second),
	}
	if in.RetryPolicy != nil {
		rp = *in.RetryPolicy
		if rp.MaxRetries == 0 && in.MaxRetries != 0 {
			rp.MaxRetries = in.MaxRetries
		}
		if rp.BaseInterval == 0 {
			rp.BaseInterval = domain.Duration(time.Second)
		}
	}
	timeout := 60 * time.Second
	if in.TimeoutSeconds > 0 {
		timeout = time.Duration(in.TimeoutSeconds * float64(time.Second))
	}
	state := domain.StateReady
	var runAt *time.Time
	if in.DelaySeconds > 0 {
		state = domain.StatePending
		r := now.Add(time.Duration(in.DelaySeconds * float64(time.Second)))
		runAt = &r
	}
	t := &domain.Task{
		ID:             "task_" + queue.NewID(),
		IdempotencyKey: in.IdempotencyKey,
		Type:           in.Type,
		Payload:        in.Payload,
		Priority:       in.Priority,
		State:          state,
		RunAt:          runAt,
		Timeout:        domain.Duration(timeout),
		RetryPolicy:    rp,
		MaxRetries:     rp.MaxRetries,
		CallbackURL:    in.CallbackURL,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if state == domain.StateReady {
		t.ReadyAt = now
	}

	existing, err := e.repo.CreateTask(ctx, t)
	if err != nil {
		return nil, false, err
	}
	if existing != nil { // idempotency hit
		return existing, true, nil
	}

	if state == domain.StatePending {
		if err := e.q.AddDelay(ctx, t.ID, int(t.Priority), *runAt); err != nil {
			return nil, false, err
		}
	} else {
		if err := e.q.EnqueueReady(ctx, t.ID, int(t.Priority), false); err != nil {
			return nil, false, err
		}
	}
	return t, false, nil
}

// Start creates the worker pool, reconciles state after a possible restart
// and launches all background loops.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return errors.New("engine already started")
	}
	e.started = true
	e.mu.Unlock()

	for i := 0; i < e.opts.WorkerCount; i++ {
		id := fmt.Sprintf("worker-%d", i+1)
		if _, err := e.pool.Add(ctx, id, e.opts.SlotsPerWorker); err != nil {
			return err
		}
	}

	if err := e.reconcile(ctx); err != nil {
		log.Printf("reconcile: %v", err)
	}

	e.wg.Add(5)
	go e.loop("dispatch", e.opts.DispatchInterval, e.dispatchTick)
	go e.loop("delay", e.opts.DelayScanInterval, e.delayTick)
	go e.loop("heartbeat", e.opts.HeartbeatInterval, e.heartbeatTick)
	go e.loop("worker-reaper", e.opts.HeartbeatInterval, e.reapWorkersTick)
	go e.loop("lease-reaper", e.opts.LeaseTTL/3+time.Second, e.reapLeasesTick)
	return nil
}

func (e *Engine) loop(name string, d time.Duration, fn func(context.Context)) {
	defer e.wg.Done()
	t := e.clk.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-t.C():
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("%s tick panic: %v", name, r)
					}
				}()
				fn(context.Background())
			}()
		}
	}
}

// Shutdown drains workers and stops loops.
func (e *Engine) Shutdown(ctx context.Context) {
	close(e.stopCh)
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	e.pool.Shutdown(ctx)
}
