// Package workerpool manages in-process workers: slots, heartbeats, lease
// renewal, preemption signals and graceful drain.
package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"taskforge/internal/clock"
	"taskforge/internal/domain"
	"taskforge/internal/store"
)

// Exec is one running execution controlled by a slot.
type Exec struct {
	TaskID   string
	WorkerID string
	// Ctx is canceled on per-task timeout, preemption or release.
	Ctx    context.Context
	cancel context.CancelFunc
	// released is closed once when the slot is freed.
	released chan struct{}
	once     sync.Once
	detached atomic.Bool
}

// Released is closed after the execution frees its slot.
func (e *Exec) Released() <-chan struct{} { return e.released }

// Worker is one pool member.
type Worker struct {
	ID         string
	TotalSlots int

	mu           sync.Mutex
	execs        map[string]*Exec
	reservations []*Reservation
	online       bool
	draining     bool
}

func (w *Worker) activeCount() int { return len(w.execs) }

// Reservation is a slot claimed before a task is popped from the queue.
type Reservation struct {
	w        *Worker
	exec     *Exec
	used     bool
	released bool
	mu       sync.Mutex
	done     chan struct{}
}

// Confirm converts the reservation into a running execution for taskID.
func (r *Reservation) Confirm(taskID string, timeout time.Duration) *Exec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used || r.released {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	exec := &Exec{TaskID: taskID, WorkerID: r.w.ID, Ctx: ctx, cancel: cancel,
		released: make(chan struct{})}
	r.exec = exec
	r.used = true

	w := r.w
	w.mu.Lock()
	w.execs[taskID] = exec
	for i, rr := range w.reservations {
		if rr == r {
			w.reservations = append(w.reservations[:i], w.reservations[i+1:]...)
			break
		}
	}
	w.mu.Unlock()
	return exec
}

// Release frees a reservation that will not be used (queue miss / lost CAS).
func (r *Reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	if r.used {
		return
	}
	w := r.w
	w.mu.Lock()
	for i, rr := range w.reservations {
		if rr == r {
			w.reservations = append(w.reservations[:i], w.reservations[i+1:]...)
			break
		}
	}
	w.mu.Unlock()
	close(r.done)
}

// Pool owns the workers.
type Pool struct {
	clk   clock.Clock
	repo  store.Repository
	mu    sync.RWMutex
	items map[string]*Worker
}

func New(clk clock.Clock, repo store.Repository) *Pool {
	return &Pool{clk: clk, repo: repo, items: map[string]*Worker{}}
}

// Add creates and persists a new worker.
func (p *Pool) Add(ctx context.Context, id string, slots int) (*Worker, error) {
	now := p.clk.Now()
	if err := p.repo.CreateWorker(ctx, &domain.Worker{
		ID:            id,
		TotalSlots:    slots,
		Status:        domain.WorkerOnline,
		LastHeartbeat: now,
		StartedAt:     now,
	}); err != nil {
		return nil, err
	}
	w := &Worker{ID: id, TotalSlots: slots, execs: map[string]*Exec{}, online: true}
	p.mu.Lock()
	p.items[id] = w
	p.mu.Unlock()
	return w, nil
}

func (p *Pool) Get(id string) *Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.items[id]
}

func (p *Pool) List() []*Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Worker, 0, len(p.items))
	for _, w := range p.items {
		out = append(out, w)
	}
	return out
}

// Reserve atomically claims a free slot on the worker and returns a token.
// The slot is held until Confirm(taskID, timeout) or ReleaseReservation.
// This guarantees a task popped from the queue always has a slot to run in.
func (p *Pool) Reserve(w *Worker) (*Reservation, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.online || w.draining || len(w.execs)+len(w.reservations) >= w.TotalSlots {
		return nil, false
	}
	r := &Reservation{w: w, done: make(chan struct{})}
	w.reservations = append(w.reservations, r)
	return r, true
}

// FreeWorker returns one online worker with a free slot and its remaining
// capacity, or nil when everything is saturated. Workers with fewer running
// tasks are preferred (spread load across the cluster).
func (p *Pool) FreeWorker() (*Worker, int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var best *Worker
	bestFree := 0
	for _, w := range p.items {
		w.mu.Lock()
		if !w.online || w.draining {
			w.mu.Unlock()
			continue
		}
		free := w.TotalSlots - len(w.execs) - len(w.reservations)
		if free > 0 && (best == nil || free > bestFree) {
			best = w
			bestFree = free
		}
		w.mu.Unlock()
	}
	return best, bestFree
}

// PreemptTarget returns a worker running an interruptible task.
func (p *Pool) PreemptTarget() *Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, w := range p.items {
		w.mu.Lock()
		interruptible := w.online && !w.draining && len(w.execs) > 0
		w.mu.Unlock()
		if interruptible {
			return w
		}
	}
	return nil
}

// Acquire binds an execution to a slot. The returned Exec carries a context
// that is canceled on per-task timeout, preemption or release.
func (p *Pool) Acquire(w *Worker, taskID string, timeout time.Duration) (*Exec, bool) {
	w.mu.Lock()
	if !w.online || w.draining || len(w.execs) >= w.TotalSlots {
		w.mu.Unlock()
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	e := &Exec{
		TaskID:   taskID,
		WorkerID: w.ID,
		Ctx:      ctx,
		cancel:   cancel,
		released: make(chan struct{}),
	}
	w.execs[taskID] = e
	w.mu.Unlock()
	return e, true
}

// Release frees a slot. Safe to call once; a no-op if the execution was
// already detached by preemption.
func (p *Pool) Release(w *Worker, taskID string) {
	w.mu.Lock()
	e, ok := w.execs[taskID]
	if ok {
		delete(w.execs, taskID)
	}
	w.mu.Unlock()
	if !ok {
		return
	}
	e.once.Do(func() {
		e.cancel()
		close(e.released)
	})
}

// Preempt detaches the task's execution from its slot immediately, freeing
// the slot for an urgent task. The detached execution goroutine continues to
// wind down (its context is canceled); its eventual Release is a no-op, so it
// can never cancel the new occupant of the slot.
func (p *Pool) Preempt(w *Worker, taskID string) bool {
	w.mu.Lock()
	e, ok := w.execs[taskID]
	if !ok {
		w.mu.Unlock()
		return false
	}
	delete(w.execs, taskID)
	w.mu.Unlock()
	e.detached.Store(true)
	e.cancel()
	e.once.Do(func() { close(e.released) })
	return true
}

// ActiveSlots reports live slot usage cluster-wide.
func (p *Pool) ActiveSlots() (active, total int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, w := range p.items {
		w.mu.Lock()
		active += len(w.execs)
		total += w.TotalSlots
		w.mu.Unlock()
	}
	return
}

// Drain marks a worker draining; it refuses new tasks.
func (p *Pool) Drain(id string) bool {
	p.mu.RLock()
	w := p.items[id]
	p.mu.RUnlock()
	if w == nil {
		return false
	}
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()
	return true
}

// MarkOffline stops a (lost) worker from receiving any further dispatch.
// Its executions are reaped separately by the reaper.
func (p *Pool) MarkOffline(id string) {
	p.mu.RLock()
	w := p.items[id]
	p.mu.RUnlock()
	if w == nil {
		return
	}
	w.mu.Lock()
	w.online = false
	w.mu.Unlock()
}

// ActiveCount reports running tasks of one worker.
func (p *Pool) ActiveCount(w *Worker) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.execs)
}

// WaitDrained blocks until the worker has no running executions.
func (p *Pool) WaitDrained(ctx context.Context, w *Worker) error {
	for {
		w.mu.Lock()
		execs := make([]*Exec, 0, len(w.execs))
		for _, e := range w.execs {
			execs = append(execs, e)
		}
		w.mu.Unlock()
		if len(execs) == 0 {
			return nil
		}
		ch := execs[0].Released()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// Shutdown drains every worker and cancels in-flight executions.
func (p *Pool) Shutdown(ctx context.Context) {
	for _, w := range p.List() {
		w.mu.Lock()
		w.draining = true
		execs := make([]*Exec, 0, len(w.execs))
		for _, e := range w.execs {
			execs = append(execs, e)
		}
		w.mu.Unlock()
		_ = p.WaitDrained(ctx, w)
		for _, e := range execs {
			e.cancel()
		}
	}
}
