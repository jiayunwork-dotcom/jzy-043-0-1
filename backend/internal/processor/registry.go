// Package processor holds the registry of task handlers. Multiple handlers
// can be registered for the same task type; dispatch round-robins between
// them for load balancing.
package processor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"taskforge/internal/domain"
)

// Handler executes one task. Returning an error marks the attempt failed;
// a typed error (implementing TypedError) also supplies the error_type used
// for dead-letter aggregation.
type Handler interface {
	Handle(ctx context.Context, t *domain.Task) ([]byte, error)
	Name() string
}

type HandlerFunc struct {
	Label string
	Fn    func(ctx context.Context, t *domain.Task) ([]byte, error)
}

func (h HandlerFunc) Handle(ctx context.Context, t *domain.Task) ([]byte, error) { return h.Fn(ctx, t) }
func (h HandlerFunc) Name() string                                               { return h.Label }

// TypedError lets handlers categorize failures for dead-letter stats.
type TypedError interface {
	error
	ErrorType() string
}

type typedErr struct {
	kind string
	err  error
}

func (e typedErr) Error() string     { return e.err.Error() }
func (e typedErr) ErrorType() string { return e.kind }
func (e typedErr) Unwrap() error     { return e.err }

func WrapError(kind string, err error) error {
	return typedErr{kind: kind, err: err}
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
	rrc      map[string]*uint64
}

func NewRegistry() *Registry {
	return &Registry{handlers: map[string][]Handler{}, rrc: map[string]*uint64{}}
}

// Register adds a handler for a task type (multiple allowed).
func (r *Registry) Register(taskType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = append(r.handlers[taskType], h)
	if _, ok := r.rrc[taskType]; !ok {
		var c uint64
		r.rrc[taskType] = &c
	}
}

func (r *Registry) RegisterFunc(taskType, name string, fn func(ctx context.Context, t *domain.Task) ([]byte, error)) {
	r.Register(taskType, HandlerFunc{Label: name, Fn: fn})
}

func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		out = append(out, k)
	}
	return out
}

func (r *Registry) Has(taskType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers[taskType]) > 0
}

// Get returns the next handler for the type using round-robin.
func (r *Registry) Get(taskType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hs := r.handlers[taskType]
	if len(hs) == 0 {
		return nil, fmt.Errorf("no handler registered for task type %q", taskType)
	}
	counter := r.rrc[taskType]
	idx := atomic.AddUint64(counter, 1) - 1
	return hs[idx%uint64(len(hs))], nil
}
