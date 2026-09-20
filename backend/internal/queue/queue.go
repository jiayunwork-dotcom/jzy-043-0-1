// Package queue abstracts the five ready queues, the delay zone and the
// inflight leases. Two implementations exist: redis (production) and
// memory (unit tests).
package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Claim is one task popped from a ready queue.
type Claim struct {
	TaskID   string
	Priority int
	Head     bool // true: re-queued after preemption
}

// Queue is the scheduling surface used by the engine.
type Queue interface {
	// EnqueueReady puts a task into a priority queue. head=true inserts it
	// ahead of normal traffic (used when a preempted task returns).
	EnqueueReady(ctx context.Context, taskID string, priority int, head bool) error
	// ReadyDepth returns the depth of the five ready queues.
	ReadyDepth(ctx context.Context) (map[int]int64, error)
	// ClaimOne atomically pops the highest-priority available task.
	// When forceLow is true (fairness window), Critical/High are skipped.
	ClaimOne(ctx context.Context, forceLow bool, now time.Time) (*Claim, error)

	// AddInflight records an active execution lease for a task.
	AddInflight(ctx context.Context, workerID, taskID string, priority int, leaseExpiresAt time.Time) error
	// RenewInflight extends the lease of a running task. Returns false if
	// the lease is gone (task was already reaped).
	RenewInflight(ctx context.Context, workerID, taskID string, leaseExpiresAt time.Time) (bool, error)
	// RemoveInflight drops a lease after completion.
	RemoveInflight(ctx context.Context, workerID, taskID string) error
	// ExpiredInflights lists (taskID, workerID) pairs whose leases expired.
	ExpiredInflights(ctx context.Context, now time.Time) ([]Inflight, error)
	// AllInflights returns every active lease (startup reconciliation).
	AllInflights(ctx context.Context) ([]Inflight, error)
	// RequeueInflight moves a lease back to its ready queue head.
	RequeueInflight(ctx context.Context, workerID, taskID string, priority int) error

	// AddDelay inserts a delayed task; PopDue returns everything due.
	AddDelay(ctx context.Context, taskID string, priority int, runAt time.Time) error
	RemoveDelay(ctx context.Context, taskID string) error
	PopDue(ctx context.Context, now time.Time) ([]Claim, error)
	DelaySize(ctx context.Context) (int64, error)

	// Reset clears all taskforge:* keys (single-instance startup reconciliation).
	Reset(ctx context.Context) error
	Ping(ctx context.Context) error
}

type Inflight struct {
	TaskID   string
	WorkerID string
	Priority int
}

// NewID returns a random identifier.
func NewID() string { return uuid.NewString() }
