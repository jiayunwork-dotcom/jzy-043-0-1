// Package store defines the persistence boundary and implements it with
// PostgreSQL (production) and an in-memory backend (unit tests).
// PostgreSQL is the source of truth; Redis only holds scheduling state.
package store

import (
	"context"
	"time"

	"taskforge/internal/domain"
)

// TaskFilter narrows task list queries.
type TaskFilter struct {
	States     []domain.TaskState
	Priorities []domain.Priority
	Types      []string
	From, To   *time.Time
	DAGID      string
	WorkerID   string
	Limit      int
	Offset     int
}

// ListResult is a generic page.
type TaskListResult struct {
	Tasks []*domain.Task
	Total int64
}

type DeadStat struct {
	ErrorType string `json:"error_type" db:"error_type"`
	Count     int64  `json:"count"`
}

// Repository is every persistence operation the engine and HTTP layer need.
type Repository interface {
	Ping(ctx context.Context) error
	Migrate(ctx context.Context) error

	CreateTask(ctx context.Context, t *domain.Task) (*domain.Task, error) // nil result => idempotency hit
	GetTask(ctx context.Context, id string) (*domain.Task, error)
	GetTasks(ctx context.Context, ids []string) ([]*domain.Task, error)
	ListTasks(ctx context.Context, f TaskFilter) (TaskListResult, error)

	// MarkReady transitions pending -> ready (delay promotion / retry).
	MarkReady(ctx context.Context, id string, now time.Time) error
	// StartAttempt creates an attempt and atomically transitions ready/dead?
	// to running for the chosen worker. Returns ErrConflict on a lost CAS.
	StartAttempt(ctx context.Context, taskID, workerID string, leaseExpiresAt, now time.Time) (*domain.Attempt, error)
	// RenewLease confirms the task is still running on that worker.
	RenewLease(ctx context.Context, taskID, workerID string, expiresAt time.Time) error

	// PreemptRunning transitions one running task of an eligible low
	// priority back to ready (at queue head) for a higher priority claim.
	// Task IDs in protect are exempt (fairness-window executions).
	PreemptRunning(ctx context.Context, workerID string, minPriority domain.Priority, protect []string, now time.Time) (*domain.Task, error)

	// CompleteAttempt finishes an attempt and transitions the task in one
	// transaction. nextState must be succeeded/dead.
	CompleteAttempt(ctx context.Context, att *domain.Attempt, next domain.TaskState, result []byte, errMsg, errType string, finishedAt time.Time) error
	// FailAttemptForRetry marks an attempt failed and moves the task back to
	// pending with its next run time, in one transaction.
	FailAttemptForRetry(ctx context.Context, att *domain.Attempt, errMsg, errType string, runAt time.Time) error
	// TagDAG links a task to a DAG node after instantiation.
	TagDAG(ctx context.Context, taskID, dagID, nodeID string) error
	// RequeueRunning moves a running task back to ready after a failure
	// that will be retried, or after worker/lease loss.
	RequeueRunning(ctx context.Context, taskID string, now time.Time, reason string) error
	// ScheduleRetry moves a failed task to pending with a next run time.
	ScheduleRetry(ctx context.Context, taskID string, runAt time.Time) error
	// RequeueDead puts dead tasks back to ready for an operator retry.
	RequeueDead(ctx context.Context, ids []string, now time.Time) (int64, error)
	// DiscardDead permanently deletes/flags dead tasks (operator action).
	DiscardDead(ctx context.Context, ids []string, now time.Time) (int64, error)

	ListAttempts(ctx context.Context, taskID string) ([]*domain.Attempt, error)

	// RecoverableTasks returns non-terminal tasks for startup reconciliation.
	RecoverableTasks(ctx context.Context) ([]*domain.Task, error)
	// ReapRunningOnWorker requeues running tasks of a lost worker and returns
	// the recovered tasks (so callers can re-enqueue them).
	ReapRunningOnWorker(ctx context.Context, workerID string, now time.Time, offline bool) ([]*domain.Task, error)
	MarkWorkersOfflineBefore(ctx context.Context, cutoff time.Time) ([]string, error)

	CancelTask(ctx context.Context, id string, now time.Time) error
	DeadStats(ctx context.Context) ([]DeadStat, error)
	CountDead(ctx context.Context) (int64, error)

	CreateWorker(ctx context.Context, w *domain.Worker) error
	HeartbeatWorker(ctx context.Context, id string, slots int, active int, now time.Time) error
	DrainWorker(ctx context.Context, id string, now time.Time) error
	ListWorkers(ctx context.Context) ([]*domain.Worker, error)
	WorkerStats(ctx context.Context) (online, draining, offline int64, err error)
	BumpWorkerCounters(ctx context.Context, id string, succeeded, failed, preempted int) error

	AddAudit(ctx context.Context, e *domain.AuditEvent) error
	ListAudit(ctx context.Context, taskID, dagID string, limit int) ([]*domain.AuditEvent, error)

	CreateDAG(ctx context.Context, d *domain.DAG) error
	GetDAG(ctx context.Context, id string) (*domain.DAG, error)
	ListDAGs(ctx context.Context, limit int) ([]*domain.DAG, error)
	SaveNode(ctx context.Context, n *domain.DAGNode) error
	SaveDAGStatus(ctx context.Context, id string, st domain.DAGStatus, finishedAt *time.Time) error
}
