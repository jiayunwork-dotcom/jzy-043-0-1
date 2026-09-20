// Package domain holds the core entities shared by every module:
// tasks, attempts, workers, DAGs and audit events.
package domain

import (
	"errors"
	"strconv"
	"time"
)

// Priority is one of the five scheduling levels. Lower numeric value =
// higher priority.
type Priority int

const (
	PriorityCritical Priority = 0
	PriorityHigh     Priority = 1
	PriorityNormal   Priority = 2
	PriorityLow      Priority = 3
	PriorityBulk     Priority = 4
)

func (p Priority) String() string {
	switch p {
	case PriorityCritical:
		return "critical"
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	case PriorityBulk:
		return "bulk"
	}
	return "unknown"
}

// ParsePriority accepts either the numeric form (0-4) or the textual one.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "0", "critical", "Critical", "CRITICAL":
		return PriorityCritical, nil
	case "1", "high", "High", "HIGH":
		return PriorityHigh, nil
	case "2", "normal", "Normal", "NORMAL", "":
		return PriorityNormal, nil
	case "3", "low", "Low", "LOW":
		return PriorityLow, nil
	case "4", "bulk", "Bulk", "BULK":
		return PriorityBulk, nil
	}
	return 0, errors.New("invalid priority: " + s)
}

func AllPriorities() []Priority {
	return []Priority{PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow, PriorityBulk}
}

// TaskState is the lifecycle state of a task.
type TaskState string

const (
	StatePending   TaskState = "pending" // in delay zone, not yet ready
	StateReady     TaskState = "ready"   // in a priority ready queue
	StateRunning   TaskState = "running" // claimed by a worker
	StateSucceeded TaskState = "succeeded"
	StateFailed    TaskState = "failed" // last attempt failed, retry scheduled (back in ready/pending)
	StateDead      TaskState = "dead"   // retries exhausted, in dead letter zone
	StateCanceled  TaskState = "canceled"
)

func (s TaskState) Terminal() bool {
	return s == StateSucceeded || s == StateDead || s == StateCanceled
}

type AttemptState string

const (
	AttemptRunning   AttemptState = "running"
	AttemptSucceeded AttemptState = "succeeded"
	AttemptFailed    AttemptState = "failed"
	AttemptPreempted AttemptState = "preempted"
	AttemptTimeout   AttemptState = "timeout"
)

// RetryKind selects the backoff strategy between attempts.
type RetryKind string

const (
	RetryExponential RetryKind = "exponential"
	RetryFixed       RetryKind = "fixed"
	RetryCron        RetryKind = "cron"
)

// RetryPolicy describes how a task is retried.
//   - exponential: delay = BaseInterval * 2^attempt
//   - fixed:       delay = BaseInterval
//   - cron:        next run is computed from the Cron expression
type RetryPolicy struct {
	Kind         RetryKind `json:"kind"`
	BaseInterval Duration  `json:"base_interval"`
	Cron         string    `json:"cron,omitempty"`
	MaxRetries   int       `json:"max_retries"`
}

// Duration is time.Duration that (un)marshals as seconds in JSON.
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(jsonNumber(float64(time.Duration(d).Seconds()))), nil
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	secs, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return err
	}
	*d = Duration(time.Duration(secs * float64(time.Second)))
	return nil
}

func jsonNumber(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

type Task struct {
	ID             string      `json:"id"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Type           string      `json:"type"`
	Payload        []byte      `json:"payload"`
	Priority       Priority    `json:"priority"`
	State          TaskState   `json:"state"`
	RunAt          *time.Time  `json:"run_at,omitempty"` // earliest start time (delay)
	Timeout        Duration    `json:"timeout"`
	RetryPolicy    RetryPolicy `json:"retry_policy"`
	Attempt        int         `json:"attempt"` // zero-based attempt counter
	MaxRetries     int         `json:"max_retries"`
	CallbackURL    string      `json:"callback_url,omitempty"`
	Result         []byte      `json:"result,omitempty"`
	LastError      string      `json:"last_error,omitempty"`
	ErrorType      string      `json:"error_type,omitempty"`
	WorkerID       string      `json:"worker_id,omitempty"`
	DAGID          string      `json:"dag_id,omitempty"`
	DAGNode        string      `json:"dag_node,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	ReadyAt        time.Time   `json:"ready_at"` // when it entered the ready queue
	StartedAt      *time.Time  `json:"started_at,omitempty"`
	FinishedAt     *time.Time  `json:"finished_at,omitempty"`
}

type Attempt struct {
	ID           string       `json:"id"`
	TaskID       string       `json:"task_id"`
	AttemptNo    int          `json:"attempt_no"`
	WorkerID     string       `json:"worker_id"`
	State        AttemptState `json:"state"`
	StartedAt    time.Time    `json:"started_at"`
	EndedAt      *time.Time   `json:"ended_at,omitempty"`
	ErrorType    string       `json:"error_type,omitempty"`
	ErrorMessage string       `json:"error_message,omitempty"`
	Result       []byte       `json:"result,omitempty"`
}

type WorkerStatus string

const (
	WorkerOnline   WorkerStatus = "online"
	WorkerDraining WorkerStatus = "draining"
	WorkerOffline  WorkerStatus = "offline"
)

type Worker struct {
	ID            string       `json:"id"`
	TotalSlots    int          `json:"total_slots"`
	ActiveSlots   int          `json:"active_slots"`
	Status        WorkerStatus `json:"status"`
	LastHeartbeat time.Time    `json:"last_heartbeat"`
	StartedAt     time.Time    `json:"started_at"`
	Succeeded     int64        `json:"succeeded"`
	Failed        int64        `json:"failed"`
	Preempted     int64        `json:"preempted"`
}

type DAGStatus string

const (
	DAGPending   DAGStatus = "pending"
	DAGRunning   DAGStatus = "running"
	DAGSucceeded DAGStatus = "succeeded"
	DAGFailed    DAGStatus = "failed"
	DAGAborted   DAGStatus = "aborted"
)

type NodeFailurePolicy string

const (
	NodeFailAbort NodeFailurePolicy = "abort"
	NodeFailSkip  NodeFailurePolicy = "skip"
	NodeFailRetry NodeFailurePolicy = "retry"
)

// DAGNodeDef is the definition of one node in a submitted DAG template.
type DAGNodeDef struct {
	ID          string            `json:"id"`
	TaskType    string            `json:"task_type"`
	Payload     []byte            `json:"payload"`
	Priority    Priority          `json:"priority"`
	Timeout     Duration          `json:"timeout"`
	DependsOn   []string          `json:"depends_on"`
	OnFailure   NodeFailurePolicy `json:"on_failure"`
	MaxRetries  int               `json:"max_retries"`
	RetriesUsed int               `json:"retries_used"`
}

type DAGDef struct {
	Name          string            `json:"name"`
	Nodes         []DAGNodeDef      `json:"nodes"`
	FailurePolicy NodeFailurePolicy `json:"failure_policy"`
}

type NodeState string

const (
	NodePending   NodeState = "pending"
	NodeReady     NodeState = "ready"
	NodeRunning   NodeState = "running"
	NodeSucceeded NodeState = "succeeded"
	NodeFailed    NodeState = "failed"
	NodeSkipped   NodeState = "skipped"
)

type DAGNode struct {
	DAGID      string     `json:"dag_id"`
	NodeID     string     `json:"node_id"`
	TaskID     string     `json:"task_id,omitempty"`
	State      NodeState  `json:"state"`
	DependsOn  []string   `json:"depends_on"`
	Def        DAGNodeDef `json:"def"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type DAG struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Status        DAGStatus         `json:"status"`
	FailurePolicy NodeFailurePolicy `json:"failure_policy"`
	CreatedAt     time.Time         `json:"created_at"`
	FinishedAt    *time.Time        `json:"finished_at,omitempty"`
	Nodes         []*DAGNode        `json:"nodes"`
}

type AuditEvent struct {
	ID        int64     `json:"id"`
	TaskID    string    `json:"task_id,omitempty"`
	DAGID     string    `json:"dag_id,omitempty"`
	WorkerID  string    `json:"worker_id,omitempty"`
	Event     string    `json:"event"`
	FromState string    `json:"from_state,omitempty"`
	ToState   string    `json:"to_state,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Sentinel errors used across modules.
var (
	ErrNotFound      = errors.New("not found")
	ErrConflict      = errors.New("state conflict")
	ErrAlreadyQueued = errors.New("task already queued")
	ErrCycle         = errors.New("dag contains a cycle")
)
