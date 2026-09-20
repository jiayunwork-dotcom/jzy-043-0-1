// Package httpapi exposes the REST + SSE API consumed by the admin panel.
package httpapi

import (
	"encoding/json"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/engine"
	"taskforge/internal/store"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// Server holds dependencies for the HTTP handlers.
type Server struct {
	eng  *engine.Engine
	repo store.Repository
}

func New(eng *engine.Engine, repo store.Repository) *Server {
	return &Server{eng: eng, repo: repo}
}

// ---- DTOs ----------------------------------------------------------------

type submitReq struct {
	Type           string          `json:"type"`
	Payload        map[string]any  `json:"payload"`
	Priority       string          `json:"priority"`
	DelaySeconds   float64         `json:"delay_seconds"`
	TimeoutSeconds float64         `json:"timeout_seconds"`
	MaxRetries     int             `json:"max_retries"`
	RetryPolicy    *retryPolicyReq `json:"retry_policy"`
	CallbackURL    string          `json:"callback_url"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type retryPolicyReq struct {
	Kind        string  `json:"kind"`
	BaseSeconds float64 `json:"base_interval_seconds"`
	Cron        string  `json:"cron"`
	MaxRetries  int     `json:"max_retries"`
}

type taskResp struct {
	ID             string         `json:"id"`
	Type           string         `json:"type"`
	Payload        any            `json:"payload"`
	Priority       string         `json:"priority"`
	State          string         `json:"state"`
	Attempt        int            `json:"attempt"`
	MaxRetries     int            `json:"max_retries"`
	CallbackURL    string         `json:"callback_url,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	ErrorType      string         `json:"error_type,omitempty"`
	WorkerID       string         `json:"worker_id,omitempty"`
	DAGID          string         `json:"dag_id,omitempty"`
	DAGNode        string         `json:"dag_node,omitempty"`
	RunAt          *time.Time     `json:"run_at,omitempty"`
	ReadyAt        time.Time      `json:"ready_at"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	TimeoutSeconds float64        `json:"timeout_seconds"`
	RetryPolicy    map[string]any `json:"retry_policy"`
}

func toTaskResp(t *domain.Task) taskResp {
	var payload any
	if len(t.Payload) > 0 {
		_ = jsonUnmarshal(t.Payload, &payload)
	}
	return taskResp{
		ID: t.ID, Type: t.Type, Payload: payload,
		Priority: t.Priority.String(), State: string(t.State),
		Attempt: t.Attempt, MaxRetries: t.MaxRetries,
		CallbackURL: t.CallbackURL, LastError: t.LastError,
		ErrorType: t.ErrorType, WorkerID: t.WorkerID,
		DAGID: t.DAGID, DAGNode: t.DAGNode,
		RunAt: t.RunAt, ReadyAt: t.ReadyAt, StartedAt: t.StartedAt,
		FinishedAt: t.FinishedAt, CreatedAt: t.CreatedAt,
		TimeoutSeconds: t.Timeout.Std().Seconds(),
		RetryPolicy: map[string]any{
			"kind":                  string(t.RetryPolicy.Kind),
			"base_interval_seconds": t.RetryPolicy.BaseInterval.Std().Seconds(),
			"cron":                  t.RetryPolicy.Cron,
			"max_retries":           t.RetryPolicy.MaxRetries,
		},
	}
}

type attemptResp struct {
	ID           string     `json:"id"`
	AttemptNo    int        `json:"attempt_no"`
	WorkerID     string     `json:"worker_id"`
	State        string     `json:"state"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	ErrorType    string     `json:"error_type,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

func toAttemptResp(a *domain.Attempt) attemptResp {
	return attemptResp{
		ID: a.ID, AttemptNo: a.AttemptNo, WorkerID: a.WorkerID,
		State: string(a.State), StartedAt: a.StartedAt, EndedAt: a.EndedAt,
		ErrorType: a.ErrorType, ErrorMessage: a.ErrorMessage,
	}
}
