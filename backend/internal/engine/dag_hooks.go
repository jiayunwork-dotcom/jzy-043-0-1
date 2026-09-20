package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"taskforge/internal/domain"
)

// submitDAGNodeTask is the callback the DAG orchestrator uses to enqueue a
// node task. Node tasks carry dag_id/dag_node so completion events can be
// routed back to the orchestrator.
func (e *Engine) submitDAGNodeTask(ctx context.Context, n *domain.DAGNode) (string, error) {
	t, _, err := e.Submit(ctx, SubmitInput{
		Type:           n.Def.TaskType,
		Payload:        n.Def.Payload,
		Priority:       n.Def.Priority,
		TimeoutSeconds: n.Def.Timeout.Std().Seconds(),
		MaxRetries:     0, // retry of a node is owned by the DAG policy
		RetryPolicy:    &domain.RetryPolicy{Kind: domain.RetryFixed, MaxRetries: 0},
	})
	if err != nil {
		return "", err
	}
	// Tag the persisted task with its DAG coordinates.
	if err := e.tagDAGTask(ctx, t.ID, n.DAGID, n.NodeID); err != nil {
		return "", err
	}
	return t.ID, nil
}

func (e *Engine) tagDAGTask(ctx context.Context, taskID, dagID, nodeID string) error {
	return e.repo.TagDAG(ctx, taskID, dagID, nodeID)
}

// afterDAGNode routes a terminal task event into the DAG orchestrator.
func (e *Engine) afterDAGNode(t *domain.Task, success bool, errMsg string) {
	if t.DAGID == "" || t.DAGNode == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if success {
		err = e.dags.NodeSucceeded(ctx, t.DAGID, t.DAGNode)
	} else {
		err = e.dags.NodeFailed(ctx, t.DAGID, t.DAGNode, errMsg)
	}
	if err != nil {
		// audit only; the task is already terminal.
		_ = e.repo.AddAudit(ctx, &domain.AuditEvent{
			TaskID: t.ID, DAGID: t.DAGID, Event: "dag-notify-failed",
			Detail: err.Error(), CreatedAt: e.clk.Now()})
	}
}

// callbackPayload is posted to the configured callback URL.
type callbackPayload struct {
	TaskID     string          `json:"task_id"`
	Type       string          `json:"type"`
	Priority   string          `json:"priority"`
	Success    bool            `json:"success"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
	Attempts   int             `json:"attempts"`
	FinishedAt time.Time       `json:"finished_at"`
}

// fireCallback is best-effort (logged on failure, never blocks scheduling).
func (e *Engine) fireCallback(t *domain.Task, success bool, result []byte, errMsg string) {
	if t.CallbackURL == "" {
		return
	}
	payload := callbackPayload{
		TaskID: t.ID, Type: t.Type, Priority: t.Priority.String(),
		Success: success, Result: result, Error: errMsg,
		Attempts: t.Attempt + 1, FinishedAt: e.clk.Now(),
	}
	body, _ := json.Marshal(payload)
	go func() {
		req, err := http.NewRequest(http.MethodPost, t.CallbackURL, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := e.httpClient.Do(req)
		if err != nil {
			return
		}
		_ = resp.Body.Close()
	}()
}
