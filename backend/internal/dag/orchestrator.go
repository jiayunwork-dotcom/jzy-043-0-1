package dag

import (
	"context"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/store"
)

// SubmitFunc enqueues one node task and returns its task id.
type SubmitFunc func(ctx context.Context, n *domain.DAGNode) (string, error)

// Orchestrator owns DAG runtime transitions. It is driven by task lifecycle
// events from the engine (NodeSucceeded / NodeFailed).
type Orchestrator struct {
	repo   store.Repository
	clk    interface{ Now() time.Time }
	submit SubmitFunc
}

func NewOrchestrator(repo store.Repository, clk interface{ Now() time.Time }, submit SubmitFunc) *Orchestrator {
	return &Orchestrator{repo: repo, clk: clk, submit: submit}
}

// Instantiate validates a definition, persists a DAG and enqueues all root
// nodes (nodes without dependencies).
func (o *Orchestrator) Instantiate(ctx context.Context, def *domain.DAGDef) (*domain.DAG, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}
	now := o.clk.Now()
	d := &domain.DAG{
		ID:            newDAGID(),
		Name:          def.Name,
		Status:        domain.DAGRunning,
		FailurePolicy: policyOrDefault(def.FailurePolicy),
		CreatedAt:     now,
	}
	for _, nd := range def.Nodes {
		d.Nodes = append(d.Nodes, &domain.DAGNode{
			DAGID:     d.ID,
			NodeID:    nd.ID,
			State:     domain.NodePending,
			DependsOn: append([]string(nil), nd.DependsOn...),
			Def:       nd,
		})
	}
	if err := o.repo.CreateDAG(ctx, d); err != nil {
		return nil, err
	}
	// launch roots
	for _, n := range d.Nodes {
		if len(n.DependsOn) == 0 {
			if err := o.launch(ctx, n); err != nil {
				return nil, err
			}
		}
	}
	return d, o.refreshStatus(ctx, d.ID)
}

func policyOrDefault(p domain.NodeFailurePolicy) domain.NodeFailurePolicy {
	switch p {
	case domain.NodeFailAbort, domain.NodeFailSkip, domain.NodeFailRetry:
		return p
	}
	return domain.NodeFailAbort
}

func (o *Orchestrator) launch(ctx context.Context, n *domain.DAGNode) error {
	now := o.clk.Now()
	n.State = domain.NodeReady
	n.StartedAt = &now
	if err := o.repo.SaveNode(ctx, n); err != nil {
		return err
	}
	taskID, err := o.submit(ctx, n)
	if err != nil {
		return err
	}
	n.TaskID = taskID
	n.State = domain.NodeRunning
	return o.repo.SaveNode(ctx, n)
}

// NodeSucceeded marks a node done and unlocks nodes whose deps are now all
// satisfied (succeeded or skipped).
func (o *Orchestrator) NodeSucceeded(ctx context.Context, dagID, nodeID string) error {
	d, err := o.repo.GetDAG(ctx, dagID)
	if err != nil {
		return err
	}
	n := findNode(d, nodeID)
	if n == nil {
		return domain.ErrNotFound
	}
	now := o.clk.Now()
	n.State = domain.NodeSucceeded
	n.FinishedAt = &now
	n.Error = ""
	if err := o.repo.SaveNode(ctx, n); err != nil {
		return err
	}

	for _, cand := range d.Nodes {
		if cand.State != domain.NodePending {
			continue
		}
		if depsSatisfied(d, cand.DependsOn) {
			if err := o.launch(ctx, cand); err != nil {
				return err
			}
		}
	}
	return o.refreshStatus(ctx, dagID)
}

// NodeFailed applies the node's failure policy (retry/skip/abort).
func (o *Orchestrator) NodeFailed(ctx context.Context, dagID, nodeID, errMsg string) error {
	d, err := o.repo.GetDAG(ctx, dagID)
	if err != nil {
		return err
	}
	n := findNode(d, nodeID)
	if n == nil {
		return domain.ErrNotFound
	}
	policy := n.Def.OnFailure
	if policy == "" {
		policy = d.FailurePolicy
	}

	switch policy {
	case domain.NodeFailRetry:
		// Node-level retry: resubmit up to MaxRetries (default 1).
		max := n.Def.MaxRetries
		if max <= 0 {
			max = 1
		}
		if retriesUsed(n.Def) < max {
			bumpRetries(&n.Def)
			now := o.clk.Now()
			n.Error = errMsg
			n.State = domain.NodeReady
			n.StartedAt = &now
			if err := o.repo.SaveNode(ctx, n); err != nil {
				return err
			}
			taskID, err := o.submit(ctx, n)
			if err != nil {
				return err
			}
			n.TaskID = taskID
			n.State = domain.NodeRunning
			return o.repo.SaveNode(ctx, n)
		}
		return o.failNode(ctx, d, n, errMsg)
	case domain.NodeFailSkip:
		// mark this node failed+skipped, then unlock dependents treating
		// it as satisfied.
		return o.skipNode(ctx, d, n, errMsg)
	default:
		return o.failNode(ctx, d, n, errMsg)
	}
}

func (o *Orchestrator) failNode(ctx context.Context, d *domain.DAG, n *domain.DAGNode, errMsg string) error {
	now := o.clk.Now()
	n.State = domain.NodeFailed
	n.FinishedAt = &now
	n.Error = errMsg
	if err := o.repo.SaveNode(ctx, n); err != nil {
		return err
	}
	// abort: every still-pending node is skipped, the DAG is aborted.
	for _, other := range d.Nodes {
		if other.State == domain.NodePending || other.State == domain.NodeReady {
			other.State = domain.NodeSkipped
			other.Error = "aborted due to failure of " + n.NodeID
			f := now
			other.FinishedAt = &f
			if err := o.repo.SaveNode(ctx, other); err != nil {
				return err
			}
		}
	}
	return o.repo.SaveDAGStatus(ctx, d.ID, domain.DAGAborted, &now)
}

func (o *Orchestrator) skipNode(ctx context.Context, d *domain.DAG, n *domain.DAGNode, errMsg string) error {
	now := o.clk.Now()
	n.State = domain.NodeSkipped
	n.FinishedAt = &now
	n.Error = errMsg
	if err := o.repo.SaveNode(ctx, n); err != nil {
		return err
	}
	// unlock dependents (skipped counts as satisfied)
	for _, cand := range d.Nodes {
		if cand.State != domain.NodePending {
			continue
		}
		if depsSatisfied(d, cand.DependsOn) {
			if err := o.launch(ctx, cand); err != nil {
				return err
			}
		}
	}
	return o.refreshStatus(ctx, d.ID)
}

func depsSatisfied(d *domain.DAG, deps []string) bool {
	for _, dep := range deps {
		dn := findNode(d, dep)
		if dn == nil {
			return false
		}
		if dn.State != domain.NodeSucceeded && dn.State != domain.NodeSkipped {
			return false
		}
	}
	return true
}

// refreshStatus derives aggregate status from node states.
func (o *Orchestrator) refreshStatus(ctx context.Context, dagID string) error {
	d, err := o.repo.GetDAG(ctx, dagID)
	if err != nil {
		return err
	}
	if d.Status == domain.DAGAborted {
		return nil
	}
	allDone := true
	anyFail := false
	for _, n := range d.Nodes {
		switch n.State {
		case domain.NodeFailed:
			anyFail = true
		case domain.NodeSucceeded, domain.NodeSkipped:
		default:
			allDone = false
		}
	}
	if !allDone {
		return o.repo.SaveDAGStatus(ctx, dagID, domain.DAGRunning, nil)
	}
	now := o.clk.Now()
	if anyFail {
		return o.repo.SaveDAGStatus(ctx, dagID, domain.DAGFailed, &now)
	}
	return o.repo.SaveDAGStatus(ctx, dagID, domain.DAGSucceeded, &now)
}

func findNode(d *domain.DAG, id string) *domain.DAGNode {
	for _, n := range d.Nodes {
		if n.NodeID == id {
			return n
		}
	}
	return nil
}
