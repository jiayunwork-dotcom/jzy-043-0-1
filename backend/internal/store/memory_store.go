package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"taskforge/internal/domain"
)

// MemStore is an in-memory Repository mirroring PostgreSQL CAS semantics.
// It lets engine unit tests run without external dependencies.
type MemStore struct {
	mu       sync.Mutex
	tasks    map[string]*domain.Task
	attempts map[string][]*domain.Attempt
	workers  map[string]*domain.Worker
	dags     map[string]*domain.DAG
	audit    []*domain.AuditEvent
	auditID  int64
}

func NewMem() *MemStore {
	return &MemStore{
		tasks:    map[string]*domain.Task{},
		attempts: map[string][]*domain.Attempt{},
		workers:  map[string]*domain.Worker{},
		dags:     map[string]*domain.DAG{},
	}
}

func (m *MemStore) Ping(context.Context) error    { return nil }
func (m *MemStore) Migrate(context.Context) error { return nil }

func cloneTask(t *domain.Task) *domain.Task {
	c := *t
	if t.RunAt != nil {
		v := *t.RunAt
		c.RunAt = &v
	}
	if t.StartedAt != nil {
		v := *t.StartedAt
		c.StartedAt = &v
	}
	if t.FinishedAt != nil {
		v := *t.FinishedAt
		c.FinishedAt = &v
	}
	c.Payload = append([]byte(nil), t.Payload...)
	c.Result = append([]byte(nil), t.Result...)
	return &c
}

func (m *MemStore) CreateTask(_ context.Context, t *domain.Task) (*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.IdempotencyKey != "" {
		for _, ex := range m.tasks {
			if ex.IdempotencyKey == t.IdempotencyKey {
				return cloneTask(ex), nil
			}
		}
	}
	if _, exists := m.tasks[t.ID]; exists {
		return nil, domain.ErrConflict
	}
	m.tasks[t.ID] = cloneTask(t)
	m.auditLocked(t.ID, "", "", "create", "", string(t.State), "")
	return nil, nil
}

func (m *MemStore) auditLocked(taskID, dagID, workerID, event, from, to, detail string) {
	m.auditID++
	m.audit = append(m.audit, &domain.AuditEvent{
		ID: m.auditID, TaskID: taskID, DAGID: dagID, WorkerID: workerID, Event: event,
		FromState: from, ToState: to, Detail: detail, CreatedAt: time.Now(),
	})
}

func (m *MemStore) GetTask(_ context.Context, id string) (*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return cloneTask(t), nil
}

func (m *MemStore) GetTasks(_ context.Context, ids []string) ([]*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*domain.Task
	for _, id := range ids {
		if t, ok := m.tasks[id]; ok {
			out = append(out, cloneTask(t))
		}
	}
	return out, nil
}

func (m *MemStore) ListTasks(_ context.Context, f TaskFilter) (TaskListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []*domain.Task
	for _, t := range m.tasks {
		if !matchFilter(t, f) {
			continue
		}
		all = append(all, cloneTask(t))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CreatedAt.After(all[j].CreatedAt) })
	total := int64(len(all))
	start := f.Offset
	if start > len(all) {
		start = len(all)
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	return TaskListResult{Tasks: all[start:end], Total: total}, nil
}

func matchFilter(t *domain.Task, f TaskFilter) bool {
	if len(f.States) > 0 {
		ok := false
		for _, s := range f.States {
			if t.State == s {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	if len(f.Priorities) > 0 {
		ok := false
		for _, p := range f.Priorities {
			if t.Priority == p {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	if len(f.Types) > 0 {
		ok := false
		for _, ty := range f.Types {
			if t.Type == ty {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	if f.From != nil && t.CreatedAt.Before(*f.From) {
		return false
	}
	if f.To != nil && t.CreatedAt.After(*f.To) {
		return false
	}
	if f.DAGID != "" && t.DAGID != f.DAGID {
		return false
	}
	if f.WorkerID != "" && t.WorkerID != f.WorkerID {
		return false
	}
	return true
}

func (m *MemStore) MarkReady(_ context.Context, id string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return domain.ErrNotFound
	}
	if t.State != domain.StatePending {
		return domain.ErrConflict
	}
	t.State = domain.StateReady
	t.ReadyAt = now
	t.RunAt = nil
	t.WorkerID = ""
	t.UpdatedAt = now
	m.auditLocked(id, "", "", "ready", "pending", "ready", "")
	return nil
}

func (m *MemStore) StartAttempt(_ context.Context, taskID, workerID string, _ time.Time, now time.Time) (*domain.Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if t.State != domain.StateReady {
		return nil, domain.ErrConflict
	}
	attemptNo := t.Attempt
	t.Attempt++
	t.State = domain.StateRunning
	t.WorkerID = workerID
	t.UpdatedAt = now
	if t.StartedAt == nil {
		s := now
		t.StartedAt = &s
	}
	att := &domain.Attempt{
		ID: newID(), TaskID: taskID, AttemptNo: attemptNo,
		WorkerID: workerID, State: domain.AttemptRunning, StartedAt: now,
	}
	// Store an independent copy: the caller mutates the returned pointer
	// (att.State) while the store may concurrently serve reads.
	stored := *att
	m.attempts[taskID] = append(m.attempts[taskID], &stored)
	m.auditLocked(taskID, "", workerID, "start", "ready", "running", "")
	if w, ok := m.workers[workerID]; ok {
		w.ActiveSlots++
	}
	return att, nil
}

func (m *MemStore) RenewLease(_ context.Context, taskID, workerID string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok || t.State != domain.StateRunning || t.WorkerID != workerID {
		return domain.ErrConflict
	}
	return nil
}

func (m *MemStore) PreemptRunning(_ context.Context, workerID string, minPriority domain.Priority, protect []string, now time.Time) (*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	protected := map[string]bool{}
	for _, id := range protect {
		protected[id] = true
	}
	var best *domain.Task
	for _, t := range m.tasks {
		if t.State != domain.StateRunning || t.WorkerID != workerID || t.Priority < minPriority {
			continue
		}
		if protected[t.ID] {
			continue
		}
		if best == nil || t.Priority > best.Priority ||
			(t.Priority == best.Priority && t.UpdatedAt.Before(best.UpdatedAt)) {
			best = t
		}
	}
	if best == nil {
		return nil, domain.ErrNotFound
	}
	best.State = domain.StateReady
	best.ReadyAt = now
	best.WorkerID = ""
	best.UpdatedAt = now
	for _, a := range m.attempts[best.ID] {
		if a.State == domain.AttemptRunning && a.WorkerID == workerID {
			a.State = domain.AttemptPreempted
			t := now
			a.EndedAt = &t
		}
	}
	if w, ok := m.workers[workerID]; ok {
		if w.ActiveSlots > 0 {
			w.ActiveSlots--
		}
	}
	m.auditLocked(best.ID, "", workerID, "preempt", "running", "ready", "")
	return cloneTask(best), nil
}

func (m *MemStore) CompleteAttempt(_ context.Context, att *domain.Attempt,
	next domain.TaskState, result []byte, errMsg, errType string, finishedAt time.Time) error {

	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[att.TaskID]
	if !ok {
		return domain.ErrNotFound
	}
	if t.State != domain.StateRunning {
		return domain.ErrConflict
	}
	t.State = next
	t.Result = append([]byte(nil), result...)
	t.LastError = errMsg
	t.ErrorType = errType
	t.UpdatedAt = finishedAt
	if next == domain.StateSucceeded || next == domain.StateDead {
		f := finishedAt
		t.FinishedAt = &f
	}
	for _, a := range m.attempts[att.TaskID] {
		if a.ID == att.ID {
			a.State = att.State
			e := finishedAt
			a.EndedAt = &e
			a.ErrorType = errType
			a.ErrorMessage = errMsg
			a.Result = append([]byte(nil), result...)
		}
	}
	if w, ok := m.workers[att.WorkerID]; ok {
		if w.ActiveSlots > 0 {
			w.ActiveSlots--
		}
	}
	m.auditLocked(att.TaskID, "", att.WorkerID, "complete:"+string(next),
		"running", string(next), errMsg)
	return nil
}

func (m *MemStore) RequeueRunning(_ context.Context, taskID string, now time.Time, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return domain.ErrNotFound
	}
	if t.State != domain.StateRunning {
		return domain.ErrConflict
	}
	worker := t.WorkerID
	t.State = domain.StateReady
	t.ReadyAt = now
	t.WorkerID = ""
	t.UpdatedAt = now
	for _, a := range m.attempts[taskID] {
		if a.State == domain.AttemptRunning {
			a.State = domain.AttemptTimeout
			e := now
			a.EndedAt = &e
			a.ErrorMessage = reason
		}
	}
	if w, ok := m.workers[worker]; ok && w.ActiveSlots > 0 {
		w.ActiveSlots--
	}
	m.auditLocked(taskID, "", worker, "requeue", "running", "ready", reason)
	return nil
}

func (m *MemStore) FailAttemptForRetry(_ context.Context, att *domain.Attempt,
	errMsg, errType string, runAt time.Time) error {

	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[att.TaskID]
	if !ok {
		return domain.ErrNotFound
	}
	if t.State != domain.StateRunning {
		return domain.ErrConflict
	}
	worker := t.WorkerID
	t.State = domain.StatePending
	t.RunAt = &runAt
	t.WorkerID = ""
	t.LastError = errMsg
	t.ErrorType = errType
	t.UpdatedAt = runAt
	for _, a := range m.attempts[att.TaskID] {
		if a.ID == att.ID {
			a.State = domain.AttemptFailed
			e := runAt
			a.EndedAt = &e
			a.ErrorType = errType
			a.ErrorMessage = errMsg
		}
	}
	if w, ok := m.workers[worker]; ok && w.ActiveSlots > 0 {
		w.ActiveSlots--
	}
	m.auditLocked(att.TaskID, "", worker, "retry-scheduled", "running", "pending", runAt.String())
	return nil
}

func (m *MemStore) TagDAG(_ context.Context, taskID, dagID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return domain.ErrNotFound
	}
	t.DAGID = dagID
	t.DAGNode = nodeID
	return nil
}

func (m *MemStore) RequeueDead(_ context.Context, ids []string, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, id := range ids {
		t, ok := m.tasks[id]
		if !ok || t.State != domain.StateDead {
			continue
		}
		t.State = domain.StateReady
		t.ReadyAt = now
		t.FinishedAt = nil
		t.LastError = ""
		t.ErrorType = ""
		t.UpdatedAt = now
		n++
		m.auditLocked(id, "", "", "dead-retry-batch", "dead", "ready", "")
	}
	return n, nil
}

func (m *MemStore) DiscardDead(_ context.Context, ids []string, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, id := range ids {
		t, ok := m.tasks[id]
		if !ok || t.State != domain.StateDead {
			continue
		}
		t.State = domain.StateCanceled
		f := now
		t.FinishedAt = &f
		t.UpdatedAt = now
		n++
		m.auditLocked(id, "", "", "dead-discard-batch", "dead", "canceled", "")
	}
	return n, nil
}

func (m *MemStore) ScheduleRetry(_ context.Context, taskID string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return domain.ErrNotFound
	}
	if t.State != domain.StateRunning {
		return domain.ErrConflict
	}
	worker := t.WorkerID
	t.State = domain.StatePending
	t.RunAt = &runAt
	t.WorkerID = ""
	t.UpdatedAt = runAt
	if w, ok := m.workers[worker]; ok && w.ActiveSlots > 0 {
		w.ActiveSlots--
	}
	m.auditLocked(taskID, "", worker, "retry-scheduled", "running", "pending", runAt.String())
	return nil
}

func (m *MemStore) ListAttempts(_ context.Context, taskID string) ([]*domain.Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.attempts[taskID]
	out := make([]*domain.Attempt, len(src))
	for i, a := range src {
		c := *a
		out[i] = &c
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AttemptNo < out[j].AttemptNo })
	return out, nil
}

func (m *MemStore) RecoverableTasks(_ context.Context) ([]*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*domain.Task
	for _, t := range m.tasks {
		if t.State == domain.StateReady || t.State == domain.StatePending || t.State == domain.StateRunning {
			out = append(out, cloneTask(t))
		}
	}
	return out, nil
}

func (m *MemStore) ReapRunningOnWorker(_ context.Context, workerID string, now time.Time, offline bool) ([]*domain.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var recovered []*domain.Task
	for _, t := range m.tasks {
		if t.State == domain.StateRunning && t.WorkerID == workerID {
			t.State = domain.StateReady
			t.ReadyAt = now
			t.WorkerID = ""
			t.UpdatedAt = now
			recovered = append(recovered, cloneTask(t))
			m.auditLocked(t.ID, "", workerID, "requeue", "running", "ready", "worker lost")
		}
	}
	if w, ok := m.workers[workerID]; ok {
		w.ActiveSlots = 0
		if offline {
			w.Status = domain.WorkerOffline
		}
	}
	return recovered, nil
}

func (m *MemStore) MarkWorkersOfflineBefore(_ context.Context, cutoff time.Time) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for _, w := range m.workers {
		if w.Status != domain.WorkerOffline && w.LastHeartbeat.Before(cutoff) {
			w.Status = domain.WorkerOffline
			w.ActiveSlots = 0
			ids = append(ids, w.ID)
		}
	}
	return ids, nil
}

func (m *MemStore) CancelTask(_ context.Context, id string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return domain.ErrNotFound
	}
	if t.State != domain.StateReady && t.State != domain.StatePending {
		return domain.ErrConflict
	}
	t.State = domain.StateCanceled
	f := now
	t.FinishedAt = &f
	t.UpdatedAt = now
	m.auditLocked(id, "", "", "cancel", "", "canceled", "")
	return nil
}

func (m *MemStore) DeadStats(_ context.Context) ([]DeadStat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := map[string]int64{}
	for _, t := range m.tasks {
		if t.State == domain.StateDead {
			et := t.ErrorType
			if et == "" {
				et = "unknown"
			}
			counts[et]++
		}
	}
	var out []DeadStat
	for k, v := range counts {
		out = append(out, DeadStat{ErrorType: k, Count: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out, nil
}

func (m *MemStore) CountDead(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, t := range m.tasks {
		if t.State == domain.StateDead {
			n++
		}
	}
	return n, nil
}

func (m *MemStore) CreateWorker(_ context.Context, w *domain.Worker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ex, ok := m.workers[w.ID]; ok {
		ex.Status = domain.WorkerOnline
		ex.TotalSlots = w.TotalSlots
		ex.LastHeartbeat = w.LastHeartbeat
		return nil
	}
	c := *w
	m.workers[w.ID] = &c
	return nil
}

func (m *MemStore) HeartbeatWorker(_ context.Context, id string, slots, active int, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workers[id]
	if !ok {
		return domain.ErrNotFound
	}
	w.LastHeartbeat = now
	w.TotalSlots = slots
	w.ActiveSlots = active
	if w.Status == domain.WorkerOffline {
		w.Status = domain.WorkerOnline
	}
	return nil
}

func (m *MemStore) DrainWorker(_ context.Context, id string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workers[id]
	if !ok || w.Status != domain.WorkerOnline {
		return domain.ErrConflict
	}
	w.Status = domain.WorkerDraining
	m.auditLocked("", "", id, "drain", "", "draining", "")
	return nil
}

func (m *MemStore) ListWorkers(_ context.Context) ([]*domain.Worker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*domain.Worker
	for _, w := range m.workers {
		c := *w
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

func (m *MemStore) WorkerStats(_ context.Context) (int64, int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var on, dr, off int64
	for _, w := range m.workers {
		switch w.Status {
		case domain.WorkerOnline:
			on++
		case domain.WorkerDraining:
			dr++
		case domain.WorkerOffline:
			off++
		}
	}
	return on, dr, off, nil
}

func (m *MemStore) BumpWorkerCounters(_ context.Context, id string, succeeded, failed, preempted int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.workers[id]; ok {
		w.Succeeded += int64(succeeded)
		w.Failed += int64(failed)
		w.Preempted += int64(preempted)
	}
	return nil
}

func (m *MemStore) AddAudit(_ context.Context, e *domain.AuditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.auditID++
	c := *e
	c.ID = m.auditID
	m.audit = append(m.audit, &c)
	return nil
}

func (m *MemStore) ListAudit(_ context.Context, taskID, dagID string, limit int) ([]*domain.AuditEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 200
	}
	var out []*domain.AuditEvent
	for i := len(m.audit) - 1; i >= 0; i-- {
		a := m.audit[i]
		if taskID != "" && a.TaskID != taskID {
			continue
		}
		if dagID != "" && a.DAGID != dagID {
			continue
		}
		c := *a
		out = append(out, &c)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *MemStore) CreateDAG(_ context.Context, d *domain.DAG) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.dags[d.ID]; ok {
		return domain.ErrConflict
	}
	c := *d
	nodes := make([]*domain.DAGNode, len(d.Nodes))
	for i, n := range d.Nodes {
		nc := *n
		nodes[i] = &nc
	}
	c.Nodes = nodes
	m.dags[d.ID] = &c
	return nil
}

func (m *MemStore) GetDAG(_ context.Context, id string) (*domain.DAG, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.dags[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	c := *d
	nodes := make([]*domain.DAGNode, len(d.Nodes))
	for i, n := range d.Nodes {
		nc := *n
		nodes[i] = &nc
	}
	c.Nodes = nodes
	return &c, nil
}

func (m *MemStore) ListDAGs(_ context.Context, limit int) ([]*domain.DAG, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*domain.DAG
	for _, d := range m.dags {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) SaveNode(_ context.Context, n *domain.DAGNode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.dags[n.DAGID]
	if !ok {
		return domain.ErrNotFound
	}
	for _, dn := range d.Nodes {
		if dn.NodeID == n.NodeID {
			dn.TaskID = n.TaskID
			dn.State = n.State
			dn.StartedAt = n.StartedAt
			dn.FinishedAt = n.FinishedAt
			dn.Error = n.Error
			dn.Def = n.Def
			dn.DependsOn = n.DependsOn
			m.auditLocked(n.TaskID, n.DAGID, "", "node:"+string(n.State),
				"", string(n.State), n.NodeID)
			return nil
		}
	}
	return domain.ErrNotFound
}

func (m *MemStore) SaveDAGStatus(_ context.Context, id string, st domain.DAGStatus, finishedAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.dags[id]
	if !ok {
		return domain.ErrNotFound
	}
	d.Status = st
	d.FinishedAt = finishedAt
	m.auditLocked("", id, "", "dag:"+string(st), "", string(st), "")
	return nil
}
