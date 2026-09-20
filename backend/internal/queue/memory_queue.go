package queue

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryQueue is an in-process Queue used by unit tests.
type MemoryQueue struct {
	mu sync.Mutex

	ready [5][]readyEntry
	seq   int64

	delay map[string]delayEntry
	inf   map[string]inflightEntry
}

type readyEntry struct {
	id   string
	seq  int64 // >0 normal, <0 head
	head bool
}

type delayEntry struct {
	priority int
	runAt    time.Time
}

type inflightEntry struct {
	workerID string
	priority int
	expires  time.Time
}

func NewMemory() *MemoryQueue {
	return &MemoryQueue{delay: map[string]delayEntry{}, inf: map[string]inflightEntry{}}
}

func (m *MemoryQueue) Ping(context.Context) error { return nil }

func (m *MemoryQueue) Reset(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = [5][]readyEntry{}
	m.seq = 0
	m.delay = map[string]delayEntry{}
	m.inf = map[string]inflightEntry{}
	return nil
}

func (m *MemoryQueue) EnqueueReady(_ context.Context, taskID string, priority int, head bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	s := m.seq
	if head {
		s = -s
	}
	m.ready[priority] = append(m.ready[priority], readyEntry{id: taskID, seq: s, head: head})
	m.sortLocked(priority)
	return nil
}

func (m *MemoryQueue) sortLocked(p int) {
	sort.SliceStable(m.ready[p], func(i, j int) bool {
		// negative (head) first; within a band FIFO by insertion
		return m.ready[p][i].seq < m.ready[p][j].seq
	})
}

func (m *MemoryQueue) ReadyDepth(context.Context) (map[int]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[int]int64, 5)
	for p := 0; p < 5; p++ {
		out[p] = int64(len(m.ready[p]))
	}
	return out, nil
}

func (m *MemoryQueue) ClaimOne(_ context.Context, forceLow bool, _ time.Time) (*Claim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := 0
	if forceLow {
		start = 2
	}
	for p := start; p < 5; p++ {
		if len(m.ready[p]) == 0 {
			continue
		}
		e := m.ready[p][0]
		m.ready[p] = m.ready[p][1:]
		return &Claim{TaskID: e.id, Priority: p, Head: e.head}, nil
	}
	return nil, nil
}

func (m *MemoryQueue) AddInflight(_ context.Context, workerID, taskID string, priority int, expires time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inf[taskID] = inflightEntry{workerID: workerID, priority: priority, expires: expires}
	return nil
}

func (m *MemoryQueue) RenewInflight(_ context.Context, _, taskID string, expires time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.inf[taskID]
	if !ok {
		return false, nil
	}
	e.expires = expires
	m.inf[taskID] = e
	return true, nil
}

func (m *MemoryQueue) RemoveInflight(_ context.Context, _, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inf, taskID)
	return nil
}

func (m *MemoryQueue) expiredLocked(now time.Time) []Inflight {
	var out []Inflight
	for id, e := range m.inf {
		if !e.expires.After(now) {
			out = append(out, Inflight{TaskID: id, WorkerID: e.workerID, Priority: e.priority})
		}
	}
	return out
}

func (m *MemoryQueue) ExpiredInflights(_ context.Context, now time.Time) ([]Inflight, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.expiredLocked(now), nil
}

func (m *MemoryQueue) AllInflights(context.Context) ([]Inflight, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Inflight
	for id, e := range m.inf {
		out = append(out, Inflight{TaskID: id, WorkerID: e.workerID, Priority: e.priority})
	}
	return out, nil
}

func (m *MemoryQueue) RequeueInflight(_ context.Context, _, taskID string, priority int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inf, taskID)
	m.seq++
	m.ready[priority] = append(m.ready[priority], readyEntry{id: taskID, seq: -m.seq, head: true})
	m.sortLocked(priority)
	return nil
}

func (m *MemoryQueue) AddDelay(_ context.Context, taskID string, priority int, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay[taskID] = delayEntry{priority: priority, runAt: runAt}
	return nil
}

func (m *MemoryQueue) RemoveDelay(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.delay, taskID)
	return nil
}

func (m *MemoryQueue) PopDue(_ context.Context, now time.Time) ([]Claim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Claim
	for id, d := range m.delay {
		if !d.runAt.After(now) {
			out = append(out, Claim{TaskID: id, Priority: d.priority})
			delete(m.delay, id)
		}
	}
	return out, nil
}

func (m *MemoryQueue) DelaySize(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.delay)), nil
}
