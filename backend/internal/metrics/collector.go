// Package metrics collects live scheduling/execution statistics.
package metrics

import (
	"sync"
	"time"

	"taskforge/internal/domain"
)

const (
	bucketSeconds = 1
	windowBuckets = 900 // 15 minutes at 1s resolution
)

type bucket struct {
	completed    int64
	succeeded    int64
	failed       int64
	dead         int64
	latencyUS    int64 // sum of scheduling latency (ready -> start), microseconds
	latencyCount int64
	execUS       int64 // sum of execution duration, microseconds
	execCount    int64
	byPriority   [5]prioCounts
}

type prioCounts struct {
	succeeded int64
	failed    int64
}

// Collector is thread-safe and keeps a ring of per-second buckets.
type Collector struct {
	mu      sync.Mutex
	start   time.Time
	buckets []bucket
}

func New(now time.Time) *Collector {
	return &Collector{start: now.Truncate(time.Second), buckets: make([]bucket, windowBuckets)}
}

func (c *Collector) idx(now time.Time) int {
	secs := int(now.Truncate(time.Second).Sub(c.start) / time.Second)
	if secs < 0 {
		secs = 0
	}
	return secs % windowBuckets
}

// Outcome records one finished attempt.
func (c *Collector) RecordOutcome(t *domain.Task, execDuration, scheduleLatency time.Duration, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := &c.buckets[c.idx(now)]
	b.completed++
	switch t.State {
	case domain.StateSucceeded:
		b.succeeded++
		b.byPriority[t.Priority].succeeded++
	case domain.StateDead:
		b.dead++
		b.failed++
		b.byPriority[t.Priority].failed++
	default:
		// failed but retry pending
		b.failed++
		b.byPriority[t.Priority].failed++
	}
	if execDuration > 0 {
		b.execUS += execDuration.Microseconds()
		b.execCount++
	}
	if scheduleLatency > 0 {
		b.latencyUS += scheduleLatency.Microseconds()
		b.latencyCount++
	}
}

// TrendPoint is one second on the dashboard chart.
type TrendPoint struct {
	TS        int64 `json:"ts"`
	Completed int64 `json:"completed"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Dead      int64 `json:"dead"`
}

// Snapshot is the overview dashboard payload.
type Snapshot struct {
	Now               time.Time          `json:"now"`
	QueueDepth        map[string]int64   `json:"queue_depth"`
	DelayDepth        int64              `json:"delay_depth"`
	DeadBacklog       int64              `json:"dead_backlog"`
	Workers           WorkerSummary      `json:"workers"`
	Utilization       float64            `json:"utilization"`
	ThroughputPerSec  float64            `json:"throughput_per_sec"`
	SuccessRate       map[string]float64 `json:"success_rate"`
	FailureRate       map[string]float64 `json:"failure_rate"`
	AvgExecLatencyMS  float64            `json:"avg_exec_latency_ms"`
	AvgSchedLatencyMS float64            `json:"avg_sched_latency_ms"`
	Trend             []TrendPoint       `json:"trend"`
	DeadTrend         []TrendPoint       `json:"dead_trend"`
}

// WorkerSummary is filled in by the engine from live worker state.
type WorkerSummary struct {
	Online      int `json:"online"`
	Draining    int `json:"draining"`
	Offline     int `json:"offline"`
	ActiveSlots int `json:"active_slots"`
	TotalSlots  int `json:"total_slots"`
}

// QueueAndWorkers inputs are read live outside the collector.
type LiveInputs struct {
	Depth        map[int]int64
	DelayDepth   int64
	DeadBacklog  int64
	Workers      WorkerSummary
	TrendSeconds int
}

func (c *Collector) Snapshot(now time.Time, in LiveInputs) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := in.TrendSeconds
	if n <= 0 || n > windowBuckets {
		n = 300
	}
	snap := Snapshot{
		Now:         now,
		QueueDepth:  map[string]int64{},
		DelayDepth:  in.DelayDepth,
		DeadBacklog: in.DeadBacklog,
		Workers:     in.Workers,
		SuccessRate: map[string]float64{},
		FailureRate: map[string]float64{},
	}
	for p, d := range in.Depth {
		snap.QueueDepth[domain.Priority(p).String()] = d
	}
	if in.Workers.TotalSlots > 0 {
		snap.Utilization = float64(in.Workers.ActiveSlots) / float64(in.Workers.TotalSlots)
	}

	cur := c.idx(now)
	var agg bucket
	var trend []TrendPoint
	var totalCompleted int64
	// walk oldest -> newest across the last n buckets
	for i := n - 1; i >= 0; i-- {
		bi := ((cur-i)%windowBuckets + windowBuckets) % windowBuckets
		b := c.buckets[bi]
		agg.completed += b.completed
		agg.succeeded += b.succeeded
		agg.failed += b.failed
		agg.dead += b.dead
		agg.latencyUS += b.latencyUS
		agg.latencyCount += b.latencyCount
		agg.execUS += b.execUS
		agg.execCount += b.execCount
		for p := 0; p < 5; p++ {
			agg.byPriority[p].succeeded += b.byPriority[p].succeeded
			agg.byPriority[p].failed += b.byPriority[p].failed
		}
		ts := now.Add(-time.Duration(i) * time.Second).Truncate(time.Second)
		trend = append(trend, TrendPoint{
			TS: ts.Unix(), Completed: b.completed, Succeeded: b.succeeded,
			Failed: b.failed, Dead: b.dead,
		})
		if b.dead > 0 {
			snap.DeadTrend = append(snap.DeadTrend, TrendPoint{TS: ts.Unix(), Dead: b.dead})
		}
		totalCompleted += b.completed
	}
	snap.Trend = trend
	snap.ThroughputPerSec = float64(totalCompleted) / float64(n)
	if agg.execCount > 0 {
		snap.AvgExecLatencyMS = float64(agg.execUS) / float64(agg.execCount) / 1000
	}
	if agg.latencyCount > 0 {
		snap.AvgSchedLatencyMS = float64(agg.latencyUS) / float64(agg.latencyCount) / 1000
	}
	for _, p := range domain.AllPriorities() {
		s := agg.byPriority[p].succeeded
		f := agg.byPriority[p].failed
		tot := s + f
		if tot > 0 {
			snap.SuccessRate[p.String()] = float64(s) / float64(tot)
			snap.FailureRate[p.String()] = float64(f) / float64(tot)
		}
	}
	return snap
}
