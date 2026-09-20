package clock

import (
	"sync"
	"time"
)

// FakeClock is a manually advanced clock for tests. All timer/ticker
// lifecycle state is guarded by the clock's own mutex so Advance, Stop and
// Reset are safe to call concurrently.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	tickers []*fakeTicker
}

func NewFake(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *FakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clock: f, fireAt: f.now.Add(d), ch: make(chan time.Time, 1)}
	f.timers = append(f.timers, t)
	return t
}

func (f *FakeClock) NewTicker(d time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTicker{clock: f, interval: d, nextAt: f.now.Add(d), ch: make(chan time.Time, 16)}
	f.tickers = append(f.tickers, t)
	return t
}

// Advance moves the clock and fires every due timer/ticker.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.now = target
	var dueTimers []*fakeTimer
	var dueTicks []*fakeTicker
	for _, t := range f.timers {
		if t.stopped || t.fired {
			continue
		}
		if !t.fireAt.After(target) {
			dueTimers = append(dueTimers, t)
		}
	}
	for _, t := range f.tickers {
		if t.stopped {
			continue
		}
		for !t.nextAt.After(target) {
			t.nextAt = t.nextAt.Add(t.interval)
			dueTicks = append(dueTicks, t)
		}
	}
	f.mu.Unlock()

	// Channel sends happen outside the clock lock to avoid blocking callers
	// that stop a timer from the same goroutine that receives.
	for _, t := range dueTimers {
		select {
		case t.ch <- target:
		default:
		}
		f.mu.Lock()
		t.fired = true
		f.mu.Unlock()
	}
	for _, t := range dueTicks {
		select {
		case t.ch <- target:
		default:
		}
	}
}

type fakeTimer struct {
	clock   *FakeClock
	ch      chan time.Time
	fireAt  time.Time
	stopped bool
	fired   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.stopped = true
	return wasActive
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.stopped = false
	t.fired = false
	t.fireAt = t.clock.now.Add(d)
	return wasActive
}

type fakeTicker struct {
	clock    *FakeClock
	ch       chan time.Time
	nextAt   time.Time
	interval time.Duration
	stopped  bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	t.stopped = true
	t.clock.mu.Unlock()
}
