package retry

import (
	"testing"
	"time"

	"taskforge/internal/domain"
)

func TestExponentialBackoff(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	p := domain.RetryPolicy{Kind: domain.RetryExponential, BaseInterval: domain.Duration(time.Second), MaxRetries: 3}
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		got, ok := NextRun(p, attempt, now)
		if !ok {
			t.Fatalf("attempt %d should retry", attempt)
		}
		if d := got.Sub(now); d != want[attempt] {
			t.Fatalf("attempt %d backoff=%v want %v", attempt, d, want[attempt])
		}
	}
	if _, ok := NextRun(p, 3, now); ok {
		t.Fatal("attempt at MaxRetries must not retry")
	}
}

func TestFixedInterval(t *testing.T) {
	now := time.Now()
	p := domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: domain.Duration(5 * time.Second), MaxRetries: 1}
	got, ok := NextRun(p, 0, now)
	if !ok || got.Sub(now) != 5*time.Second {
		t.Fatalf("fixed next=%v ok=%v", got, ok)
	}
}

func TestCronSchedule(t *testing.T) {
	// every minute at :00
	now := time.Date(2026, 9, 20, 10, 30, 15, 0, time.UTC)
	p := domain.RetryPolicy{Kind: domain.RetryCron, Cron: "0 * * * *", MaxRetries: 5}
	got, ok := NextRun(p, 0, now)
	if !ok {
		t.Fatal("cron should schedule")
	}
	want := time.Date(2026, 9, 20, 11, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("cron next=%v want %v", got, want)
	}
}
