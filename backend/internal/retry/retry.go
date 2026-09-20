// Package retry computes the next run time for a failed task.
package retry

import (
	"time"

	"github.com/robfig/cron/v3"

	"taskforge/internal/domain"
)

// NextRun computes the next attempt time. attempt is the just-failed
// attempt number (0-based). Returns ok=false when retries are exhausted.
func NextRun(p domain.RetryPolicy, attempt int, now time.Time) (time.Time, bool) {
	if attempt >= p.MaxRetries {
		return time.Time{}, false
	}
	switch p.Kind {
	case domain.RetryFixed:
		return now.Add(p.BaseInterval.Std()), true
	case domain.RetryCron:
		if p.Cron == "" {
			return time.Time{}, false
		}
		sched, err := cron.ParseStandard(p.Cron)
		if err != nil {
			// Invalid cron: fall back to a minute rather than losing the task.
			return now.Add(time.Minute), true
		}
		return sched.Next(now), true
	case domain.RetryExponential, "":
		d := p.BaseInterval.Std()
		if d <= 0 {
			d = time.Second
		}
		return now.Add(d << attempt), true
	}
	return time.Time{}, false
}
