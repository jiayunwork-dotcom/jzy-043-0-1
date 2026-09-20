// Package demo registers built-in task handlers so the service is usable
// immediately after `docker compose up` without external workers.
package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/processor"
)

type payloadSpec struct {
	SleepMS      int    `json:"sleep_ms"`
	Fail         bool   `json:"fail"`
	FailFirstN   int    `json:"fail_first_n"`
	FlakyPercent int    `json:"flaky_percent"`
	ErrorType    string `json:"error_type"`
}

// Register installs: echo, sleep, flaky, always-fail, compute.
// echo/sleep/flaky/compute each get two handler instances to demonstrate
// per-type load balancing.
func Register(reg *processor.Registry) {
	reg.RegisterFunc("echo", "echo-1", func(ctx context.Context, t *domain.Task) ([]byte, error) {
		return t.Payload, nil
	})
	reg.RegisterFunc("echo", "echo-2", func(ctx context.Context, t *domain.Task) ([]byte, error) {
		return t.Payload, nil
	})

	reg.RegisterFunc("sleep", "sleep-1", sleepHandler)
	reg.RegisterFunc("sleep", "sleep-2", sleepHandler)

	reg.RegisterFunc("flaky", "flaky-1", makeFlaky())
	reg.RegisterFunc("flaky", "flaky-2", makeFlaky())

	reg.RegisterFunc("always-fail", "always-fail-1", func(ctx context.Context, t *domain.Task) ([]byte, error) {
		return nil, processor.WrapError("PermanentFailure", fmt.Errorf("task always fails"))
	})

	reg.RegisterFunc("compute", "compute-1", computeHandler)
	reg.RegisterFunc("compute", "compute-2", computeHandler)
}

func parse(t *domain.Task) payloadSpec {
	var p payloadSpec
	_ = json.Unmarshal(t.Payload, &p)
	return p
}

func sleepHandler(ctx context.Context, t *domain.Task) ([]byte, error) {
	p := parse(t)
	if p.SleepMS <= 0 {
		p.SleepMS = 50
	}
	select {
	case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
		return []byte(`{"slept":true}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// makeFlaky fails the first FailFirstN attempts or randomly per percent.
func makeFlaky() func(ctx context.Context, t *domain.Task) ([]byte, error) {
	return func(ctx context.Context, t *domain.Task) ([]byte, error) {
		p := parse(t)
		if p.Fail {
			return nil, processor.WrapError(or(p.ErrorType, "FlakyFailure"),
				fmt.Errorf("configured failure"))
		}
		if p.FailFirstN > 0 && int(t.Attempt) < p.FailFirstN {
			return nil, processor.WrapError(or(p.ErrorType, "TransientFailure"),
				fmt.Errorf("attempt %d failed (fail_first_n=%d)", t.Attempt, p.FailFirstN))
		}
		if p.FlakyPercent > 0 && rand.Intn(100) < p.FlakyPercent {
			return nil, processor.WrapError(or(p.ErrorType, "TransientFailure"),
				fmt.Errorf("random failure"))
		}
		return []byte(`{"ok":true}`), nil
	}
}

func computeHandler(ctx context.Context, t *domain.Task) ([]byte, error) {
	var p struct {
		A  float64 `json:"a"`
		B  float64 `json:"b"`
		Op string  `json:"op"`
	}
	_ = json.Unmarshal(t.Payload, &p)
	var r float64
	switch p.Op {
	case "add", "":
		r = p.A + p.B
	case "sub":
		r = p.A - p.B
	case "mul":
		r = p.A * p.B
	case "div":
		if p.B == 0 {
			return nil, processor.WrapError("MathError", fmt.Errorf("division by zero"))
		}
		r = p.A / p.B
	}
	return json.Marshal(map[string]any{"result": r})
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
