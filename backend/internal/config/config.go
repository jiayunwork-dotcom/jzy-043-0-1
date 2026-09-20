// Package config loads runtime configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	QueueDriver   string

	HTTPAddr string

	WorkerCount       int
	SlotsPerWorker    int
	FairnessThreshold int

	DispatchInterval  time.Duration
	DelayScanInterval time.Duration
	HeartbeatInterval time.Duration
	WorkerTimeout     time.Duration
	LeaseTTL          time.Duration
	ShutdownTimeout   time.Duration
}

func Load() Config {
	c := Config{
		PostgresDSN:       env("POSTGRES_DSN", "postgres://taskforge:taskforge@localhost:5432/taskforge?sslmode=disable"),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     env("REDIS_PASSWORD", ""),
		RedisDB:           envInt("REDIS_DB", 0),
		QueueDriver:       env("QUEUE_DRIVER", "redis"),
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		WorkerCount:       envInt("WORKER_COUNT", 3),
		SlotsPerWorker:    envInt("SLOTS_PER_WORKER", 4),
		FairnessThreshold: envInt("FAIRNESS_THRESHOLD", 5),
		DispatchInterval:  envDuration("DISPATCH_INTERVAL", 50*time.Millisecond),
		DelayScanInterval: envDuration("DELAY_SCAN_INTERVAL", 250*time.Millisecond),
		HeartbeatInterval: envDuration("HEARTBEAT_INTERVAL", 3*time.Second),
		WorkerTimeout:     envDuration("WORKER_TIMEOUT", 15*time.Second),
		LeaseTTL:          envDuration("LEASE_TTL", 15*time.Second),
		ShutdownTimeout:   envDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	if c.FairnessThreshold < 1 {
		c.FairnessThreshold = 5
	}
	return c
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
