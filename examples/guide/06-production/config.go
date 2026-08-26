package main

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	RedisAddr    string
	Namespace    string
	DebugAddr    string
	SyncEvery    time.Duration
	DrainTimeout time.Duration
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func configFromEnv() (Config, error) {
	syncEvery, err := envDuration("SYNC_EVERY", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	drainTimeout, err := envDuration("DRAIN_TIMEOUT", 20*time.Second)
	if err != nil {
		return Config{}, err
	}
	return Config{
		RedisAddr:    env("REDIS_ADDR", "localhost:6379"),
		Namespace:    env("CONVERGE_NAMESPACE", "shop"),
		DebugAddr:    env("DEBUG_ADDR", "localhost:6060"),
		SyncEvery:    syncEvery,
		DrainTimeout: drainTimeout,
	}, nil
}
