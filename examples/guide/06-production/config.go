package main

import (
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

func configFromEnv() Config {
	return Config{
		RedisAddr:    env("REDIS_ADDR", "localhost:6379"),
		Namespace:    env("CONVERGE_NAMESPACE", "shop"),
		DebugAddr:    env("DEBUG_ADDR", "localhost:6060"),
		SyncEvery:    10 * time.Second,
		DrainTimeout: 20 * time.Second,
	}
}
