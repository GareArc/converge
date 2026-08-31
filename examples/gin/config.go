package main

import "os"

const (
	namespace          = "shop"
	defaultHTTPAddr    = ":8080"
	defaultRedisAddr   = "localhost:6379"
	defaultPostgresDSN = "postgres://converge:converge@localhost:5432/converge?sslmode=disable"
)

type config struct {
	HTTPAddr    string
	RedisAddr   string
	PostgresDSN string
	Namespace   string
}

func loadConfig() config {
	return config{
		HTTPAddr:    env("HTTP_ADDR", defaultHTTPAddr),
		RedisAddr:   env("REDIS_ADDR", defaultRedisAddr),
		PostgresDSN: env("POSTGRES_DSN", defaultPostgresDSN),
		Namespace:   namespace,
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
