package worker

import (
	"context"
	"time"
)

type Meta struct {
	Task        string
	Queue       string
	MessageID   string
	Attempt     int
	MaxAttempts int
	EnqueuedAt  time.Time
	Headers     map[string]string
}

type metaKey struct{}

func withMeta(ctx context.Context, m Meta) context.Context {
	return context.WithValue(ctx, metaKey{}, m)
}

func MetaFromContext(ctx context.Context) (Meta, bool) {
	m, ok := ctx.Value(metaKey{}).(Meta)
	return m, ok
}
