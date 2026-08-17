package converge

import "context"

type Run struct {
	Job     string
	Surface Surface
	ID      string
}

type Handler func(ctx context.Context, run Run) error

type Middleware func(next Handler) Handler
