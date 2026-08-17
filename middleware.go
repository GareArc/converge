package converge

import "context"

// Run is the normalized view of one execution, both surfaces.
type Run struct {
	Job     string
	Surface Surface
	ID      string // reconcile ID or worker message ID
}

type Handler func(ctx context.Context, run Run) error

// Middleware wraps every run; Options.Middleware applies outermost-first,
// followed by per-spec middleware.
type Middleware func(next Handler) Handler
