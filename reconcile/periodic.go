package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/GareArc/converge"
)

type PeriodicOpts struct {
	Timeout    time.Duration
	RunMode    converge.RunMode
	Middleware []converge.Middleware
}

func Periodic(rt *converge.Runtime, name string, c Cadence, fn func(ctx context.Context) error, o PeriodicOpts) error {
	if fn == nil {
		return fmt.Errorf("reconcile: job %q: Periodic needs a function", name)
	}
	return Register(rt, Spec{
		Name:       name,
		Reconcile:  func(ctx context.Context, _ ID) error { return fn(ctx) },
		Triggers:   []Trigger{Schedule(SingleID(), c)},
		Timeout:    o.Timeout,
		RunMode:    o.RunMode,
		Middleware: o.Middleware,
	})
}
