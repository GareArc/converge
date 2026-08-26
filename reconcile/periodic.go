package reconcile

import (
	"context"
	"fmt"

	"github.com/GareArc/converge"
)

func Periodic(rt *converge.Runtime, name string, c Cadence, fn func(ctx context.Context) error) error {
	if fn == nil {
		return fmt.Errorf("reconcile: job %q: Periodic needs a function", name)
	}
	return Register(rt, Spec{
		Name:      name,
		Reconcile: func(ctx context.Context, _ ID) error { return fn(ctx) },
		Triggers:  []Trigger{Schedule(SingleID(), c)},
	})
}
