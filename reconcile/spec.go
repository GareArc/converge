package reconcile

import "context"

type Reconciler interface {
	Reconcile(ctx context.Context, id ID) error
}

type Func func(ctx context.Context, id ID) error

func (f Func) Reconcile(ctx context.Context, id ID) error { return f(ctx, id) }
