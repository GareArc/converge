package convergetest

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/keys"
)

var _ converge.Lease = (*Lease)(nil)

type Lease struct {
	base      *inmem.Lease
	namespace string
}

func WrapLease(base *inmem.Lease, namespace string) *Lease {
	return &Lease{base: base, namespace: namespace}
}

func (l *Lease) TryAcquire(ctx context.Context, name string, ttl time.Duration) (converge.LeaseHandle, bool, error) {
	return l.base.TryAcquire(ctx, name, ttl)
}

func (l *Lease) Expire(name string) {
	targets := map[string]struct{}{
		name:                                   {},
		keys.WorkerLease(l.namespace, name):    {},
		keys.ReconcileLease(l.namespace, name): {},
	}
	for _, held := range l.base.Names() {
		if _, ok := targets[held]; ok {
			l.base.Expire(held)
		}
	}
}
