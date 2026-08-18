package convergetest

import (
	"context"
	"strings"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
)

var _ converge.Lease = (*Lease)(nil)

type Lease struct {
	base *inmem.Lease
}

func WrapLease(base *inmem.Lease) *Lease {
	return &Lease{base: base}
}

func (l *Lease) TryAcquire(ctx context.Context, name string, ttl time.Duration) (converge.LeaseHandle, bool, error) {
	return l.base.TryAcquire(ctx, name, ttl)
}

func (l *Lease) Expire(name string) {
	for _, held := range l.base.Names() {
		if leaseNameMatches(held, name) {
			l.base.Expire(held)
		}
	}
}

func leaseNameMatches(held, name string) bool {
	if held == name {
		return true
	}
	for _, seg := range strings.Split(held, "/") {
		if seg == name {
			return true
		}
	}
	return false
}
