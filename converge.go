// Package converge gives services one model for all background work: a
// level-triggered reconcile surface and an edge-triggered worker surface on
// one kernel. This package is the kernel: the runtime, its ports, and the
// shared value types. See github.com/GareArc/converge/reconcile and
// github.com/GareArc/converge/worker for the two surfaces.
package converge

import (
	"errors"
	"slices"
	"time"
)

type Options struct {
	Namespace    string        // prefixes all leases, KV keys, engine queues
	MQ           MQ            // default transport (optional until a job needs it)
	Lease        Lease         // required by OnOneReplica (validated at registration)
	KV           KV            // engine state: last-fire, dead-letter marks, cursors
	Observer     Observer      // nil = no-op
	Middleware   []Middleware  // wraps every run, both surfaces, outermost first
	Clock        Clock         // nil = wall clock
	LeaseTTL     time.Duration // default 30s; heartbeat at TTL/3
	DrainTimeout time.Duration // default 30s
}

const (
	defaultLeaseTTL     = 30 * time.Second
	defaultDrainTimeout = 30 * time.Second
)

func New(o Options) (*Runtime, error) {
	if o.LeaseTTL < 0 || o.DrainTimeout < 0 {
		return nil, errors.New("converge: durations in Options must not be negative")
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = defaultLeaseTTL
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = defaultDrainTimeout
	}
	if o.Observer == nil {
		o.Observer = noopObserver{}
	}
	if o.Clock == nil {
		o.Clock = systemClock{}
	}
	o.Middleware = slices.Clone(o.Middleware)
	return &Runtime{
		opts:  o,
		jobs:  map[string]job{},
		ready: make(chan struct{}),
	}, nil
}
