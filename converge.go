package converge

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"time"
)

type Options struct {
	Namespace    string
	MQ           MQ
	Lease        Lease
	KV           KV
	Observer     Observer
	Middleware   []Middleware
	Clock        Clock
	LeaseTTL     time.Duration
	DrainTimeout time.Duration
	attach       func(rt *Runtime)
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
	rt := &Runtime{
		opts:    o,
		jobs:    map[string]job{},
		queues:  map[string]queueBinding{},
		ready:   make(chan struct{}),
		replica: newReplicaID(),
	}
	if o.attach != nil {
		o.attach(rt)
	}
	return rt, nil
}

func newReplicaID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
