package converge

import (
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
