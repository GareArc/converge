// Package reconcile is converge's level-triggered surface: a notification
// only names an ID, and the registered function re-reads the caller's store
// and converges its state. Losing a notification costs latency, never
// correctness — the schedule sweep is the backstop.
package reconcile

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type Spec struct {
	Job         Job
	Reconcile   func(ctx context.Context, id ID) error
	Triggers    []Trigger
	Concurrency int
	RunMode     converge.RunMode
	Timeout     time.Duration
	Versions    VersionSource
	Middleware  []converge.Middleware
	Until       converge.StopCondition
}

func newEngine(s Spec) (*engine, error) {
	if err := s.Job.check(); err != nil {
		return nil, err
	}
	fail := func(msg string) error { return fmt.Errorf("reconcile: job %q: %s", s.Job.name, msg) }
	if s.Reconcile == nil {
		return nil, fail("Spec.Reconcile is required")
	}
	if s.Concurrency < 0 {
		return nil, fail("Concurrency must not be negative")
	}
	if s.Timeout < 0 {
		return nil, fail("Timeout must not be negative")
	}
	cfg := config{
		job:         s.Job,
		fn:          s.Reconcile,
		triggers:    slices.Clone(s.Triggers),
		concurrency: s.Concurrency,
		runMode:     s.RunMode,
		timeout:     s.Timeout,
		versions:    s.Versions,
		middleware:  slices.Clone(s.Middleware),
		until:       s.Until,
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = 1
	}
	if cfg.runMode.IsZero() {
		cfg.runMode = converge.OnOneReplica
	}
	if cfg.runMode == converge.Competing {
		return nil, fail("Competing is a worker mode")
	}
	periodic := false
	for _, t := range cfg.triggers {
		if t == nil {
			return nil, fail("Triggers must not contain nil")
		}
		switch tr := t.(type) {
		case *scheduleTrigger:
			periodic = true
			if tr.source.IsZero() {
				return nil, fail("Schedule needs an IDSource")
			}
			if tr.cad.err != nil {
				return nil, fmt.Errorf("reconcile: job %q: %w", s.Job.name, tr.cad.err)
			}
			if tr.cad.every == 0 && tr.cad.sched == nil {
				return nil, fail("Schedule needs a Cadence; use Every or Cron")
			}
			if tr.source.single {
				cfg.single = true
			}
		case *notificationTrigger:
			if tr.foreign {
				if tr.source == "" {
					return nil, fail("NotificationsFrom needs a source name")
				}
				if tr.id == nil {
					return nil, fail("NotificationsFrom needs an ID function")
				}
			}
		default:
			if _, ok := t.(PeriodicTrigger); ok {
				return nil, fail("only Schedule is swept; a custom PeriodicTrigger runs but never sweeps")
			}
		}
	}
	if !periodic {
		return nil, fail("no Schedule trigger; every reconcile job needs one")
	}
	return &engine{cfg: cfg, ready: make(chan struct{}), state: converge.NotStarted}, nil
}

func Register(rt *converge.Runtime, s Spec) error {
	e, err := newEngine(s)
	if err != nil {
		return err
	}
	return hook.RegisterJob(rt, e)
}
