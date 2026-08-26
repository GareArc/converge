package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type Spec struct {
	Name        string
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
	if s.Name == "" {
		return nil, errors.New("reconcile: Spec.Name is required")
	}
	fail := func(msg string) error { return fmt.Errorf("reconcile: job %q: %s", s.Name, msg) }
	if strings.Contains(s.Name, "/") {
		return nil, fail(`Name must not contain "/"`)
	}
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
		name:        s.Name,
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
		if _, ok := t.(PeriodicTrigger); ok {
			periodic = true
		}
		switch tr := t.(type) {
		case *scheduleTrigger:
			if tr.source.IsZero() {
				return nil, fail("Schedule needs an IDSource")
			}
			if tr.cad.err != nil {
				return nil, fmt.Errorf("reconcile: job %q: %w", s.Name, tr.cad.err)
			}
			if tr.cad.every == 0 && tr.cad.sched == nil {
				return nil, fail("Schedule needs a Cadence; use Every or Cron")
			}
			if tr.source.single {
				cfg.single = true
			}
		case *notificationTrigger:
			if tr.foreign {
				if tr.queue == "" {
					return nil, fail("NotificationsFrom needs a queue name")
				}
				if tr.opts.ID == nil {
					return nil, fail("NotificationsFrom needs an ID function")
				}
			} else if tr.opts.MQ != nil {
				return nil, fail("Notifications always reads Options.MQ; MQ is NotificationsFrom only")
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
