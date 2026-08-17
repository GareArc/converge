package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type Reconciler interface {
	Reconcile(ctx context.Context, id ID) error
}

type Func func(ctx context.Context, id ID) error

func (f Func) Reconcile(ctx context.Context, id ID) error { return f(ctx, id) }

type Spec struct {
	Name             string
	Reconciler       Reconciler
	Triggers         []Trigger
	Concurrency      int
	RunMode          converge.RunMode
	DeadLetterAfter  int
	Versions         VersionSource
	RateLimit        converge.Rate
	Middleware       []converge.Middleware
	AllowUnscheduled bool
	Paused           bool
}

func newEngine(s Spec) (*engine, error) {
	if s.Name == "" {
		return nil, errors.New("reconcile: Spec.Name is required")
	}
	fail := func(msg string) error { return fmt.Errorf("reconcile: job %q: %s", s.Name, msg) }
	if s.Reconciler == nil {
		return nil, fail("Spec.Reconciler is required")
	}
	if s.Concurrency < 0 {
		return nil, fail("Concurrency must not be negative")
	}
	if s.DeadLetterAfter < 0 {
		return nil, fail("DeadLetterAfter must not be negative")
	}
	if s.RateLimit.Events < 0 || s.RateLimit.Per < 0 {
		return nil, fail("RateLimit must not be negative")
	}
	if !s.RateLimit.IsZero() && (s.RateLimit.Events == 0 || s.RateLimit.Per == 0) {
		return nil, fail("RateLimit needs both Events and Per")
	}
	cfg := config{
		name:             s.Name,
		rec:              s.Reconciler,
		triggers:         slices.Clone(s.Triggers),
		concurrency:      s.Concurrency,
		runMode:          s.RunMode,
		deadLetterAfter:  s.DeadLetterAfter,
		versions:         s.Versions,
		rateLimit:        s.RateLimit,
		middleware:       slices.Clone(s.Middleware),
		allowUnscheduled: s.AllowUnscheduled,
		paused:           s.Paused,
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = 1
	}
	if cfg.runMode.IsZero() {
		cfg.runMode = converge.OnOneReplica
	}
	switch cfg.runMode {
	case converge.SplitAcrossReplicas:
		return nil, fail("SplitAcrossReplicas is not supported on the reconcile surface in v1")
	case converge.OnAllReplicas:
		if s.Versions != nil {
			return nil, fail("OnAllReplicas cannot use Versions")
		}
		if s.DeadLetterAfter > 0 {
			return nil, fail("OnAllReplicas cannot use DeadLetterAfter")
		}
		if !s.RateLimit.IsZero() {
			return nil, fail("OnAllReplicas cannot use RateLimit")
		}
	}
	if s.Versions != nil {
		return nil, fail("Versions is not supported yet")
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
			if tr.source.paged && cfg.runMode == converge.OnAllReplicas {
				return nil, fail("OnAllReplicas cannot use IDsByPage")
			}
			if tr.source.single {
				cfg.single = true
			}
		case *messageTrigger:
			if tr.queue == "" {
				return nil, fail("OnMessage needs a queue name")
			}
			if tr.idf == nil {
				return nil, fail("OnMessage needs an IDFunc")
			}
			if cfg.runMode == converge.OnAllReplicas && tr.opts.Delivery == converge.Group {
				return nil, fail("OnAllReplicas requires Broadcast delivery")
			}
		}
	}
	if !periodic && !cfg.allowUnscheduled {
		return nil, fail("no periodic trigger; set AllowUnscheduled to opt out of the schedule guarantee")
	}
	return &engine{cfg: cfg, ready: make(chan struct{})}, nil
}

func Register(rt *converge.Runtime, s Spec) error {
	e, err := newEngine(s)
	if err != nil {
		return err
	}
	return hook.RegisterJob(rt, e)
}
