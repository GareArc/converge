// Package worker is converge's edge-triggered surface: the message is the
// work, carried at-least-once with retry, and shelved once the retries run
// out. Use it when the truth lives in the message itself, not in a store
// you can re-read.
package worker

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/wiring"
)

const (
	DefaultConcurrency = 4
	defaultVisibility  = 5 * time.Minute
	visibilityMargin   = time.Minute
	DefaultMaxAttempts = 25
	DefaultMinBackoff  = time.Second
	DefaultMaxBackoff  = 15 * time.Minute
	DefaultMaxAge      = 24 * time.Hour
)

type RetryPolicy struct {
	MaxAttempts int
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	MaxAge      time.Duration
}

type HandleOpts struct {
	Concurrency int
	RunMode     converge.RunMode
	Retry       RetryPolicy
	Timeout     time.Duration
	RateLimit   converge.Rate
	Middleware  []converge.Middleware
	Until       converge.StopCondition
}

type decodeError struct{ err error }

func (d decodeError) Error() string { return "worker: decode: " + d.err.Error() }

func (d decodeError) Unwrap() error { return d.err }

func Handle[T any](rt *converge.Runtime, t Task[T], fn func(ctx context.Context, payload T) error, o HandleOpts) error {
	if err := t.check(); err != nil {
		return fmt.Errorf("worker: Handle: %w", err)
	}
	if fn == nil {
		return fmt.Errorf("worker: task %q: handler fn is required", t.name)
	}
	deps, err := wiring.DepsFor(rt)
	if err != nil {
		return err
	}
	queue := t.QueueName(deps.Namespace)
	if err := refuseSharedQueue(rt, t.name, queue); err != nil {
		return err
	}
	run := func(ctx context.Context, payload []byte) error {
		var p T
		if err := t.codec.Unmarshal(payload, &p); err != nil {
			return decodeError{err}
		}
		return fn(ctx, p)
	}
	e, err := newEngine(taskInfo{name: t.name, queue: queue, version: t.version}, run, o)
	if err != nil {
		return err
	}
	return hook.RegisterJob(rt, e)
}

func refuseSharedQueue(rt *converge.Runtime, task, queue string) error {
	infos, err := wiring.Jobs(rt)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if info.Surface == converge.SurfaceWorker && info.Queue == queue {
			return fmt.Errorf("worker: task %q: queue %q is already read by task %q", task, queue, info.Job)
		}
	}
	return nil
}

func newEngine(t taskInfo, run runFunc, o HandleOpts) (*engine, error) {
	fail := func(msg string) error { return fmt.Errorf("worker: task %q: %s", t.name, msg) }
	if o.Concurrency < 0 {
		return nil, fail("Concurrency must not be negative")
	}
	if o.Timeout < 0 {
		return nil, fail("Timeout must not be negative")
	}
	r := o.Retry
	if r.MaxAttempts < 0 || r.MinBackoff < 0 || r.MaxBackoff < 0 || r.MaxAge < 0 {
		return nil, fail("Retry values must not be negative")
	}
	if o.RateLimit.Events < 0 || o.RateLimit.Per < 0 {
		return nil, fail("RateLimit must not be negative")
	}
	if !o.RateLimit.IsZero() && (o.RateLimit.Events == 0 || o.RateLimit.Per == 0) {
		return nil, fail("RateLimit needs both Events and Per")
	}
	cfg := config{
		info:        t,
		run:         run,
		concurrency: o.Concurrency,
		runMode:     o.RunMode,
		retry:       r,
		timeout:     o.Timeout,
		rateLimit:   o.RateLimit,
		middleware:  slices.Clone(o.Middleware),
		until:       o.Until,
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = DefaultConcurrency
	}
	cfg.visibility = defaultVisibility
	if o.Timeout > 0 {
		if o.Timeout > math.MaxInt64-visibilityMargin {
			return nil, fmt.Errorf("worker: task %q: Timeout leaves no room for the redelivery margin", t.name)
		}
		cfg.visibility = o.Timeout + visibilityMargin
	}
	if cfg.retry.MaxAttempts == 0 {
		cfg.retry.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.retry.MinBackoff == 0 {
		cfg.retry.MinBackoff = DefaultMinBackoff
	}
	if cfg.retry.MaxBackoff == 0 {
		cfg.retry.MaxBackoff = DefaultMaxBackoff
	}
	if cfg.retry.MaxAge == 0 {
		cfg.retry.MaxAge = DefaultMaxAge
	}
	if cfg.retry.MinBackoff > cfg.retry.MaxBackoff {
		return nil, fail("Retry.MinBackoff must not exceed Retry.MaxBackoff")
	}
	if cfg.runMode.IsZero() {
		cfg.runMode = converge.Competing
	}
	if cfg.runMode == converge.OnAllReplicas && o.Retry != (RetryPolicy{}) {
		return nil, fail("OnAllReplicas cannot use Retry")
	}
	return &engine{cfg: cfg, ready: make(chan struct{}), state: converge.NotStarted}, nil
}
