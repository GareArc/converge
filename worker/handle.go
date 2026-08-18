package worker

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/pausegate"
)

const (
	DefaultConcurrency = 4
	DefaultVisibility  = 5 * time.Minute
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
	Visibility  time.Duration
	MQ          converge.MQ
	RateLimit   converge.Rate
	Middleware  []converge.Middleware
	Paused      bool
}

type decodeError struct{ err error }

func (d decodeError) Error() string { return "worker: decode: " + d.err.Error() }

func (d decodeError) Unwrap() error { return d.err }

func Handle[T any](rt *converge.Runtime, t Task[T], fn func(ctx context.Context, payload T) error, o HandleOpts) error {
	if t.err != nil {
		return fmt.Errorf("worker: Handle: %w", t.err)
	}
	if fn == nil {
		return fmt.Errorf("worker: task %q: handler fn is required", t.name)
	}
	run := func(ctx context.Context, payload []byte) error {
		var p T
		if err := t.codec.Unmarshal(payload, &p); err != nil {
			return decodeError{err}
		}
		return fn(ctx, p)
	}
	e, err := newEngine(taskInfo{name: t.name, queue: t.queue, version: t.version}, run, o)
	if err != nil {
		return err
	}
	return hook.RegisterJob(rt, e)
}

func newEngine(t taskInfo, run runFunc, o HandleOpts) (*engine, error) {
	fail := func(msg string) error { return fmt.Errorf("worker: task %q: %s", t.name, msg) }
	if o.Concurrency < 0 {
		return nil, fail("Concurrency must not be negative")
	}
	if o.Visibility < 0 {
		return nil, fail("Visibility must not be negative")
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
		visibility:  o.Visibility,
		mq:          o.MQ,
		rateLimit:   o.RateLimit,
		middleware:  slices.Clone(o.Middleware),
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = DefaultConcurrency
	}
	if cfg.visibility == 0 {
		cfg.visibility = DefaultVisibility
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
		cfg.runMode = converge.SplitAcrossReplicas
	}
	if cfg.runMode == converge.OnAllReplicas && o.Retry != (RetryPolicy{}) {
		return nil, fail("OnAllReplicas cannot use Retry")
	}
	return &engine{cfg: cfg, ready: make(chan struct{}), gate: pausegate.New(o.Paused)}, nil
}
