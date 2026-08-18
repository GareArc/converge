package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/mw"
	"github.com/GareArc/converge/internal/tokenbucket"
)

type taskInfo struct {
	name    string
	queue   string
	version int
}

type runFunc func(ctx context.Context, payload []byte) error

type config struct {
	info        taskInfo
	run         runFunc
	concurrency int
	runMode     converge.RunMode
	retry       RetryPolicy
	visibility  time.Duration
	mq          converge.MQ
	rateLimit   converge.Rate
	middleware  []converge.Middleware
	paused      bool
}

type engine struct {
	cfg       config
	deps      converge.JobDeps
	mq        converge.MQ
	limit     *tokenbucket.Bucket
	handler   converge.Handler
	ready     chan struct{}
	readyOnce sync.Once

	mu          sync.Mutex
	depth       int
	deadLetters int
	lastSuccess time.Time
	consecFails int
}

func (e *engine) Name() string { return e.cfg.info.name }

func (e *engine) Ready() <-chan struct{} { return e.ready }

func (e *engine) markReady() { e.readyOnce.Do(func() { close(e.ready) }) }

func (e *engine) QueueBinding() (string, converge.MQ) { return e.cfg.info.queue, e.cfg.mq }

func (e *engine) Poke(string) error {
	return fmt.Errorf("worker: job %q: poke is a reconcile verb; requeue dead letters via ops instead", e.cfg.info.name)
}

func (e *engine) durable() bool { return e.cfg.runMode != converge.OnAllReplicas }

func (e *engine) key(parts ...string) string {
	elems := make([]string, 0, len(parts)+4)
	if e.deps.Namespace != "" {
		elems = append(elems, e.deps.Namespace)
	}
	elems = append(elems, "converge", "worker", e.cfg.info.name)
	elems = append(elems, parts...)
	return strings.Join(elems, "/")
}

func (e *engine) dlqKey(id string) string { return e.key("dlq", id) }

func (e *engine) Stats() converge.JobStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return converge.JobStats{
		Job:              e.cfg.info.name,
		Surface:          converge.SurfaceWorker,
		RunMode:          e.cfg.runMode,
		QueueDepth:       e.depth,
		Parked:           e.deadLetters,
		LastSuccess:      e.lastSuccess,
		ConsecutiveFails: e.consecFails,
	}
}

func (e *engine) bind(deps converge.JobDeps) error {
	e.deps = deps
	e.mq = e.cfg.mq
	if e.mq == nil {
		e.mq = deps.MQ
	}
	if e.mq == nil {
		return fmt.Errorf("worker: job %q: needs an MQ (HandleOpts.MQ or Options.MQ)", e.cfg.info.name)
	}
	switch e.cfg.runMode {
	case converge.SplitAcrossReplicas:
		if _, ok := e.mq.(converge.GroupConsumer); !ok {
			return fmt.Errorf("worker: job %q: SplitAcrossReplicas needs the GroupConsumer capability", e.cfg.info.name)
		}
	case converge.OnAllReplicas:
		if _, ok := e.mq.(converge.BroadcastConsumer); !ok {
			return fmt.Errorf("worker: job %q: OnAllReplicas needs the BroadcastConsumer capability", e.cfg.info.name)
		}
	case converge.OnOneReplica:
		if deps.Lease == nil {
			return fmt.Errorf("worker: job %q: OnOneReplica needs Options.Lease", e.cfg.info.name)
		}
	}
	if e.durable() {
		if deps.KV == nil {
			return fmt.Errorf("worker: job %q: dead-lettering needs Options.KV", e.cfg.info.name)
		}
		if _, ok := e.mq.(converge.DelayedPublisher); !ok {
			return fmt.Errorf("worker: job %q: Snooze needs the DelayedPublisher capability", e.cfg.info.name)
		}
	}
	mws := append(slices.Clone(deps.Middleware), e.cfg.middleware...)
	final := func(ctx context.Context, r converge.Run) error {
		inv, ok := invocationFrom(ctx)
		if !ok {
			return fmt.Errorf("worker: job %q: missing invocation context", e.cfg.info.name)
		}
		return e.cfg.run(ctx, inv.payload)
	}
	e.handler = mw.Chain(mws, final)
	e.limit = tokenbucket.New(e.cfg.rateLimit, deps.Clock)
	return nil
}

type invocation struct{ payload []byte }

type invocationKey struct{}

func withInvocation(ctx context.Context, inv invocation) context.Context {
	return context.WithValue(ctx, invocationKey{}, inv)
}

func invocationFrom(ctx context.Context) (invocation, bool) {
	inv, ok := ctx.Value(invocationKey{}).(invocation)
	return inv, ok
}

func (e *engine) Run(ctx context.Context, deps converge.JobDeps) error {
	if err := e.bind(deps); err != nil {
		return err
	}
	e.markReady()
	<-ctx.Done()
	return nil
}
