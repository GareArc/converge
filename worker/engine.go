package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
	"github.com/GareArc/converge/internal/clockctx"
	"github.com/GareArc/converge/internal/durfmt"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/mw"
	"github.com/GareArc/converge/internal/sig"
	"github.com/GareArc/converge/internal/tokenbucket"
)

const (
	reasonMaxAttempts   = "max attempts"
	reasonMaxAge        = "max age"
	reasonSchemaVersion = "schema version"
	reasonUndecodable   = "undecodable"
	reasonWrongSurface  = "wrong surface"
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
	timeout     time.Duration
	visibility  time.Duration
	rateLimit   converge.Rate
	middleware  []converge.Middleware
	until       converge.StopCondition
}

type engine struct {
	cfg       config
	deps      converge.JobDeps
	limit     *tokenbucket.Bucket
	handler   converge.Handler
	ready     chan struct{}
	readyOnce sync.Once

	mu          sync.Mutex
	inFlight    int
	shelved     int
	retrying    map[string]time.Time
	leaseHeld   bool
	lastSuccess time.Time
	lastErr     error
	lastErrAt   time.Time
	consecFails int
	state       converge.State

	stopCh      chan struct{}
	destroyOnce sync.Once
}

func (e *engine) Name() string { return e.cfg.info.name }

func (e *engine) Ready() <-chan struct{} { return e.ready }

func (e *engine) markReady() { e.readyOnce.Do(func() { close(e.ready) }) }

func (e *engine) Quiet() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inFlight == 0
}

func (e *engine) Notify(string) error {
	return fmt.Errorf("worker: job %q: notify is a reconcile verb; workers react to deliveries instead", e.cfg.info.name)
}

func (e *engine) RunPassNow(context.Context) error {
	return fmt.Errorf("worker: job %q: passes are a reconcile verb; workers have no schedule to run", e.cfg.info.name)
}

func (e *engine) durable() bool { return e.cfg.runMode != converge.OnAllReplicas }

func (e *engine) key(parts ...string) string {
	return keys.Worker(e.deps.Namespace, e.cfg.info.name, parts...)
}

func (e *engine) shelfKey(id string) string {
	return keys.WorkerShelf(e.deps.Namespace, e.cfg.info.name, id)
}

func (e *engine) Stats() converge.JobStats {
	failing := e.failing()
	e.mu.Lock()
	defer e.mu.Unlock()
	return converge.JobStats{
		Job:              e.cfg.info.name,
		Surface:          converge.SurfaceWorker,
		RunMode:          e.cfg.runMode,
		State:            e.state,
		LeaseHeld:        e.leaseHeld,
		InFlight:         e.inFlight,
		Failing:          failing,
		Shelved:          e.shelved,
		LastSuccess:      e.lastSuccess,
		LastError:        e.lastErr,
		LastErrorAt:      e.lastErrAt,
		ConsecutiveFails: e.consecFails,
	}
}

const workerRetryingBound = 65536

func (e *engine) failing() int {
	now := e.deps.Clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, until := range e.retrying {
		if !until.After(now) {
			delete(e.retrying, id)
		}
	}
	return len(e.retrying)
}

func (e *engine) markRetrying(id string, delay time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.retrying == nil {
		e.retrying = map[string]time.Time{}
	}
	if len(e.retrying) >= workerRetryingBound {
		return
	}
	e.retrying[id] = e.deps.Clock.Now().Add(delay)
}

func (e *engine) setLeaseHeld(held bool) {
	e.mu.Lock()
	e.leaseHeld = held
	e.mu.Unlock()
}

func (e *engine) Info() converge.JobInfo {
	settings := map[string]string{
		"concurrency":    strconv.Itoa(e.cfg.concurrency),
		"retry":          retrySetting(e.cfg.retry),
		"schema-version": strconv.Itoa(e.cfg.info.version),
	}
	if e.cfg.timeout > 0 {
		settings["timeout"] = durfmt.Format(e.cfg.timeout)
	}
	if !e.cfg.rateLimit.IsZero() {
		settings["rate-limit"] = e.cfg.rateLimit.String()
	}
	return converge.JobInfo{
		Job:      e.cfg.info.name,
		Surface:  converge.SurfaceWorker,
		RunMode:  e.cfg.runMode,
		Queue:    e.inbox(),
		Settings: settings,
	}
}

func (e *engine) inbox() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg.info.queue
}

func retrySetting(r RetryPolicy) string {
	return fmt.Sprintf("%d attempts, backoff %s..%s, max-age %s",
		r.MaxAttempts, durfmt.Format(r.MinBackoff), durfmt.Format(r.MaxBackoff), durfmt.Format(r.MaxAge))
}

func (e *engine) bind(deps converge.JobDeps) error {
	e.deps = deps
	e.mu.Lock()
	e.cfg.info.queue = keys.Inbox(deps.Namespace, e.cfg.info.name)
	e.mu.Unlock()
	if deps.MQ == nil {
		return fmt.Errorf("worker: job %q: needs Options.MQ", e.cfg.info.name)
	}
	switch e.cfg.runMode {
	case converge.Competing:
		if _, ok := deps.MQ.(converge.GroupConsumer); !ok {
			return fmt.Errorf("worker: job %q: Competing needs the GroupConsumer capability", e.cfg.info.name)
		}
	case converge.OnAllReplicas:
		if _, ok := deps.MQ.(converge.BroadcastConsumer); !ok {
			return fmt.Errorf("worker: job %q: OnAllReplicas needs the BroadcastConsumer capability", e.cfg.info.name)
		}
	case converge.OnOneReplica:
		if deps.Lease == nil {
			return fmt.Errorf("worker: job %q: OnOneReplica needs Options.Lease", e.cfg.info.name)
		}
	}
	if e.durable() {
		if deps.KV == nil {
			return fmt.Errorf("worker: job %q: shelving needs Options.KV", e.cfg.info.name)
		}
		if _, ok := deps.MQ.(converge.DelayedPublisher); !ok {
			return fmt.Errorf("worker: job %q: Snooze needs the DelayedPublisher capability", e.cfg.info.name)
		}
	}
	if !e.cfg.until.IsZero() && deps.KV == nil {
		return fmt.Errorf("worker: job %q: Until needs Options.KV", e.cfg.info.name)
	}
	e.stopCh = make(chan struct{})
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

const (
	consumeRetryInterval = time.Second
	destroyCheckInterval = 30 * time.Second
)

func (e *engine) setState(s converge.State) {
	e.mu.Lock()
	e.state = s
	e.mu.Unlock()
}

func (e *engine) destroyed(ctx context.Context) (converge.StopCondition, bool) {
	if e.cfg.until.IsZero() || e.deps.KV == nil {
		return converge.StopCondition{}, false
	}
	if at, ok := hook.StopConditionDeadline(e.cfg.until); ok && !e.deps.Clock.Now().Before(at) {
		e.deps.KV.Set(ctx, keys.Tombstone(e.deps.Namespace, e.cfg.info.name), []byte("1"), 0)
		return e.cfg.until, true
	}
	key := keys.Tombstone(e.deps.Namespace, e.cfg.info.name)
	if k, ok := hook.StopConditionKey(e.cfg.until); ok {
		key = k
	}
	if _, found, err := e.deps.KV.Get(ctx, key); err != nil || !found {
		return converge.StopCondition{}, false
	}
	return converge.StopKey(key), true
}

func (e *engine) checkDestroy(ctx context.Context) bool {
	cause, ok := e.destroyed(ctx)
	if !ok {
		return false
	}
	first := false
	e.destroyOnce.Do(func() {
		e.setState(converge.Destroyed)
		close(e.stopCh)
		first = true
	})
	if first {
		e.deps.Observer.Observe(converge.JobDestroyed{Job: e.cfg.info.name, Cause: cause})
	}
	return true
}

func (e *engine) destroyChecks(ctx context.Context, stop <-chan struct{}) {
	if e.cfg.until.IsZero() {
		return
	}
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-e.deps.Clock.After(destroyCheckInterval):
			if e.checkDestroy(ctx) {
				return
			}
		}
	}
}

func (e *engine) isDestroyed() bool {
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

func (e *engine) Run(ctx context.Context, deps converge.JobDeps) error {
	if err := e.bind(deps); err != nil {
		return err
	}
	e.setState(converge.Active)
	e.markReady()
	switch e.cfg.runMode {
	case converge.OnOneReplica:
		e.leaseLoop(ctx)
	default:
		e.runActive(ctx, nil)
	}
	return nil
}

func (e *engine) runActive(ctx context.Context, h converge.LeaseHandle) {
	base := context.WithoutCancel(ctx)
	hctx, cancelHandlers := context.WithCancel(base)
	defer cancelHandlers()
	ictx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()

	stopHB := make(chan struct{})
	defer close(stopHB)
	if h != nil {
		lost := make(chan struct{})
		go e.leaseHeartbeat(ctx, h, lost, stopHB)
		go func() {
			select {
			case <-lost:
				stopIntake()
				cancelHandlers()
			case <-stopHB:
			}
		}()
	}

	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-e.stopCh:
			stopIntake()
			cancelHandlers()
		case <-stopWatch:
		}
	}()

	var wg sync.WaitGroup
	intakeDone := make(chan struct{})
	go func() {
		e.intake(ictx, hctx, &wg)
		close(intakeDone)
	}()

	<-ictx.Done()
	finished := make(chan struct{})
	go func() {
		<-intakeDone
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-e.deps.Clock.After(e.deps.DrainTimeout):
		cancelHandlers()
		<-finished
	}
	if h != nil {
		select {
		case <-h.Done():
		default:
			h.Release(base)
		}
	}
}

func (e *engine) intake(ictx, hctx context.Context, wg *sync.WaitGroup) {
	slots := make(chan struct{}, e.cfg.concurrency)
	deliver := func(d converge.Delivery) { e.receive(ictx, hctx, d, slots, wg) }
	stopChecks := make(chan struct{})
	defer close(stopChecks)
	go e.destroyChecks(ictx, stopChecks)
	for {
		if e.checkDestroy(ictx) {
			return
		}
		e.startConsumer(ictx, deliver)
		if ictx.Err() != nil {
			return
		}
		select {
		case <-ictx.Done():
			return
		case <-e.deps.Clock.After(consumeRetryInterval):
		}
	}
}

func (e *engine) startConsumer(ctx context.Context, deliver func(converge.Delivery)) {
	switch e.cfg.runMode {
	case converge.OnOneReplica:
		e.deps.MQ.Consume(ctx, e.cfg.info.queue, deliver)
	case converge.OnAllReplicas:
		e.deps.MQ.(converge.BroadcastConsumer).ConsumeBroadcast(ctx, e.cfg.info.queue, deliver)
	default:
		e.deps.MQ.(converge.GroupConsumer).ConsumeGroup(ctx, e.cfg.info.queue, e.key("group"), deliver)
	}
}

func (e *engine) receive(ictx, hctx context.Context, d converge.Delivery, slots chan struct{}, wg *sync.WaitGroup) {
	m := d.Message()
	if e.durable() {
		d.Extend(ictx, e.cfg.visibility)
	}
	e.setInFlight(1)
	select {
	case slots <- struct{}{}:
	case <-ictx.Done():
		e.setInFlight(-1)
		e.neutral(context.WithoutCancel(ictx), d, m)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { <-slots }()
		defer e.setInFlight(-1)
		e.process(hctx, d, m)
	}()
}

func (e *engine) setInFlight(delta int) {
	e.mu.Lock()
	e.inFlight += delta
	e.mu.Unlock()
}

func (e *engine) classify(d converge.Delivery, m converge.Message) (Meta, string) {
	env := newEnvelope(d, m)
	attempt, ok := env.attempt()
	meta := Meta{
		Task:        e.cfg.info.name,
		Queue:       e.cfg.info.queue,
		MessageID:   env.messageID(),
		Attempt:     attempt,
		MaxAttempts: e.cfg.retry.MaxAttempts,
		EnqueuedAt:  env.enqueuedAt(),
		Headers:     maps.Clone(m.Headers),
	}
	if !e.durable() {
		return meta, ""
	}
	if env.schemaVersion() != strconv.Itoa(e.cfg.info.version) {
		return meta, reasonSchemaVersion
	}
	if !ok {
		return meta, reasonUndecodable
	}
	if e.deps.Clock.Now().Sub(meta.EnqueuedAt) > e.cfg.retry.MaxAge {
		return meta, reasonMaxAge
	}
	if meta.Attempt > e.cfg.retry.MaxAttempts {
		return meta, reasonMaxAttempts
	}
	return meta, ""
}

func (e *engine) process(hctx context.Context, d converge.Delivery, m converge.Message) {
	meta, guard := e.classify(d, m)
	sctx := context.WithoutCancel(hctx)
	if guard != "" {
		e.observeRun(meta, 0, converge.Shelved, nil)
		e.shelve(sctx, d, meta, m, guard, nil)
		return
	}
	stopHB := make(chan struct{})
	if e.durable() {
		go e.extendLoop(sctx, d, stopHB)
	}
	if err := e.limit.Wait(hctx); err != nil {
		close(stopHB)
		e.neutral(sctx, d, m)
		return
	}
	runCtx := hctx
	if e.cfg.timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = clockctx.WithTimeout(hctx, e.deps.Clock, e.cfg.timeout)
		defer cancel()
	}
	start := e.deps.Clock.Now()
	err := e.invokeChain(runCtx, meta, m.Payload)
	took := e.deps.Clock.Now().Sub(start)
	close(stopHB)
	e.settle(hctx, d, m, meta, err, took)
}

func (e *engine) invokeChain(ctx context.Context, meta Meta, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("worker: job %q: handler panic: %v", e.cfg.info.name, r)
		}
	}()
	ctx = withMeta(ctx, meta)
	ctx = withInvocation(ctx, invocation{payload: payload})
	return e.handler(ctx, converge.Run{Job: e.cfg.info.name, Surface: converge.SurfaceWorker, ID: meta.MessageID})
}

func (e *engine) settle(hctx context.Context, d converge.Delivery, m converge.Message, meta Meta, err error, took time.Duration) {
	sctx := context.WithoutCancel(hctx)
	if err == nil {
		d.Ack(sctx)
		e.recordSuccess()
		e.observeRun(meta, took, converge.Succeeded, nil)
		return
	}
	var dec decodeError
	if errors.As(err, &dec) {
		e.observeRun(meta, took, converge.Shelved, err)
		e.shelveOrDrop(sctx, d, meta, m, reasonUndecodable, err)
		return
	}
	if s, ok := sig.FromError(err); ok {
		if e.settleSignal(sctx, d, m, meta, s, err, took) {
			return
		}
	}
	if hctx.Err() != nil {
		e.neutral(sctx, d, m)
		return
	}
	e.recordFailure(err)
	e.observeRun(meta, took, converge.Retrying, err)
	if !e.durable() {
		return
	}
	if meta.Attempt >= e.cfg.retry.MaxAttempts {
		e.shelve(sctx, d, meta, m, reasonMaxAttempts, err)
		return
	}
	delay := e.retryCurve().Delay(meta.Attempt)
	e.markRetrying(meta.MessageID, delay)
	d.Nack(sctx, delay)
}

func (e *engine) retryCurve() backoff.Curve {
	return backoff.Curve{Min: e.cfg.retry.MinBackoff, Max: e.cfg.retry.MaxBackoff}
}

func (e *engine) recordSuccess() {
	now := e.deps.Clock.Now()
	e.mu.Lock()
	e.lastSuccess = now
	e.consecFails = 0
	e.mu.Unlock()
}

func (e *engine) recordFailure(err error) {
	now := e.deps.Clock.Now()
	e.mu.Lock()
	e.consecFails++
	e.lastErr = err
	e.lastErrAt = now
	e.mu.Unlock()
}

func (e *engine) observeRun(meta Meta, took time.Duration, outcome converge.Outcome, err error) {
	e.deps.Observer.Observe(converge.RunCompleted{
		Job:      e.cfg.info.name,
		ID:       meta.MessageID,
		Attempt:  meta.Attempt,
		Duration: took,
		Outcome:  outcome,
		Err:      err,
	})
}

func (e *engine) settleSignal(sctx context.Context, d converge.Delivery, m converge.Message, meta Meta, s sig.Signal, err error, took time.Duration) bool {
	if s.ControlSurface() != converge.SurfaceWorker {
		e.observeRun(meta, took, converge.Shelved, err)
		e.shelveOrDrop(sctx, d, meta, m, reasonWrongSurface, err)
		return true
	}
	switch v := s.(type) {
	case Discard:
		e.discard(sctx, d, meta, v, took)
		return true
	case *Discard:
		e.discard(sctx, d, meta, *v, took)
		return true
	case Snooze:
		e.snooze(sctx, d, m, meta, v.In, err, took)
		return true
	case *Snooze:
		e.snooze(sctx, d, m, meta, v.In, err, took)
		return true
	case Shelve:
		e.observeRun(meta, took, converge.Shelved, err)
		e.shelveOrDrop(sctx, d, meta, m, v.Reason, nil)
		return true
	case *Shelve:
		e.observeRun(meta, took, converge.Shelved, err)
		e.shelveOrDrop(sctx, d, meta, m, v.Reason, nil)
		return true
	}
	return false
}

func (e *engine) discard(sctx context.Context, d converge.Delivery, meta Meta, v Discard, took time.Duration) {
	d.Ack(sctx)
	e.recordSuccess()
	e.observeRun(meta, took, converge.Discarded, nil)
}

func (e *engine) snooze(sctx context.Context, d converge.Delivery, m converge.Message, meta Meta, in time.Duration, err error, took time.Duration) {
	if !e.durable() {
		e.recordFailure(err)
		e.observeRun(meta, took, converge.Deferred, nil)
		return
	}
	remaining := e.cfg.retry.MaxAge - e.deps.Clock.Now().Sub(meta.EnqueuedAt)
	if remaining <= 0 {
		e.observeRun(meta, took, converge.Shelved, nil)
		e.shelve(sctx, d, meta, m, reasonMaxAge, nil)
		return
	}
	env := newEnvelope(d, m)
	snoozes := env.snoozes() + 1
	delay := backoff.Floor(in)
	if snoozes > backoff.NoBackoffCap {
		delay = e.retryCurve().Delay(snoozes - backoff.NoBackoffCap)
	}
	if delay > remaining {
		delay = remaining
	}
	republished := env.forSnooze()
	if err := e.deps.MQ.(converge.DelayedPublisher).PublishDelayed(sctx, e.cfg.info.queue, republished, delay); err != nil {
		d.Nack(sctx, delay)
		e.observeRun(meta, took, converge.Deferred, nil)
		return
	}
	d.Ack(sctx)
	e.observeRun(meta, took, converge.Deferred, nil)
}

func (e *engine) leaseLoop(ctx context.Context) {
	name := keys.WorkerLease(e.deps.Namespace, e.cfg.info.name)
	retry := e.deps.LeaseTTL / 3
	e.markReady()
	for {
		if e.checkDestroy(ctx) {
			return
		}
		h, ok, err := e.deps.Lease.TryAcquire(ctx, name, e.deps.LeaseTTL)
		if err == nil && ok {
			e.setLeaseHeld(true)
			e.deps.Observer.Observe(converge.LeaseChanged{Job: e.cfg.info.name, Held: true})
			e.runActive(ctx, h)
			e.setLeaseHeld(false)
			e.deps.Observer.Observe(converge.LeaseChanged{Job: e.cfg.info.name, Held: false})
			if e.isDestroyed() {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-e.deps.Clock.After(retry):
		}
	}
}

func (e *engine) leaseHeartbeat(runCtx context.Context, h converge.LeaseHandle, lost chan<- struct{}, stop <-chan struct{}) {
	interval := e.deps.LeaseTTL / 3
	base := context.WithoutCancel(runCtx)
	for {
		select {
		case <-stop:
			return
		case <-h.Done():
			close(lost)
			return
		case <-e.deps.Clock.After(interval):
			if e.checkDestroy(runCtx) {
				return
			}
			ectx, cancel := context.WithTimeout(base, interval)
			err := h.Extend(ectx, e.deps.LeaseTTL)
			cancel()
			if err != nil && runCtx.Err() == nil {
				close(lost)
				return
			}
		}
	}
}

func (e *engine) extendLoop(ctx context.Context, d converge.Delivery, stop <-chan struct{}) {
	interval := e.cfg.visibility / 3
	for {
		select {
		case <-stop:
			return
		case <-e.deps.Clock.After(interval):
			d.Extend(ctx, e.cfg.visibility)
		}
	}
}

func (e *engine) shelveOrDrop(ctx context.Context, d converge.Delivery, meta Meta, m converge.Message, reason string, cause error) {
	e.recordFailure(cause)
	if !e.durable() {
		return
	}
	e.shelve(ctx, d, meta, m, reason, cause)
}

func (e *engine) shelve(ctx context.Context, d converge.Delivery, meta Meta, m converge.Message, reason string, cause error) {
	rec := ShelvedMessage{
		Task:       e.cfg.info.name,
		Queue:      e.cfg.info.queue,
		MessageID:  meta.MessageID,
		Attempt:    meta.Attempt,
		Reason:     reason,
		EnqueuedAt: meta.EnqueuedAt,
		ShelvedAt:  e.deps.Clock.Now(),
		Headers:    meta.Headers,
		Payload:    m.Payload,
	}
	if cause != nil {
		rec.Error = cause.Error()
	}
	raw, err := json.Marshal(rec)
	if err == nil {
		err = e.deps.KV.Set(ctx, e.shelfKey(meta.MessageID), raw, 0)
	}
	if err != nil {
		delay := e.retryCurve().Delay(meta.Attempt)
		e.markRetrying(meta.MessageID, delay)
		d.Nack(ctx, delay)
		return
	}
	d.Ack(ctx)
	e.mu.Lock()
	e.shelved++
	e.mu.Unlock()
}

func (e *engine) neutral(ctx context.Context, d converge.Delivery, m converge.Message) {
	if !e.durable() {
		return
	}
	env := newEnvelope(d, m)
	if _, ok := env.attempt(); !ok {
		d.Nack(ctx, 0)
		return
	}
	if err := e.deps.MQ.Publish(ctx, e.cfg.info.queue, env.forNeutral()); err != nil {
		d.Nack(ctx, 0)
		return
	}
	d.Ack(ctx)
}
