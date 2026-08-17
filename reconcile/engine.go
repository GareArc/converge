package reconcile

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/mw"
	"github.com/GareArc/converge/internal/sig"
)

const (
	backoffMin     = time.Second
	backoffMax     = 15 * time.Minute
	noBackoffFloor = 250 * time.Millisecond
)

func backoffAfter(consecutiveFails int) time.Duration {
	d := backoffMin
	for i := 1; i < consecutiveFails; i++ {
		d *= 2
		if d >= backoffMax {
			return jitter(backoffMax)
		}
	}
	return jitter(d)
}

func floorDelay(d time.Duration) time.Duration {
	if d < noBackoffFloor {
		return noBackoffFloor + time.Duration(rand.Int63n(int64(noBackoffFloor/2)+1))
	}
	return d
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

func refillWait(tokens float64, r converge.Rate) time.Duration {
	need := time.Duration((1 - tokens) / float64(r.Events) * float64(r.Per))
	if need < time.Millisecond {
		need = time.Millisecond
	}
	return need
}

type tokenBucket struct {
	rate  converge.Rate
	clock converge.Clock

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newTokenBucket(r converge.Rate, clock converge.Clock) *tokenBucket {
	if r.Events <= 0 || r.Per <= 0 {
		return nil
	}
	return &tokenBucket{rate: r, clock: clock, tokens: float64(r.Events), last: clock.Now()}
}

func (b *tokenBucket) wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	for {
		b.mu.Lock()
		now := b.clock.Now()
		b.tokens += now.Sub(b.last).Seconds() * float64(b.rate.Events) / b.rate.Per.Seconds()
		if b.tokens > float64(b.rate.Events) {
			b.tokens = float64(b.rate.Events)
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		need := refillWait(b.tokens, b.rate)
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.clock.After(need):
		}
	}
}

type config struct {
	name             string
	rec              Reconciler
	triggers         []Trigger
	concurrency      int
	runMode          converge.RunMode
	deadLetterAfter  int
	versions         VersionSource
	rateLimit        converge.Rate
	middleware       []converge.Middleware
	allowUnscheduled bool
	paused           bool
	single           bool
}

type engine struct {
	cfg       config
	deps      converge.JobDeps
	limit     *tokenBucket
	handler   converge.Handler
	ready     chan struct{}
	readyOnce sync.Once

	mu          sync.Mutex
	queue       *wakeQueue
	lastSuccess time.Time
	consecFails int
}

func (e *engine) Name() string { return e.cfg.name }

func (e *engine) Ready() <-chan struct{} { return e.ready }

func (e *engine) markReady() { e.readyOnce.Do(func() { close(e.ready) }) }

func (e *engine) bindCore(deps converge.JobDeps) error {
	e.deps = deps
	if e.cfg.concurrency <= 0 {
		e.cfg.concurrency = 1
	}
	mws := append(slices.Clone(deps.Middleware), e.cfg.middleware...)
	final := func(ctx context.Context, r converge.Run) error {
		return e.invoke(ctx, ID(r.ID))
	}
	e.handler = mw.Chain(mws, final)
	e.limit = newTokenBucket(e.cfg.rateLimit, deps.Clock)
	e.mu.Lock()
	e.queue = newWakeQueue(deps.Clock, wakePolicy{
		deadLetterAfter: e.cfg.deadLetterAfter,
		backoff:         backoffAfter,
		floor:           floorDelay,
	}, e.cfg.paused)
	e.mu.Unlock()
	return nil
}

func (e *engine) wakeQueueRef() *wakeQueue {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queue
}

func (e *engine) Poke(id string) error {
	q := e.wakeQueueRef()
	if q == nil {
		return fmt.Errorf("reconcile: job %q is not running", e.cfg.name)
	}
	if id == "" && !e.cfg.single {
		return fmt.Errorf("reconcile: job %q: poke needs an id", e.cfg.name)
	}
	if e.cfg.single {
		id = ""
	}
	res := q.wake(ID(id), wakePoke)
	if res == wakeRevived && e.durableParks() {
		e.deps.KV.Delete(context.Background(), e.parkKey(ID(id)))
	}
	e.report(ID(id), res)
	return nil
}

func (e *engine) Stats() converge.JobStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := converge.JobStats{
		Job:              e.cfg.name,
		Surface:          converge.SurfaceReconcile,
		RunMode:          e.cfg.runMode,
		LastSuccess:      e.lastSuccess,
		ConsecutiveFails: e.consecFails,
	}
	if e.queue != nil {
		c := e.queue.counts()
		s.QueueDepth = c.depth
		s.Parked = c.parked
	}
	return s
}

func (e *engine) hint(ctx context.Context, id ID) {
	if id == "" && !e.cfg.single {
		e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, Reason: converge.DiscardEmptyID})
		return
	}
	res := e.queue.wake(id, wakeHint)
	if res == wakeDroppedParked && e.tryRevive(ctx, id) {
		return
	}
	e.report(id, res)
}

func (e *engine) tryRevive(ctx context.Context, id ID) bool {
	if e.cfg.versions == nil {
		return false
	}
	marked := e.parkedVersion(ctx, id)
	latest, err := e.cfg.versions.Latest(ctx, id)
	if err != nil || latest <= marked {
		return false
	}
	if e.queue.wake(id, wakeVersion) != wakeRevived {
		return false
	}
	e.deleteKey(ctx, e.parkKey(id))
	return true
}

func (e *engine) parkedVersion(ctx context.Context, id ID) Version {
	raw := e.readString(ctx, e.parkKey(id))
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return Version(n)
}

func (e *engine) report(id ID, res wakeResult) {
	var reason converge.WakeDiscardReason
	switch res {
	case wakeDroppedParked:
		reason = converge.DiscardParked
	case wakeDroppedPaused:
		reason = converge.DiscardPaused
	case wakeDroppedOverflow:
		reason = converge.DiscardOverflow
	default:
		return
	}
	e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, ID: string(id), Reason: reason})
}

func (e *engine) dispatch(ctx context.Context, hctx context.Context, wg *sync.WaitGroup) {
	slots := make(chan struct{}, e.cfg.concurrency)
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		case <-hctx.Done():
			return
		}
		id, ok := e.awaitDue(ctx)
		if !ok {
			return
		}
		if err := e.limit.wait(ctx); err != nil {
			e.queue.finish(id, finishNeutral, 0)
			return
		}
		wg.Add(1)
		go func(id ID) {
			defer wg.Done()
			defer func() { <-slots }()
			e.runOne(hctx, id)
		}(id)
	}
}

func (e *engine) awaitDue(ctx context.Context) (ID, bool) {
	for {
		if id, ok := e.queue.next(e.deps.Clock.Now()); ok {
			return id, true
		}
		var timer <-chan time.Time
		if due, ok := e.queue.nextDue(); ok {
			timer = e.deps.Clock.After(due.Sub(e.deps.Clock.Now()))
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-e.queue.notify:
		case <-timer:
		}
	}
}

type versionSnapshot struct {
	v     Version
	known bool
}

func (e *engine) preRunVersion(ctx context.Context, id ID) versionSnapshot {
	if e.cfg.versions == nil {
		return versionSnapshot{}
	}
	v, err := e.cfg.versions.Latest(ctx, id)
	if err != nil {
		return versionSnapshot{}
	}
	return versionSnapshot{v: v, known: true}
}

func (e *engine) runOne(hctx context.Context, id ID) {
	start := e.deps.Clock.Now()
	snap := e.preRunVersion(hctx, id)
	run := converge.Run{Job: e.cfg.name, Surface: converge.SurfaceReconcile, ID: string(id)}
	err := e.invokeChain(hctx, run)
	e.settle(hctx, id, err, e.deps.Clock.Now().Sub(start), snap)
}

func (e *engine) invokeChain(ctx context.Context, run converge.Run) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicErr(e.cfg.name, r)
		}
	}()
	return e.handler(ctx, run)
}

func panicErr(name string, r any) error {
	return fmt.Errorf("reconcile: %s: panic: %v", name, r)
}

func (e *engine) invoke(ctx context.Context, id ID) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicErr(e.cfg.name, r)
		}
	}()
	return e.cfg.rec.Reconcile(ctx, id)
}

func (e *engine) settle(hctx context.Context, id ID, err error, took time.Duration, snap versionSnapshot) {
	var (
		kind  finishKind
		delay time.Duration
		wrong converge.Surface
	)
	s, isSig := sig.FromError(err)
	switch {
	case err == nil:
		kind = finishSuccess
	case isSig && s.ControlSurface() != converge.SurfaceReconcile:
		kind = finishForcePark
		wrong = s.ControlSurface()
	case isSig:
		if d, ok := checkAgainDelay(s); ok {
			kind = finishDelay
			delay = d
		} else if errors.Is(s, ErrOutdated) {
			kind = finishDelay
		} else {
			kind = finishFailure
		}
	case hctx.Err() != nil:
		kind = finishNeutral
	default:
		kind = finishFailure
	}
	res := e.queue.finish(id, kind, delay)
	if !res.settled {
		return
	}
	if kind == finishNeutral {
		return
	}
	e.record(kind)
	e.deps.Observer.Observe(converge.RunCompleted{
		Job:      e.cfg.name,
		Surface:  converge.SurfaceReconcile,
		ID:       string(id),
		Attempt:  res.attempt,
		Duration: took,
		Err:      runErr(kind, err),
	})
	if kind == finishForcePark {
		e.deps.Observer.Observe(converge.WrongSurfaceSignal{Job: e.cfg.name, ID: string(id), Surface: wrong})
	}
	if res.fallback {
		e.deps.Observer.Observe(converge.BackoffFallback{Job: e.cfg.name, ID: string(id), Consecutive: noBackoffLimit + 1})
	}
	if res.parked {
		e.deps.Observer.Observe(converge.IDParked{Job: e.cfg.name, ID: string(id), Failures: res.attempt, Err: err})
		if !res.revived {
			e.markParked(hctx, id, snap)
		}
	}
	if res.droppedHint {
		e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, ID: string(id), Reason: converge.DiscardParked})
	}
}

func checkAgainDelay(s sig.Signal) (time.Duration, bool) {
	switch v := s.(type) {
	case CheckAgain:
		return v.In, true
	case *CheckAgain:
		return v.In, true
	}
	return 0, false
}

func runErr(kind finishKind, err error) error {
	if kind == finishSuccess || kind == finishDelay {
		return nil
	}
	return err
}

func (e *engine) record(kind finishKind) {
	now := e.deps.Clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	switch kind {
	case finishSuccess, finishDelay:
		e.lastSuccess = now
		e.consecFails = 0
	case finishFailure, finishForcePark:
		e.consecFails++
	}
}

func (e *engine) key(parts ...string) string {
	elems := make([]string, 0, len(parts)+4)
	if e.deps.Namespace != "" {
		elems = append(elems, e.deps.Namespace)
	}
	elems = append(elems, "converge", "reconcile", e.cfg.name)
	elems = append(elems, parts...)
	return strings.Join(elems, "/")
}

func (e *engine) parkKey(id ID) string { return e.key("parked", string(id)) }

func (e *engine) durableParks() bool {
	return e.deps.KV != nil && e.cfg.runMode != converge.OnAllReplicas
}

func (e *engine) markParked(ctx context.Context, id ID, snap versionSnapshot) {
	if !e.durableParks() {
		return
	}
	if e.cfg.versions != nil && snap.known && snap.v == 0 {
		e.deps.Observer.Observe(converge.VersionZero{Job: e.cfg.name, ID: string(id)})
	}
	e.writeString(ctx, e.parkKey(id), strconv.FormatUint(uint64(snap.v), 10))
}

func (e *engine) loadParked(ctx context.Context) {
	if !e.durableParks() {
		return
	}
	prefix := e.parkKey("")
	cursor := ""
	for {
		keys, next, err := e.deps.KV.Scan(ctx, prefix, cursor)
		if err != nil {
			if !e.pauseOnInfraError(ctx) {
				return
			}
			continue
		}
		for _, k := range keys {
			e.queue.restorePark(ID(strings.TrimPrefix(k, prefix)))
		}
		if next == "" {
			return
		}
		cursor = next
	}
}

func (e *engine) Run(ctx context.Context, deps converge.JobDeps) error {
	if err := e.bind(deps); err != nil {
		return err
	}
	defer func() {
		e.mu.Lock()
		e.queue = nil
		e.mu.Unlock()
	}()
	if e.cfg.runMode == converge.OnAllReplicas {
		e.runActive(ctx, nil)
		return nil
	}
	return e.leaseLoop(ctx)
}

func (e *engine) bind(deps converge.JobDeps) error {
	e.deps = deps
	if e.cfg.runMode == converge.OnOneReplica && deps.Lease == nil {
		return fmt.Errorf("reconcile: job %q: OnOneReplica needs Options.Lease", e.cfg.name)
	}
	if e.cfg.versions != nil && deps.KV == nil {
		return fmt.Errorf("reconcile: job %q: Versions needs Options.KV", e.cfg.name)
	}
	for _, t := range e.cfg.triggers {
		switch tr := t.(type) {
		case *scheduleTrigger:
			if deps.KV == nil {
				return fmt.Errorf("reconcile: job %q: Schedule needs Options.KV", e.cfg.name)
			}
		case *messageTrigger:
			if err := tr.bind(e); err != nil {
				return err
			}
		}
	}
	return e.bindCore(deps)
}

func (e *engine) leaseInterval() time.Duration {
	return e.deps.LeaseTTL / 3
}

func (e *engine) leaseLoop(ctx context.Context) error {
	name := e.key("lease")
	retry := e.leaseInterval()
	for {
		h, ok, err := e.deps.Lease.TryAcquire(ctx, name, e.deps.LeaseTTL)
		e.markReady()
		if err == nil && ok {
			e.deps.Observer.Observe(converge.LeaseTransition{Job: e.cfg.name, Acquired: true})
			e.runActive(ctx, h)
			e.deps.Observer.Observe(converge.LeaseTransition{Job: e.cfg.name, Acquired: false})
			if ctx.Err() != nil {
				return nil
			}
			e.queue.reset()
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-e.deps.Clock.After(retry):
		}
	}
}

func (e *engine) runActive(ctx context.Context, h converge.LeaseHandle) {
	intake, stopIntake := context.WithCancel(ctx)
	defer stopIntake()
	hctx, stopHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHandlers()

	hbStop := make(chan struct{})
	var hb sync.WaitGroup
	if h != nil {
		hb.Add(1)
		go func() {
			defer hb.Done()
			e.heartbeat(ctx, h, hbStop, stopIntake, stopHandlers)
		}()
	}

	e.loadParked(intake)

	var aux sync.WaitGroup
	for i, t := range e.cfg.triggers {
		aux.Add(1)
		go func(i int, t Trigger) {
			defer aux.Done()
			e.runTrigger(intake, i, t)
		}(i, t)
	}
	var runs sync.WaitGroup
	aux.Add(1)
	go func() {
		defer aux.Done()
		e.dispatch(intake, hctx, &runs)
	}()
	e.markReady()

	<-intake.Done()
	aux.Wait()
	if ctx.Err() != nil {
		drained := make(chan struct{})
		go func() {
			runs.Wait()
			close(drained)
		}()
		select {
		case <-drained:
		case <-e.deps.Clock.After(e.deps.DrainTimeout):
			stopHandlers()
			<-drained
		}
	} else {
		stopHandlers()
		runs.Wait()
	}
	close(hbStop)
	hb.Wait()
	if h != nil {
		select {
		case <-h.Done():
		default:
			h.Release(context.WithoutCancel(ctx))
		}
	}
}

func (e *engine) heartbeat(ctx context.Context, h converge.LeaseHandle, stop <-chan struct{}, stopIntake, stopHandlers func()) {
	interval := e.leaseInterval()
	for {
		select {
		case <-stop:
			return
		case <-h.Done():
			stopHandlers()
			stopIntake()
			return
		case <-e.deps.Clock.After(interval):
			extendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
			err := h.Extend(extendCtx, e.deps.LeaseTTL)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				stopHandlers()
				stopIntake()
				return
			}
		}
	}
}
