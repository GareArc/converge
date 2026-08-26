package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/mw"
	"github.com/GareArc/converge/internal/sig"
)

const (
	backoffMin = time.Second
	backoffMax = 15 * time.Minute
)

type config struct {
	name        string
	fn          func(ctx context.Context, id ID) error
	triggers    []Trigger
	concurrency int
	runMode     converge.RunMode
	versions    VersionSource
	middleware  []converge.Middleware
	single      bool
}

type engine struct {
	cfg       config
	deps      converge.JobDeps
	handler   converge.Handler
	ready     chan struct{}
	readyOnce sync.Once

	mu          sync.Mutex
	queue       *wakeQueue
	lastSuccess time.Time
	consecFails int
	passes      int
	active      bool
	opsInFlight sync.WaitGroup
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
	curve := backoff.Curve{Min: backoffMin, Max: backoffMax}
	e.mu.Lock()
	e.queue = newWakeQueue(deps.Clock, wakePolicy{
		backoff: curve.Delay,
		floor:   backoff.Floor,
	})
	e.mu.Unlock()
	return nil
}

func (e *engine) wakeQueueRef() *wakeQueue {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queue
}

func (e *engine) Quiet() bool {
	e.mu.Lock()
	q := e.queue
	inFlight := e.passes > 0
	e.mu.Unlock()
	if q == nil {
		return true
	}
	if inFlight {
		return false
	}
	return q.quiet(e.deps.Clock.Now())
}

func (e *engine) Hint(id string) error {
	q := e.wakeQueueRef()
	if q == nil {
		return fmt.Errorf("reconcile: job %q is not running", e.cfg.name)
	}
	if id == "" && !e.cfg.single {
		return fmt.Errorf("reconcile: job %q: hint needs an id", e.cfg.name)
	}
	if e.cfg.single {
		id = ""
	}
	e.hintVia(context.Background(), q, ID(id))
	return nil
}

func (e *engine) admitOps() (*wakeQueue, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.active {
		return nil, false
	}
	e.opsInFlight.Add(1)
	return e.queue, true
}

func (e *engine) isActive() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active
}

func (e *engine) RunPassNow(ctx context.Context) error {
	q, ok := e.admitOps()
	if !ok {
		return fmt.Errorf("reconcile: job %q: run-pass-now needs the engine to be active", e.cfg.name)
	}
	defer e.opsInFlight.Done()
	found := false
	for idx, t := range e.cfg.triggers {
		st, ok := t.(*scheduleTrigger)
		if !ok {
			continue
		}
		if !e.isActive() {
			return fmt.Errorf("reconcile: job %q: run-pass-now needs the engine to be active", e.cfg.name)
		}
		found = true
		cursorKey := e.key("opspass", strconv.Itoa(idx))
		e.deleteKey(ctx, cursorKey)
		if !e.runPass(ctx, q, st, cursorKey) {
			return ctx.Err()
		}
	}
	if !found {
		return fmt.Errorf("reconcile: job %q: run-pass-now needs a Schedule trigger", e.cfg.name)
	}
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
	}
	return s
}

func (e *engine) Info() converge.JobInfo {
	settings := map[string]string{
		"concurrency": strconv.Itoa(e.cfg.concurrency),
	}
	if sched := scheduleSetting(e.cfg.triggers); sched != "" {
		settings["schedule"] = sched
	}
	if trig := triggersSetting(e.cfg.triggers); trig != "" {
		settings["triggers"] = trig
	}
	if v := versionsSetting(e.cfg.versions); v != "" {
		settings["versions"] = v
	}
	return converge.JobInfo{
		Job:      e.cfg.name,
		Surface:  converge.SurfaceReconcile,
		RunMode:  e.cfg.runMode,
		Settings: settings,
	}
}

func triggerLabel(t Trigger) string {
	switch tr := t.(type) {
	case *scheduleTrigger:
		return "schedule"
	case *messageTrigger:
		return "on-message " + tr.queue
	default:
		return "custom"
	}
}

func triggersSetting(triggers []Trigger) string {
	labels := make([]string, 0, len(triggers))
	for _, t := range triggers {
		labels = append(labels, triggerLabel(t))
	}
	return strings.Join(labels, " + ")
}

func scheduleSetting(triggers []Trigger) string {
	var rendered []string
	for _, t := range triggers {
		if st, ok := t.(*scheduleTrigger); ok {
			rendered = append(rendered, st.cad.render())
		}
	}
	return strings.Join(rendered, " + ")
}

func versionsSetting(v VersionSource) string {
	if v == nil {
		return ""
	}
	return "custom"
}

func (e *engine) hint(ctx context.Context, id ID) {
	e.hintVia(ctx, e.wakeQueueRef(), id)
}

func (e *engine) hintVia(ctx context.Context, q *wakeQueue, id ID) {
	if id == "" && !e.cfg.single {
		e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, Reason: converge.DiscardEmptyID})
		return
	}
	if q == nil {
		return
	}
	e.report(id, q.wake(id, wakeHint))
}

func (e *engine) report(id ID, res wakeResult) {
	if res != wakeDroppedOverflow {
		return
	}
	e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, ID: string(id), Reason: converge.DiscardOverflow})
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
	return e.cfg.fn(ctx, id)
}

func (e *engine) settle(hctx context.Context, id ID, err error, took time.Duration, snap versionSnapshot) {
	var (
		kind  finishKind
		delay time.Duration
	)
	s, isSig := sig.FromError(err)
	switch {
	case err == nil:
		kind = finishSuccess
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
	if res.fallback {
		e.deps.Observer.Observe(converge.BackoffFallback{Job: e.cfg.name, ID: string(id), Consecutive: backoff.NoBackoffCap + 1})
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
	case finishFailure:
		e.consecFails++
	}
}

func (e *engine) key(parts ...string) string {
	return keys.Reconcile(e.deps.Namespace, e.cfg.name, parts...)
}

func (e *engine) Run(ctx context.Context, deps converge.JobDeps) error {
	if err := e.bind(deps); err != nil {
		return err
	}
	defer func() {
		e.opsInFlight.Wait()
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
	name := keys.ReconcileLease(e.deps.Namespace, e.cfg.name)
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
	e.mu.Lock()
	e.active = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.active = false
		e.mu.Unlock()
	}()

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
