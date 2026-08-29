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
	"github.com/GareArc/converge/internal/clockctx"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/mw"
	"github.com/GareArc/converge/internal/sig"
)

const (
	backoffMin = time.Second
	backoffMax = 15 * time.Minute
)

type config struct {
	job         Job
	fn          func(ctx context.Context, id ID) error
	triggers    []Trigger
	concurrency int
	runMode     converge.RunMode
	timeout     time.Duration
	versions    VersionSource
	middleware  []converge.Middleware
	single      bool
	until       converge.StopCondition
}

type engine struct {
	cfg       config
	deps      converge.JobDeps
	handler   converge.Handler
	ready     chan struct{}
	readyOnce sync.Once

	mu             sync.Mutex
	queue          *idQueue
	lastSuccess    time.Time
	lastErr        error
	lastErrAt      time.Time
	consecFails    int
	passes         int
	active         bool
	leaseHeld      bool
	backlog        int
	backlogKnown   bool
	backlogAt      time.Time
	state          converge.State
	sweepsInFlight sync.WaitGroup

	stopCh      chan struct{}
	destroyOnce sync.Once
}

func (e *engine) Name() string { return e.cfg.job.Name() }

func (e *engine) Ready() <-chan struct{} { return e.ready }

func (e *engine) markReady() { e.readyOnce.Do(func() { close(e.ready) }) }

func (e *engine) bindCore(deps converge.JobDeps) error {
	e.deps = deps
	mws := append(slices.Clone(deps.Middleware), e.cfg.middleware...)
	final := func(ctx context.Context, r converge.Run) error {
		return e.invoke(ctx, ID(r.ID))
	}
	e.handler = mw.Chain(mws, final)
	curve := backoff.Curve{Min: backoffMin, Max: backoffMax}
	e.mu.Lock()
	e.queue = newIDQueue(deps.Clock, idQueuePolicy{
		backoff: curve.Delay,
		floor:   backoff.Floor,
	})
	e.mu.Unlock()
	e.stopCh = make(chan struct{})
	return nil
}

func (e *engine) idQueueRef() *idQueue {
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

func (e *engine) Notify(id string) error {
	q := e.idQueueRef()
	if q == nil {
		return fmt.Errorf("reconcile: job %q is not running", e.cfg.job.Name())
	}
	if id == "" && !e.cfg.single {
		return fmt.Errorf("reconcile: job %q: notify needs an id", e.cfg.job.Name())
	}
	if e.cfg.single {
		id = ""
	}
	e.notifyVia(context.Background(), q, ID(id), causeNotification)
	return nil
}

func (e *engine) admitSweep() (*idQueue, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.active {
		return nil, false
	}
	e.sweepsInFlight.Add(1)
	return e.queue, true
}

func (e *engine) isActive() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active
}

func (e *engine) Sweep(ctx context.Context) error {
	q, ok := e.admitSweep()
	if !ok {
		return fmt.Errorf("reconcile: job %q: sweep needs the engine to be active", e.cfg.job.Name())
	}
	defer e.sweepsInFlight.Done()
	found := false
	for idx, t := range e.cfg.triggers {
		st, ok := t.(*scheduleTrigger)
		if !ok {
			continue
		}
		if !e.isActive() {
			return fmt.Errorf("reconcile: job %q: sweep needs the engine to be active", e.cfg.job.Name())
		}
		found = true
		cursorKey := e.key("sweep", strconv.Itoa(idx))
		e.deleteKey(ctx, cursorKey)
		if !e.runPass(ctx, q, st, cursorKey) {
			return ctx.Err()
		}
	}
	if !found {
		return fmt.Errorf("reconcile: job %q: sweep needs a Schedule trigger", e.cfg.job.Name())
	}
	return nil
}

func (e *engine) Stats() converge.JobStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := converge.JobStats{
		Job:              e.cfg.job.Name(),
		Surface:          converge.SurfaceReconcile,
		RunMode:          e.cfg.runMode,
		State:            e.state,
		LeaseHeld:        e.leaseHeld,
		Backlog:          e.backlog,
		BacklogKnown:     e.backlogKnown,
		BacklogAt:        e.backlogAt,
		LastSuccess:      e.lastSuccess,
		LastError:        e.lastErr,
		LastErrorAt:      e.lastErrAt,
		ConsecutiveFails: e.consecFails,
	}
	if e.queue != nil {
		c := e.queue.counts()
		s.InFlight = c.inFlight
		s.Failing = c.failing
	}
	return s
}

func (e *engine) FailingIDs() []converge.FailingID {
	e.mu.Lock()
	q := e.queue
	e.mu.Unlock()
	if q == nil {
		return nil
	}
	return q.failing()
}

func (e *engine) setLeaseHeld(held bool) {
	e.mu.Lock()
	e.leaseHeld = held
	e.mu.Unlock()
}

func (e *engine) Info() converge.JobInfo {
	settings := map[string]string{
		"concurrency": strconv.Itoa(e.cfg.concurrency),
	}
	if sched := scheduleSetting(e.cfg.triggers); sched != "" {
		settings["schedule"] = sched
	}
	if trig := e.triggersSetting(); trig != "" {
		settings["triggers"] = trig
	}
	if v := versionsSetting(e.cfg.versions); v != "" {
		settings["versions"] = v
	}
	return converge.JobInfo{
		Job:      e.cfg.job.Name(),
		Surface:  converge.SurfaceReconcile,
		RunMode:  e.cfg.runMode,
		Settings: settings,
	}
}

func (e *engine) triggerLabel(t Trigger) string {
	switch tr := t.(type) {
	case *scheduleTrigger:
		return "schedule"
	case *notificationTrigger:
		e.mu.Lock()
		source := tr.source
		e.mu.Unlock()
		if tr.foreign {
			return "notifications-from " + source
		}
		return "notifications"
	default:
		return "custom"
	}
}

func (e *engine) triggersSetting() string {
	labels := make([]string, 0, len(e.cfg.triggers))
	for _, t := range e.cfg.triggers {
		labels = append(labels, e.triggerLabel(t))
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

func (e *engine) notify(ctx context.Context, id ID) {
	e.notifyVia(ctx, e.idQueueRef(), id, causeSweep)
}

func (e *engine) notifyVia(ctx context.Context, q *idQueue, id ID, class queueCause) {
	if id == "" && !e.cfg.single {
		e.deps.Observer.Observe(converge.NotificationDropped{Job: e.cfg.job.Name(), Err: converge.ErrNotificationEmptyID})
		return
	}
	if q == nil {
		return
	}
	e.report(id, q.add(id, class))
}

func (e *engine) report(id ID, res queueResult) {
	if res != resultDroppedOverflow {
		return
	}
	e.deps.Observer.Observe(converge.NotificationDropped{Job: e.cfg.job.Name(), ID: string(id), Err: converge.ErrInboxOverflow})
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

func (e *engine) versionAdvanced(ctx context.Context, id ID, snap versionSnapshot) bool {
	if !snap.known {
		return false
	}
	v, err := e.cfg.versions.Latest(ctx, id)
	if err != nil {
		return false
	}
	return v > snap.v
}

func (e *engine) runOne(hctx context.Context, id ID) {
	start := e.deps.Clock.Now()
	snap := e.preRunVersion(hctx, id)
	run := converge.Run{Job: e.cfg.job.Name(), Surface: converge.SurfaceReconcile, ID: string(id)}
	runCtx := hctx
	if e.cfg.timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = clockctx.WithTimeout(hctx, e.deps.Clock, e.cfg.timeout)
		defer cancel()
	}
	err := e.invokeChain(runCtx, run)
	e.settle(hctx, id, err, e.deps.Clock.Now().Sub(start), snap)
}

func (e *engine) invokeChain(ctx context.Context, run converge.Run) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicErr(e.cfg.job.Name(), r)
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
			err = panicErr(e.cfg.job.Name(), r)
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
	case err == nil && e.versionAdvanced(hctx, id, snap):
		kind = finishDelay
	case err == nil:
		kind = finishSuccess
	case isSig && isDeferralSignal(s):
		kind = finishDelay
		if d, ok := checkAgainDelay(s); ok {
			delay = d
		}
	case isSig:
		kind = finishFailure
	case hctx.Err() != nil:
		kind = finishNeutral
	default:
		kind = finishFailure
	}
	res := e.queue.finish(id, kind, delay, err)
	if !res.settled {
		return
	}
	if kind == finishNeutral {
		return
	}
	e.record(kind, err)
	outcome, oerr := converge.Retrying, err
	switch {
	case err == nil:
		outcome, oerr = converge.Succeeded, nil
	case isSig && isDeferralSignal(s):
		outcome, oerr = converge.Deferred, nil
	}
	e.deps.Observer.Observe(converge.RunCompleted{
		Job:      e.cfg.job.Name(),
		ID:       string(id),
		Attempt:  res.attempt,
		Duration: took,
		Outcome:  outcome,
		Err:      oerr,
	})
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

func isDeferralSignal(s sig.Signal) bool {
	if _, ok := checkAgainDelay(s); ok {
		return true
	}
	return errors.Is(s, ErrOutdated)
}

func (e *engine) record(kind finishKind, err error) {
	now := e.deps.Clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	switch kind {
	case finishSuccess, finishDelay:
		e.lastSuccess = now
		e.consecFails = 0
	case finishFailure:
		e.consecFails++
		e.lastErr = err
		e.lastErrAt = now
	}
}

func (e *engine) key(parts ...string) string {
	return keys.Reconcile(e.deps.Namespace, e.cfg.job.Name(), parts...)
}

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
		e.deps.KV.Set(ctx, keys.Tombstone(e.deps.Namespace, e.cfg.job.Name()), []byte("1"), 0)
		return e.cfg.until, true
	}
	key := keys.Tombstone(e.deps.Namespace, e.cfg.job.Name())
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
		e.deps.Observer.Observe(converge.JobDestroyed{Job: e.cfg.job.Name(), Cause: cause})
	}
	return true
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
	defer func() {
		e.sweepsInFlight.Wait()
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
		return fmt.Errorf("reconcile: job %q: OnOneReplica needs Options.Lease", e.cfg.job.Name())
	}
	if !e.cfg.until.IsZero() && deps.KV == nil {
		return fmt.Errorf("reconcile: job %q: Until needs Options.KV", e.cfg.job.Name())
	}
	for _, t := range e.cfg.triggers {
		if tr, ok := t.(*notificationTrigger); ok {
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
	name := keys.ReconcileLease(e.deps.Namespace, e.cfg.job.Name())
	retry := e.leaseInterval()
	e.markReady()
	for {
		if e.checkDestroy(ctx) {
			return nil
		}
		h, ok, err := e.deps.Lease.TryAcquire(ctx, name, e.deps.LeaseTTL)
		e.markReady()
		if err == nil && ok {
			e.setLeaseHeld(true)
			e.deps.Observer.Observe(converge.LeaseChanged{Job: e.cfg.job.Name(), Held: true})
			e.runActive(ctx, h)
			e.setLeaseHeld(false)
			e.deps.Observer.Observe(converge.LeaseChanged{Job: e.cfg.job.Name(), Held: false})
			if ctx.Err() != nil {
				return nil
			}
			if e.isDestroyed() {
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

	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-e.stopCh:
			stopIntake()
			stopHandlers()
		case <-stopWatch:
		}
	}()

	hbStop := make(chan struct{})
	var hb sync.WaitGroup
	if h != nil {
		hb.Add(1)
		go func() {
			defer hb.Done()
			e.heartbeat(ctx, h, hbStop, stopIntake, stopHandlers)
		}()
	}

	backlogStop := make(chan struct{})
	defer close(backlogStop)
	go e.pollBacklog(ctx, backlogStop)

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

func (e *engine) backlogReaders() []func(context.Context) (int, error) {
	var out []func(context.Context) (int, error)
	for _, t := range e.cfg.triggers {
		tr, ok := t.(*notificationTrigger)
		if !ok {
			continue
		}
		read := e.triggerBacklogReader(tr)
		if read == nil {
			return nil
		}
		out = append(out, read)
	}
	return out
}

func (e *engine) triggerBacklogReader(t *notificationTrigger) func(context.Context) (int, error) {
	if t.mq == nil {
		return nil
	}
	switch e.cfg.runMode {
	case converge.OnAllReplicas:
		return nil
	case converge.OnOneReplica:
		br, ok := t.mq.(converge.BacklogReporter)
		if !ok {
			return nil
		}
		return func(ctx context.Context) (int, error) { return br.Backlog(ctx, t.source) }
	default:
		gr, ok := t.mq.(converge.GroupBacklogReporter)
		if !ok {
			return nil
		}
		group := e.key("notifications")
		return func(ctx context.Context) (int, error) { return gr.BacklogForGroup(ctx, t.source, group) }
	}
}

func (e *engine) pollBacklog(ctx context.Context, stop <-chan struct{}) {
	defer e.forgetBacklog()
	readers := e.backlogReaders()
	if len(readers) == 0 {
		return
	}
	interval := e.leaseInterval()
	e.refreshBacklog(ctx, readers, interval)
	for {
		select {
		case <-stop:
			return
		case <-e.deps.Clock.After(interval):
			e.refreshBacklog(ctx, readers, interval)
		}
	}
}

func (e *engine) refreshBacklog(ctx context.Context, readers []func(context.Context) (int, error), timeout time.Duration) {
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	total := 0
	for _, read := range readers {
		n, err := read(bctx)
		if err != nil {
			return
		}
		total += n
	}
	now := e.deps.Clock.Now()
	e.mu.Lock()
	e.backlog = total
	e.backlogKnown = true
	e.backlogAt = now
	e.mu.Unlock()
}

func (e *engine) forgetBacklog() {
	e.mu.Lock()
	e.backlogKnown = false
	e.backlogAt = time.Time{}
	e.mu.Unlock()
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
			if e.checkDestroy(ctx) {
				return
			}
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
