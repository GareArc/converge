package convergetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/wiring"
)

var epoch = time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

const (
	readyDeadline   = 2 * time.Second
	stopDeadline    = 2 * time.Second
	stopAdvance     = 10 * time.Second
	drainDeadline   = 10 * time.Second
	drainGap        = 5 * time.Millisecond
	sweepDeadline   = 5 * time.Second
	harnessLeaseTTL = 24 * 365 * time.Hour
)

type Options struct {
	Namespace    string
	LeaseTTL     time.Duration
	DrainTimeout time.Duration
	Clock        *Clock
	MQ           func(*Clock) converge.MQ
	KV           func(*Clock) converge.KV
	Lease        func(*Clock) converge.Lease
}

type Harness struct {
	MQ    *MQ
	KV    *inmem.KV
	Lease *inmem.Lease

	clock *Clock

	t   testing.TB
	rec *Recorder

	namespace    string
	leaseTTL     time.Duration
	drainTimeout time.Duration
	mq           converge.MQ
	kv           converge.KV
	lease        converge.Lease

	mu       sync.Mutex
	rt       *converge.Runtime
	attached bool
	started  bool
	starting chan struct{}
	done     chan struct{}
	runErr   error
	cancel   context.CancelFunc
	settled  bool
}

func New(t testing.TB) *Harness {
	t.Helper()
	return NewWith(t, Options{})
}

func NewWith(t testing.TB, o Options) *Harness {
	t.Helper()
	namespace := o.Namespace
	if namespace == "" {
		namespace = "test"
	}
	leaseTTL := o.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = harnessLeaseTTL
	}
	clock := o.Clock
	if clock == nil {
		clock = NewClock(epoch)
	}

	h := &Harness{
		clock:        clock,
		t:            t,
		rec:          &Recorder{},
		done:         make(chan struct{}),
		namespace:    namespace,
		leaseTTL:     leaseTTL,
		drainTimeout: o.DrainTimeout,
	}

	if o.MQ != nil {
		h.mq = o.MQ(clock)
	} else {
		mq := WrapMQ(inmem.NewMQWithClock(clock))
		h.MQ = mq
		h.mq = mq
	}

	if o.KV != nil {
		h.kv = o.KV(clock)
	} else {
		kv := inmem.NewKVWithClock(clock)
		h.KV = kv
		h.kv = kv
	}

	if o.Lease != nil {
		h.lease = o.Lease(clock)
	} else {
		lease := inmem.NewLeaseWithClock(clock)
		h.Lease = lease
		h.lease = lease
	}

	return h
}

func (h *Harness) Options() converge.Options {
	h.t.Helper()
	o := converge.Options{
		Namespace:    h.namespace,
		MQ:           h.mq,
		Lease:        h.lease,
		KV:           h.kv,
		Observer:     h.rec,
		Clock:        h.clock,
		LeaseTTL:     h.leaseTTL,
		DrainTimeout: h.drainTimeout,
	}
	opts, err := wiring.Attach(o, h.attach)
	if err != nil {
		h.t.Fatalf("convergetest: internal: %v", err)
		return o
	}
	return opts
}

func (h *Harness) Build(t testing.TB) *converge.Runtime {
	t.Helper()
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatalf("convergetest: converge.New(h.Options()): %v", err)
		return nil
	}
	return rt
}

func (h *Harness) attach(rt any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.attached {
		h.t.Fatalf("convergetest: converge.New(h.Options()) called twice on the same Harness; one runtime per Harness")
		return
	}
	r, ok := rt.(*converge.Runtime)
	if !ok || r == nil {
		h.t.Fatalf("convergetest: attach: %T is not a usable *converge.Runtime", rt)
		return
	}
	h.rt = r
	h.attached = true
}

func (h *Harness) ensureRunning(t testing.TB) bool {
	return h.ensureRunningState(t, false)
}

func (h *Harness) ensureRunningReadOnly(t testing.TB) bool {
	return h.ensureRunningState(t, true)
}

func (h *Harness) ensureRunningState(t testing.TB, allowStopped bool) bool {
	t.Helper()
	h.mu.Lock()
	if !h.attached {
		h.mu.Unlock()
		t.Fatalf("convergetest: no runtime attached; call h.Build(t) before using the Harness")
		return false
	}
	if h.started {
		starting := h.starting
		h.mu.Unlock()
		<-starting
		return h.checkAlive(t, allowStopped)
	}
	h.started = true
	starting := make(chan struct{})
	h.starting = starting
	rt := h.rt
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.mu.Unlock()
	defer close(starting)

	go func() {
		err := rt.Run(ctx)
		h.mu.Lock()
		h.runErr = err
		h.mu.Unlock()
		close(h.done)
	}()

	select {
	case <-rt.Ready():
	case <-time.After(readyDeadline):
		cancel()
		t.Fatalf("convergetest: runtime never became ready within %s", readyDeadline)
		return false
	}

	h.t.Cleanup(func() {
		cancel()
		h.mu.Lock()
		settled := h.settled
		h.mu.Unlock()
		if settled {
			return
		}
		h.awaitStop(h.t)
	})

	return h.checkAlive(t, allowStopped)
}

func (h *Harness) checkAlive(t testing.TB, allowStopped bool) bool {
	t.Helper()
	select {
	case <-h.done:
		h.mu.Lock()
		err := h.runErr
		settled := h.settled
		h.mu.Unlock()
		if settled {
			if allowStopped {
				return true
			}
			if err != nil {
				t.Fatalf("convergetest: harness was stopped via Stop(t) (which returned %v); this verb needs a running runtime, call Events to inspect recorded state instead", err)
				return false
			}
			t.Fatalf("convergetest: harness was stopped via Stop(t); this verb needs a running runtime, call Events to inspect recorded state instead")
			return false
		}
		if err == nil && h.anyDestroyed() {
			return true
		}
		t.Fatalf("convergetest: runtime exited early: %v", err)
		return false
	default:
		return true
	}
}

func (h *Harness) anyDestroyed() bool {
	h.mu.Lock()
	rt := h.rt
	h.mu.Unlock()
	if rt == nil {
		return false
	}
	for _, s := range rt.Stats() {
		if s.State == converge.Destroyed {
			return true
		}
	}
	return false
}

func (h *Harness) waitForStop(t testing.TB) error {
	t.Helper()
	ok := pollUntil(t, pollSpec{
		deadline: stopDeadline,
		step:     pollStep,
		advance:  func() { h.clock.Advance(stopAdvance) },
		fail:     func() { t.Helper(); t.Fatalf("convergetest: Run never returned") },
	}, func() bool {
		select {
		case <-h.done:
			return true
		default:
			return false
		}
	})
	if !ok {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runErr
}

func (h *Harness) awaitStop(t testing.TB) {
	t.Helper()
	if err := h.waitForStop(t); err != nil {
		t.Fatalf("convergetest: Run returned %v", err)
	}
}

func (h *Harness) Runtime(t testing.TB) *converge.Runtime {
	t.Helper()
	if !h.ensureRunning(t) {
		return nil
	}
	return h.runtime(t)
}

func (h *Harness) Stop(t testing.TB) error {
	t.Helper()
	if !h.ensureRunning(t) {
		return nil
	}
	h.mu.Lock()
	cancel := h.cancel
	h.settled = true
	h.mu.Unlock()
	cancel()
	return h.waitForStop(t)
}

func (h *Harness) runtime(t testing.TB) *converge.Runtime {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rt
}

func (h *Harness) Clock() *Clock { return h.clock }

func (h *Harness) Notify(job, id string) {
	h.t.Helper()
	if !h.ensureRunning(h.t) {
		return
	}
	rt := h.runtime(h.t)
	if err := hook.Notify(rt, job, id); err != nil {
		h.t.Fatalf("convergetest: Notify(%q, %q): %v", job, id, err)
	}
}

func (h *Harness) Drain(t testing.TB) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	rt := h.runtime(t)
	pollUntil(t, pollSpec{
		deadline: drainDeadline,
		step:     pollStep,
		fail: func() {
			t.Helper()
			t.Fatalf("convergetest: Drain: never quiet after %s; stats=%+v", drainDeadline, rt.Stats())
		},
	}, func() bool {
		if !h.quiet(rt) {
			return false
		}
		time.Sleep(drainGap)
		return h.quiet(rt)
	})
}

func (h *Harness) quiet(rt *converge.Runtime) bool {
	if h.MQ == nil {
		return hook.Quiet(rt)
	}
	return h.MQ.Idle() && hook.Quiet(rt)
}

func (h *Harness) Sweep(t testing.TB, job string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	rt := h.runtime(t)
	ctx := context.Background()
	var lastErr error
	ok := pollUntil(t, pollSpec{
		deadline: sweepDeadline,
		step:     pollStep,
		fail: func() {
			t.Helper()
			t.Fatalf("convergetest: Sweep(%q): %v", job, lastErr)
		},
	}, func() bool {
		err := hook.Sweep(rt, ctx, job)
		if err != nil {
			lastErr = err
			return false
		}
		return true
	})
	if ok {
		h.Drain(t)
	}
}

func (h *Harness) Events() []converge.Event {
	h.t.Helper()
	if !h.ensureRunningReadOnly(h.t) {
		return nil
	}
	return h.rec.Events()
}
