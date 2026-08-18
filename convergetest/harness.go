package convergetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/hook"
)

var epoch = time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

const (
	readyDeadline   = 2 * time.Second
	stopDeadline    = 2 * time.Second
	stopAdvance     = 10 * time.Second
	stopPoll        = 2 * time.Millisecond
	drainDeadline   = 10 * time.Second
	drainPoll       = 2 * time.Millisecond
	drainGap        = 5 * time.Millisecond
	runPassDeadline = 5 * time.Second
	runPassPoll     = 2 * time.Millisecond
)

type Harness struct {
	Clock *Clock
	MQ    *MQ
	KV    *inmem.KV
	Lease *Lease

	t   testing.TB
	rec *Recorder

	mu       sync.Mutex
	rt       *converge.Runtime
	attached bool
	started  bool
	starting chan struct{}
	done     chan struct{}
	runErr   error
}

func New(t testing.TB) *Harness {
	t.Helper()
	clock := NewClock(epoch)
	return &Harness{
		Clock: clock,
		MQ:    WrapMQ(inmem.NewMQWithClock(clock)),
		KV:    inmem.NewKVWithClock(clock),
		Lease: WrapLease(inmem.NewLeaseWithClock(clock)),
		t:     t,
		rec:   &Recorder{},
		done:  make(chan struct{}),
	}
}

func (h *Harness) Options() converge.Options {
	h.t.Helper()
	o := converge.Options{
		Namespace: "test",
		MQ:        h.MQ,
		Lease:     h.Lease,
		KV:        h.KV,
		Observer:  h.rec,
		Clock:     h.Clock,
	}
	out := hook.AttachOptions(o, h.attach)
	opts, ok := out.(converge.Options)
	if !ok {
		h.t.Fatalf("convergetest: internal: AttachOptions returned %T, want converge.Options", out)
		return o
	}
	return opts
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
	t.Helper()
	h.mu.Lock()
	if !h.attached {
		h.mu.Unlock()
		t.Fatalf("convergetest: no runtime attached; call converge.New(h.Options()) before using the Harness")
		return false
	}
	if h.started {
		starting := h.starting
		rt := h.rt
		h.mu.Unlock()
		<-starting
		return h.checkAlive(t, rt)
	}
	h.started = true
	starting := make(chan struct{})
	h.starting = starting
	rt := h.rt
	ctx, cancel := context.WithCancel(context.Background())
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
		h.awaitStop(h.t)
	})

	return h.checkAlive(t, rt)
}

func (h *Harness) checkAlive(t testing.TB, rt *converge.Runtime) bool {
	t.Helper()
	select {
	case <-h.done:
		h.mu.Lock()
		err := h.runErr
		h.mu.Unlock()
		t.Fatalf("convergetest: runtime exited early: %v", err)
		return false
	default:
		return true
	}
}

func (h *Harness) awaitStop(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(stopDeadline)
	for {
		select {
		case <-h.done:
			h.mu.Lock()
			err := h.runErr
			h.mu.Unlock()
			if err != nil {
				t.Fatalf("convergetest: Run returned %v", err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: Run never returned")
			return
		}
		h.Clock.Advance(stopAdvance)
		time.Sleep(stopPoll)
	}
}

func (h *Harness) runtime(t testing.TB) *converge.Runtime {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rt
}

func (h *Harness) Wake(job, id string) {
	h.t.Helper()
	if !h.ensureRunning(h.t) {
		return
	}
	rt := h.runtime(h.t)
	if err := hook.Hint(rt, job, id); err != nil {
		h.t.Fatalf("convergetest: Wake(%q, %q): %v", job, id, err)
	}
}

func (h *Harness) Drain(t testing.TB) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	rt := h.runtime(t)
	deadline := time.Now().Add(drainDeadline)
	for {
		if h.quiet(rt) {
			time.Sleep(drainGap)
			if h.quiet(rt) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: Drain: never quiet after %s; stats=%+v", drainDeadline, rt.Stats())
			return
		}
		time.Sleep(drainPoll)
	}
}

func (h *Harness) quiet(rt *converge.Runtime) bool {
	return h.MQ.Idle() && hook.Quiet(rt)
}

func (h *Harness) RunPass(t testing.TB, job string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	rt := h.runtime(t)
	ctx := context.Background()
	deadline := time.Now().Add(runPassDeadline)
	var lastErr error
	for {
		err := hook.RunPassNow(rt, ctx, job)
		if err == nil {
			h.Drain(t)
			return
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: RunPass(%q): %v", job, lastErr)
			return
		}
		time.Sleep(runPassPoll)
	}
}

func (h *Harness) Events() []converge.Event {
	h.t.Helper()
	if !h.ensureRunning(h.t) {
		return nil
	}
	return h.rec.Events()
}
