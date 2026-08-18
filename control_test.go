package converge_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/ctl"
	"github.com/GareArc/converge/internal/hook"
)

type errStubJob struct {
	name  string
	ready chan struct{}

	mu      sync.Mutex
	poked   []string
	hinted  []string
	ranPass int
	paused  []bool

	pokeErr    error
	hintErr    error
	runPassErr error

	pokeGate  chan struct{}
	pauseGate chan struct{}
}

func newErrStubJob(name string) *errStubJob {
	return &errStubJob{name: name, ready: make(chan struct{})}
}

func (s *errStubJob) Name() string { return s.name }

func (s *errStubJob) Run(ctx context.Context, d converge.JobDeps) error {
	close(s.ready)
	<-ctx.Done()
	return nil
}

func (s *errStubJob) Ready() <-chan struct{} { return s.ready }

func (s *errStubJob) Poke(id string) error {
	if s.pokeGate != nil {
		<-s.pokeGate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poked = append(s.poked, id)
	return s.pokeErr
}

func (s *errStubJob) Quiet() bool { return true }

func (s *errStubJob) Hint(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hinted = append(s.hinted, id)
	return s.hintErr
}

func (s *errStubJob) RunPassNow(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranPass++
	return s.runPassErr
}

func (s *errStubJob) SetPaused(paused bool) {
	if paused && s.pauseGate != nil {
		<-s.pauseGate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = append(s.paused, paused)
}

func (s *errStubJob) Stats() converge.JobStats { return converge.JobStats{Job: s.name} }

func (s *errStubJob) Info() converge.JobInfo { return converge.JobInfo{Job: s.name} }

func (s *errStubJob) pokeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.poked)
}

type errKV struct{ err error }

func (k errKV) Get(context.Context, string) ([]byte, bool, error) { return nil, false, k.err }
func (k errKV) SetCAS(context.Context, string, []byte, []byte) (bool, error) {
	return false, nil
}
func (k errKV) Set(context.Context, string, []byte, time.Duration) error { return nil }
func (k errKV) Delete(context.Context, string) error                     { return nil }
func (k errKV) Scan(context.Context, string, string) ([]string, string, error) {
	return nil, "", nil
}

type selectiveFailKV struct {
	converge.KV
	failPrefix string
	failErr    error
}

func (k selectiveFailKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if strings.HasPrefix(key, k.failPrefix) {
		return k.failErr
	}
	return k.KV.Set(ctx, key, val, ttl)
}

func stubLastPaused(s *stubJob) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.paused) == 0 {
		return false, false
	}
	return s.paused[len(s.paused)-1], true
}

func newCtlRuntime(t *testing.T, ns string, mq converge.MQ, kv converge.KV, clock converge.Clock) *converge.Runtime {
	t.Helper()
	rt, err := converge.New(converge.Options{Namespace: ns, MQ: mq, KV: kv, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func startAndAwaitReady(t *testing.T, rt *converge.Runtime, ctx context.Context) {
	t.Helper()
	go rt.Run(ctx)
	select {
	case <-rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never became ready")
	}
}

type ctlDispatchResult struct {
	resp []ctl.Response
	err  error
}

const controlTestPollStep = 60 * time.Millisecond

func startControlDispatch(rt *converge.Runtime, req ctl.Request) <-chan ctlDispatchResult {
	done := make(chan ctlDispatchResult, 1)
	go func() {
		resp, err := hook.ControlDispatch(rt, context.Background(), req)
		done <- ctlDispatchResult{resp, err}
	}()
	return done
}

func awaitControlDispatchDone(t *testing.T, clock *convergetest.Clock, done <-chan ctlDispatchResult) ctlDispatchResult {
	t.Helper()
	var result ctlDispatchResult
	convergetest.AdvanceUntil(t, clock, controlTestPollStep, func() bool {
		select {
		case result = <-done:
			return true
		default:
			return false
		}
	})
	return result
}

func assertControlDispatchPending(t *testing.T, clock *convergetest.Clock, done <-chan ctlDispatchResult, ticks int) {
	t.Helper()
	for i := 0; i < ticks; i++ {
		select {
		case r := <-done:
			t.Fatalf("control dispatch returned early: %+v", r)
		default:
		}
		clock.Advance(controlTestPollStep)
		time.Sleep(2 * time.Millisecond)
	}
}

func awaitControlDispatch(t *testing.T, clock *convergetest.Clock, rt *converge.Runtime, req ctl.Request) ([]ctl.Response, time.Duration, error) {
	t.Helper()
	start := clock.Now()
	result := awaitControlDispatchDone(t, clock, startControlDispatch(rt, req))
	return result.resp, clock.Now().Sub(start), result.err
}

func collectAllControlResponses(t *testing.T, kv converge.KV, ns string) []ctl.Response {
	t.Helper()
	prefix := ctl.ResPrefix(ns, "")
	var out []ctl.Response
	cursor := ""
	for {
		keys, next, err := kv.Scan(context.Background(), prefix, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			raw, ok, err := kv.Get(context.Background(), key)
			if err != nil || !ok {
				continue
			}
			var resp ctl.Response
			if err := json.Unmarshal(raw, &resp); err != nil {
				continue
			}
			out = append(out, resp)
		}
		if next == "" {
			return out
		}
		cursor = next
	}
}

func controlResponseFor(t *testing.T, kv converge.KV, ns, replica string) *ctl.Response {
	t.Helper()
	for _, r := range collectAllControlResponses(t, kv, ns) {
		if r.Replica == replica {
			return &r
		}
	}
	return nil
}

func TestControlDispatchLocalFallbackPoke(t *testing.T) {
	rt := mustRuntime(t)
	s := newStubJob("a")
	if err := hook.RegisterJob(rt, s); err != nil {
		t.Fatal(err)
	}

	resp, err := hook.ControlDispatch(rt, context.Background(), ctl.Request{Op: ctl.OpPoke, Job: "a", ID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 1 || !resp[0].Acted {
		t.Fatalf("resp = %+v, want one acted response", resp)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.poked) != 1 || s.poked[0] != "x" {
		t.Fatalf("poked = %v, want [x]", s.poked)
	}
}

func TestControlDispatchLocalFallbackUnknownJob(t *testing.T) {
	rt := mustRuntime(t)

	resp, err := hook.ControlDispatch(rt, context.Background(), ctl.Request{Op: ctl.OpPoke, Job: "missing", ID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 1 || resp[0].Acted || resp[0].Err == "" {
		t.Fatalf("resp = %+v, want a single unacted response carrying an error", resp)
	}
}

func TestControlDispatchLocalFallbackUnknownOp(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RegisterJob(rt, newStubJob("a")); err != nil {
		t.Fatal(err)
	}

	resp, err := hook.ControlDispatch(rt, context.Background(), ctl.Request{Op: "frobnicate", Job: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 1 || resp[0].Acted || !strings.Contains(resp[0].Err, "unknown") {
		t.Fatalf("resp = %+v, want a single unacted response mentioning unknown", resp)
	}
}

func TestControlDispatchBroadcastPauseAndResume(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rtA := newCtlRuntime(t, ns, mq, kv, clock)
	jobA := newStubJob("worker")
	if err := hook.RegisterJob(rtA, jobA); err != nil {
		t.Fatal(err)
	}
	rtB := newCtlRuntime(t, ns, mq, kv, clock)
	jobB := newStubJob("worker")
	if err := hook.RegisterJob(rtB, jobB); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rtA, ctx)
	startAndAwaitReady(t, rtB, ctx)

	wiringA, err := hook.OpsDeps(rtA)
	if err != nil {
		t.Fatal(err)
	}
	wiringB, err := hook.OpsDeps(rtB)
	if err != nil {
		t.Fatal(err)
	}

	resp, _, err := awaitControlDispatch(t, clock, rtA, ctl.Request{Op: ctl.OpPause, Job: "worker", Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 2 {
		t.Fatalf("resp = %+v, want responses from both replicas", resp)
	}
	seen := map[string]bool{}
	for _, r := range resp {
		if !r.Acted {
			t.Fatalf("resp = %+v, want every pause response acted", resp)
		}
		seen[r.Replica] = true
	}
	if !seen[wiringA.Replica] || !seen[wiringB.Replica] {
		t.Fatalf("resp = %+v, want replicas %q and %q", resp, wiringA.Replica, wiringB.Replica)
	}

	raw, ok, err := kv.Get(context.Background(), ctl.PausedKey(ns, "worker"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(raw) != "1" {
		t.Fatalf("paused flag = (%q, %v), want (1, true)", raw, ok)
	}

	convergetest.Await(t, func() bool {
		p, ok := stubLastPaused(jobA)
		return ok && p
	})
	convergetest.Await(t, func() bool {
		p, ok := stubLastPaused(jobB)
		return ok && p
	})

	resp, _, err = awaitControlDispatch(t, clock, rtA, ctl.Request{Op: ctl.OpResume, Job: "worker", Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 2 {
		t.Fatalf("resp = %+v, want responses from both replicas", resp)
	}

	if _, ok, err := kv.Get(context.Background(), ctl.PausedKey(ns, "worker")); err != nil || ok {
		t.Fatalf("paused flag ok=%v err=%v, want deleted", ok, err)
	}

	convergetest.Await(t, func() bool {
		p, ok := stubLastPaused(jobA)
		return ok && !p
	})
	convergetest.Await(t, func() bool {
		p, ok := stubLastPaused(jobB)
		return ok && !p
	})
}

func TestControlDispatchBroadcastPokeIgnoresErrorForEarlyReturn(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rtA := newCtlRuntime(t, ns, mq, kv, clock)
	good := newErrStubJob("worker")
	if err := hook.RegisterJob(rtA, good); err != nil {
		t.Fatal(err)
	}
	rtB := newCtlRuntime(t, ns, mq, kv, clock)
	bad := newErrStubJob("worker")
	bad.pokeErr = errors.New("poke boom")
	if err := hook.RegisterJob(rtB, bad); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rtA, ctx)
	startAndAwaitReady(t, rtB, ctx)

	resp, _, err := awaitControlDispatch(t, clock, rtA, ctl.Request{Op: ctl.OpPoke, Job: "worker", ID: "x", Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) == 0 || len(resp) > 2 {
		t.Fatalf("resp = %+v, want one or two responses", resp)
	}
	var sawActed bool
	for _, r := range resp {
		if r.Acted {
			sawActed = true
		}
	}
	if !sawActed {
		t.Fatalf("resp = %+v, want an acted=true response present", resp)
	}
	if len(resp) == 2 {
		var sawErr bool
		for _, r := range resp {
			if !r.Acted && r.Err != "" {
				sawErr = true
			}
		}
		if !sawErr {
			t.Fatalf("resp = %+v, want the erroring replica's response when both arrive", resp)
		}
	}

	convergetest.Await(t, func() bool { return good.pokeCount() == 1 })
	convergetest.Await(t, func() bool { return bad.pokeCount() == 1 })
}

func TestControlDispatchRunPassEarlyReturnBeforeDeadline(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rtA := newCtlRuntime(t, ns, mq, kv, clock)
	good := newErrStubJob("worker")
	if err := hook.RegisterJob(rtA, good); err != nil {
		t.Fatal(err)
	}
	rtB := newCtlRuntime(t, ns, mq, kv, clock)
	bad := newErrStubJob("worker")
	bad.runPassErr = errors.New("run-pass boom")
	if err := hook.RegisterJob(rtB, bad); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rtA, ctx)
	startAndAwaitReady(t, rtB, ctx)

	resp, elapsed, err := awaitControlDispatch(t, clock, rtA, ctl.Request{Op: ctl.OpRunPass, Job: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) == 0 || len(resp) > 2 {
		t.Fatalf("resp = %+v, want one or two responses", resp)
	}
	var sawActed bool
	for _, r := range resp {
		if r.Acted {
			sawActed = true
		}
	}
	if !sawActed {
		t.Fatalf("resp = %+v, want an acted response present", resp)
	}
	if elapsed >= time.Second {
		t.Fatalf("elapsed = %v, want early return well before the 2s default deadline", elapsed)
	}
}

func TestControlDispatchBroadcastNoListenersReturnsEmpty(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rt := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}

	resp, _, err := awaitControlDispatch(t, clock, rt, ctl.Request{Op: ctl.OpPoke, Job: "worker", ID: "x", Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || len(resp) != 0 {
		t.Fatalf("resp = %v, want a non-nil empty slice", resp)
	}
}

func TestControlDispatchPauseDurableAcrossNewRuntime(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rtA := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rtA, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	startAndAwaitReady(t, rtA, ctxA)

	if _, _, err := awaitControlDispatch(t, clock, rtA, ctl.Request{Op: ctl.OpPause, Job: "worker", Timeout: 100 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}

	rtC := newCtlRuntime(t, ns, mq, kv, clock)
	s := newStubJob("worker")
	if err := hook.RegisterJob(rtC, s); err != nil {
		t.Fatal(err)
	}
	ctxC, cancelC := context.WithCancel(context.Background())
	defer cancelC()
	startAndAwaitReady(t, rtC, ctxC)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.paused) != 1 || s.paused[0] != true {
		t.Fatalf("paused = %v, want [true] before the job started running", s.paused)
	}
}

func TestControlListenerSurvivesUndecodableCommand(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rt := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rt, ctx)

	if err := mq.Publish(context.Background(), ctl.Queue(ns), converge.Message{Payload: []byte("not-json")}); err != nil {
		t.Fatal(err)
	}

	resp, _, err := awaitControlDispatch(t, clock, rt, ctl.Request{Op: ctl.OpPoke, Job: "worker", ID: "x", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 1 || !resp[0].Acted {
		t.Fatalf("resp = %+v, want the listener to keep serving after garbage input", resp)
	}
}

func TestControlListenerUnknownOpRespondsVisibly(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rt := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rt, ctx)

	wiring, err := hook.OpsDeps(rt)
	if err != nil {
		t.Fatal(err)
	}

	opID := "1234567890abcdef"
	payload, err := json.Marshal(ctl.Command{Op: "frobnicate", Job: "worker", OpID: opID})
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(context.Background(), ctl.Queue(ns), converge.Message{Payload: payload}); err != nil {
		t.Fatal(err)
	}

	key := ctl.ResKey(ns, opID, wiring.Replica)
	var resp ctl.Response
	convergetest.Await(t, func() bool {
		raw, ok, err := kv.Get(context.Background(), key)
		if err != nil || !ok {
			return false
		}
		return json.Unmarshal(raw, &resp) == nil
	})
	if resp.Acted || !strings.Contains(resp.Err, "unknown") {
		t.Fatalf("resp = %+v, want an unacted response mentioning unknown", resp)
	}
}

func TestControlResponseExpiresAfterTTL(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rt := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rt, ctx)

	wiring, err := hook.OpsDeps(rt)
	if err != nil {
		t.Fatal(err)
	}

	opID := "aaaaaaaaaaaaaaaa"
	payload, err := json.Marshal(ctl.Command{Op: ctl.OpPoke, Job: "worker", ID: "x", OpID: opID})
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(context.Background(), ctl.Queue(ns), converge.Message{Payload: payload}); err != nil {
		t.Fatal(err)
	}

	key := ctl.ResKey(ns, opID, wiring.Replica)
	convergetest.Await(t, func() bool {
		_, ok, _ := kv.Get(context.Background(), key)
		return ok
	})

	clock.Advance(61 * time.Second)
	if _, ok, err := kv.Get(context.Background(), key); err != nil || ok {
		t.Fatalf("response key ok=%v err=%v, want expired", ok, err)
	}
}

func TestRunStartupPausedFlagReadErrorAbortsRun(t *testing.T) {
	boom := errors.New("kv down")
	rt, err := converge.New(converge.Options{KV: errKV{err: boom}})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}

	if err := rt.Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run() = %v, want %v", err, boom)
	}
}

func TestControlDispatchEarlyReturnRequiresActedTrue(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rtBad := newCtlRuntime(t, ns, mq, kv, clock)
	bad := newErrStubJob("worker")
	bad.pokeErr = errors.New("poke boom")
	if err := hook.RegisterJob(rtBad, bad); err != nil {
		t.Fatal(err)
	}

	rtGood := newCtlRuntime(t, ns, mq, kv, clock)
	good := newErrStubJob("worker")
	release := make(chan struct{})
	good.pokeGate = release
	if err := hook.RegisterJob(rtGood, good); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rtBad, ctx)
	startAndAwaitReady(t, rtGood, ctx)

	wiringBad, err := hook.OpsDeps(rtBad)
	if err != nil {
		t.Fatal(err)
	}
	wiringGood, err := hook.OpsDeps(rtGood)
	if err != nil {
		t.Fatal(err)
	}

	done := startControlDispatch(rtBad, ctl.Request{Op: ctl.OpPoke, Job: "worker", ID: "x"})

	convergetest.AdvanceUntil(t, clock, controlTestPollStep, func() bool {
		return controlResponseFor(t, kv, ns, wiringBad.Replica) != nil
	})
	if r := controlResponseFor(t, kv, ns, wiringBad.Replica); r == nil || r.Acted {
		t.Fatalf("bad replica response = %+v, want a written acted=false response", r)
	}

	assertControlDispatchPending(t, clock, done, 10)
	if controlResponseFor(t, kv, ns, wiringGood.Replica) != nil {
		t.Fatal("good replica responded before its gate was released")
	}

	close(release)

	result := awaitControlDispatchDone(t, clock, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	var sawGoodActed bool
	for _, r := range result.resp {
		if r.Replica == wiringGood.Replica && r.Acted {
			sawGoodActed = true
		}
	}
	if !sawGoodActed {
		t.Fatalf("resp = %+v, want the healthy replica's acted response", result.resp)
	}
}

func TestControlDispatchPauseWaitsForAllReplicas(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rtFast := newCtlRuntime(t, ns, mq, kv, clock)
	fast := newErrStubJob("worker")
	if err := hook.RegisterJob(rtFast, fast); err != nil {
		t.Fatal(err)
	}

	rtSlow := newCtlRuntime(t, ns, mq, kv, clock)
	slow := newErrStubJob("worker")
	release := make(chan struct{})
	slow.pauseGate = release
	if err := hook.RegisterJob(rtSlow, slow); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rtFast, ctx)
	startAndAwaitReady(t, rtSlow, ctx)

	wiringFast, err := hook.OpsDeps(rtFast)
	if err != nil {
		t.Fatal(err)
	}
	wiringSlow, err := hook.OpsDeps(rtSlow)
	if err != nil {
		t.Fatal(err)
	}

	done := startControlDispatch(rtFast, ctl.Request{Op: ctl.OpPause, Job: "worker", Timeout: 5 * time.Second})

	convergetest.AdvanceUntil(t, clock, controlTestPollStep, func() bool {
		return controlResponseFor(t, kv, ns, wiringFast.Replica) != nil
	})

	assertControlDispatchPending(t, clock, done, 10)
	if controlResponseFor(t, kv, ns, wiringSlow.Replica) != nil {
		t.Fatal("slow replica responded before its gate was released")
	}

	close(release)

	result := awaitControlDispatchDone(t, clock, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if len(result.resp) != 2 {
		t.Fatalf("resp = %+v, want both replicas once the slow one unblocks", result.resp)
	}
}

func TestControlDispatchRejectsForeignRuntime(t *testing.T) {
	if _, err := hook.ControlDispatch("not a runtime", context.Background(), ctl.Request{Op: ctl.OpPoke, Job: "a"}); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
	var nilRt *converge.Runtime
	if _, err := hook.ControlDispatch(nilRt, context.Background(), ctl.Request{Op: ctl.OpPoke, Job: "a"}); err == nil {
		t.Fatal("typed-nil runtime must be rejected")
	}
}

func TestControlListenerUnknownJobRespondsVisibly(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	rt := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rt, ctx)

	wiring, err := hook.OpsDeps(rt)
	if err != nil {
		t.Fatal(err)
	}

	opID := "fedcba0987654321"
	payload, err := json.Marshal(ctl.Command{Op: ctl.OpPoke, Job: "does-not-exist", ID: "x", OpID: opID})
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(context.Background(), ctl.Queue(ns), converge.Message{Payload: payload}); err != nil {
		t.Fatal(err)
	}

	key := ctl.ResKey(ns, opID, wiring.Replica)
	var resp ctl.Response
	convergetest.Await(t, func() bool {
		raw, ok, err := kv.Get(context.Background(), key)
		if err != nil || !ok {
			return false
		}
		return json.Unmarshal(raw, &resp) == nil
	})
	if resp.Acted || !strings.Contains(resp.Err, "unknown") {
		t.Fatalf("resp = %+v, want an unacted response mentioning unknown", resp)
	}
}

func TestControlDispatchBroadcastResponsesSortedByReplica(t *testing.T) {
	const replicas = 6

	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	ns := "svc"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var rts []*converge.Runtime
	for i := 0; i < replicas; i++ {
		rt := newCtlRuntime(t, ns, mq, kv, clock)
		if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
			t.Fatal(err)
		}
		startAndAwaitReady(t, rt, ctx)
		rts = append(rts, rt)
	}

	resp, _, err := awaitControlDispatch(t, clock, rts[0], ctl.Request{Op: ctl.OpPause, Job: "worker", Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != replicas {
		t.Fatalf("resp = %+v, want %d responses", resp, replicas)
	}
	if !slices.IsSortedFunc(resp, func(a, b ctl.Response) int { return strings.Compare(a.Replica, b.Replica) }) {
		t.Fatalf("resp = %+v, want responses sorted by replica", resp)
	}
}

func TestControlListenerSurvivesResponseWriteFailure(t *testing.T) {
	clock := convergetest.NewClock(time.Unix(0, 0))
	mq := inmem.NewMQWithClock(clock)
	realKV := inmem.NewKVWithClock(clock)
	ns := "svc"
	kv := selectiveFailKV{KV: realKV, failPrefix: ctl.ResPrefix(ns, ""), failErr: errors.New("kv set boom")}

	rt := newCtlRuntime(t, ns, mq, kv, clock)
	if err := hook.RegisterJob(rt, newStubJob("worker")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startAndAwaitReady(t, rt, ctx)

	resp, _, err := awaitControlDispatch(t, clock, rt, ctl.Request{Op: ctl.OpPoke, Job: "worker", ID: "x", Timeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 0 {
		t.Fatalf("resp = %+v, want no responses when the KV write fails", resp)
	}

	if _, _, err := awaitControlDispatch(t, clock, rt, ctl.Request{Op: ctl.OpPause, Job: "worker", Timeout: 150 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := realKV.Get(context.Background(), ctl.PausedKey(ns, "worker"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(raw) != "1" {
		t.Fatalf("paused flag = (%q, %v), want (1, true): a response-write failure must not affect unrelated KV writes", raw, ok)
	}
}
