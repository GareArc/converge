package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

var wstart = time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

type recorder struct {
	mu     sync.Mutex
	events []converge.Event
}

func (r *recorder) Observe(e converge.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) count(match func(converge.Event) bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if match(e) {
			n++
		}
	}
	return n
}

type world struct {
	rt    *converge.Runtime
	clock *convergetest.Clock
	mq    converge.MQ
	kv    converge.KV
	rec   *recorder
	done  chan error
}

type worldOpts struct {
	kv           func(clock *convergetest.Clock) converge.KV
	mq           func(clock *convergetest.Clock) converge.MQ
	drainTimeout time.Duration
}

func newWorld(t *testing.T) *world { return newWorldWith(t, worldOpts{}) }

func newWorldWith(t *testing.T, o worldOpts) *world {
	t.Helper()
	clock := convergetest.NewClock(wstart)
	var mq converge.MQ
	if o.mq != nil {
		mq = o.mq(clock)
	} else {
		mq = inmem.NewMQWithClock(clock)
	}
	var kv converge.KV
	if o.kv != nil {
		kv = o.kv(clock)
	} else {
		kv = inmem.NewKVWithClock(clock)
	}
	rec := &recorder{}
	rt, err := converge.New(converge.Options{
		Namespace:    "wt",
		MQ:           mq,
		Lease:        inmem.NewLeaseWithClock(clock),
		KV:           kv,
		Observer:     rec,
		Clock:        clock,
		DrainTimeout: o.drainTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &world{rt: rt, clock: clock, mq: mq, kv: kv, rec: rec, done: make(chan error, 1)}
}

func (w *world) run(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		w.stop(t)
	})
	go func() { w.done <- w.rt.Run(ctx) }()
	select {
	case <-w.rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never ready")
	}
	return cancel
}

func (w *world) stop(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-w.done:
			if err != nil {
				t.Fatalf("Run returned %v", err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned")
		}
		w.clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func (w *world) stats(t *testing.T, job string) converge.JobStats {
	t.Helper()
	for _, s := range w.rt.Stats() {
		if s.Job == job {
			return s
		}
	}
	t.Fatalf("no stats for job %q", job)
	return converge.JobStats{}
}

func (w *world) advanceUntil(t *testing.T, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true while advancing")
		}
		w.clock.Advance(step)
		time.Sleep(2 * time.Millisecond)
	}
}

func assertStable(t *testing.T, cond func() bool) {
	t.Helper()
	time.Sleep(20 * time.Millisecond)
	if !cond() {
		t.Fatal("state changed while it must hold")
	}
}

func dlqKeys(t *testing.T, w *world, job string) []string {
	t.Helper()
	prefix := "wt/converge/worker/" + job + "/dlq/"
	keys, _, err := w.kv.Scan(context.Background(), prefix, "")
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func dlqRecordAt(t *testing.T, w *world, key string) dlqRecord {
	t.Helper()
	raw, ok, err := w.kv.Get(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("get dlq record %q: ok=%v err=%v", key, ok, err)
	}
	var rec dlqRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestHandleRunsAndAcks(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var runs int
	var gotMeta Meta
	var gotPayload string
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		mu.Lock()
		defer mu.Unlock()
		runs++
		gotPayload = payload
		gotMeta, _ = MetaFromContext(ctx)
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	enqueuedAt := w.clock.Now()
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 1
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 1
	})
	mu.Lock()
	defer mu.Unlock()
	if gotPayload != "hello" {
		t.Fatalf("payload = %q, want %q", gotPayload, "hello")
	}
	if gotMeta.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", gotMeta.Attempt)
	}
	if gotMeta.MaxAttempts != DefaultMaxAttempts {
		t.Fatalf("maxAttempts = %d, want %d", gotMeta.MaxAttempts, DefaultMaxAttempts)
	}
	if gotMeta.MessageID == "" {
		t.Fatal("empty message id")
	}
	if !gotMeta.EnqueuedAt.Equal(enqueuedAt) {
		t.Fatalf("enqueuedAt = %v, want %v", gotMeta.EnqueuedAt, enqueuedAt)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Surface == converge.SurfaceWorker && rc.Err == nil && rc.Attempt == 1
	}); n != 1 {
		t.Fatalf("successful RunCompleted count = %d, want 1", n)
	}
}

func TestErrorRetriesWithBackoffThenDeadLetters(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	w.advanceUntil(t, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 2 })
	w.advanceUntil(t, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 3 })
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 3 })
	keys := dlqKeys(t, w, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w, keys[0])
	if rec.Attempt != 3 {
		t.Fatalf("dlq record attempt = %d, want 3", rec.Attempt)
	}
	if rec.Reason != converge.DeadLetterMaxAttempts.String() {
		t.Fatalf("dlq record reason = %q, want %q", rec.Reason, converge.DeadLetterMaxAttempts.String())
	}
	if rec.Error == "" {
		t.Fatal("dlq record error must be non-empty")
	}
	var payload string
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload != "hello" {
		t.Fatalf("dlq record payload = %q, want %q", payload, "hello")
	}
	if n := w.rec.count(func(e converge.Event) bool {
		_, ok := e.(converge.MessageDeadLettered)
		return ok
	}); n != 1 {
		t.Fatalf("MessageDeadLettered count = %d, want 1", n)
	}
	stats := w.stats(t, "job")
	if stats.ConsecutiveFails != 3 {
		t.Fatalf("ConsecutiveFails = %d, want 3", stats.ConsecutiveFails)
	}
}

func TestMetaAttemptCountsTransportRedeliveries(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		mu.Unlock()
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	w.advanceUntil(t, 100*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 3
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("attempts = %v, want %v", attempts, want)
	}
}

func TestDecodeFailureDeadLettersImmediately(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var ran int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	h := map[string]string{
		converge.HeaderMessageID:     "msg-decode",
		converge.HeaderSchemaVersion: "1",
		converge.HeaderEnqueuedAt:    w.clock.Now().UTC().Format(time.RFC3339Nano),
		converge.HeaderAttempt:       "0",
	}
	if err := w.mq.Publish(context.Background(), "job", converge.Message{Kind: "job", Headers: h, Payload: []byte(`{not valid json`)}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterUndecodable
		}) == 1
	})
	assertStable(t, func() bool { return atomic.LoadInt32(&ran) == 0 })
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 1 {
		t.Fatalf("RunCompleted with non-nil Err count = %d, want 1", n)
	}
	keys := dlqKeys(t, w, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w, keys[0])
	if rec.Reason != converge.DeadLetterUndecodable.String() {
		t.Fatalf("reason = %q, want %q", rec.Reason, converge.DeadLetterUndecodable.String())
	}
}

func TestReceiptGuardsDeadLetter(t *testing.T) {
	type tc struct {
		name       string
		mutate     func(h map[string]string)
		kind       string
		wantReason converge.DeadLetterReason
	}
	cases := []tc{
		{"wrong kind", func(map[string]string) {}, "not-job", converge.DeadLetterWrongKind},
		{"missing schema header", func(h map[string]string) { delete(h, converge.HeaderSchemaVersion) }, "job", converge.DeadLetterSchemaVersion},
		{"wrong schema version", func(h map[string]string) { h[converge.HeaderSchemaVersion] = "2" }, "job", converge.DeadLetterSchemaVersion},
		{"unparseable attempt", func(h map[string]string) { h[converge.HeaderAttempt] = "nope" }, "job", converge.DeadLetterUndecodable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			tk := NewTask[string]("job", TaskOpts{})
			var ran int32
			err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
				atomic.AddInt32(&ran, 1)
				return nil
			}, HandleOpts{})
			if err != nil {
				t.Fatal(err)
			}
			w.run(t)
			h := map[string]string{
				converge.HeaderMessageID:     "msg-" + c.name,
				converge.HeaderSchemaVersion: "1",
				converge.HeaderEnqueuedAt:    w.clock.Now().UTC().Format(time.RFC3339Nano),
				converge.HeaderAttempt:       "0",
			}
			c.mutate(h)
			if err := w.mq.Publish(context.Background(), "job", converge.Message{Kind: c.kind, Headers: h, Payload: []byte(`"x"`)}); err != nil {
				t.Fatal(err)
			}
			await(t, func() bool {
				return w.rec.count(func(e converge.Event) bool {
					dl, ok := e.(converge.MessageDeadLettered)
					return ok && dl.Reason == c.wantReason
				}) == 1
			})
			assertStable(t, func() bool { return atomic.LoadInt32(&ran) == 0 })
			if n := w.rec.count(func(e converge.Event) bool {
				_, ok := e.(converge.RunCompleted)
				return ok
			}); n != 0 {
				t.Fatalf("RunCompleted must not fire, got %d", n)
			}
			keys := dlqKeys(t, w, "job")
			if len(keys) != 1 {
				t.Fatalf("dlq keys = %v, want exactly 1", keys)
			}
			rec := dlqRecordAt(t, w, keys[0])
			if rec.Reason != c.wantReason.String() {
				t.Fatalf("reason = %q, want %q", rec.Reason, c.wantReason.String())
			}
		})
	}
}

type failingDLQKV struct {
	inner converge.KV

	mu     sync.Mutex
	failed bool
}

func (k *failingDLQKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return k.inner.Get(ctx, key)
}

func (k *failingDLQKV) SetCAS(ctx context.Context, key string, old, new []byte) (bool, error) {
	return k.inner.SetCAS(ctx, key, old, new)
}

func (k *failingDLQKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	k.mu.Lock()
	if !k.failed && strings.Contains(key, "/dlq/") {
		k.failed = true
		k.mu.Unlock()
		return errors.New("kv write failed")
	}
	k.mu.Unlock()
	return k.inner.Set(ctx, key, val, ttl)
}

func (k *failingDLQKV) Delete(ctx context.Context, key string) error {
	return k.inner.Delete(ctx, key)
}

func (k *failingDLQKV) Scan(ctx context.Context, prefix, cursor string) ([]string, string, error) {
	return k.inner.Scan(ctx, prefix, cursor)
}

func TestDeadLetterKVFailureNacksAndRecovers(t *testing.T) {
	w := newWorldWith(t, worldOpts{kv: func(clock *convergetest.Clock) converge.KV {
		return &failingDLQKV{inner: inmem.NewKVWithClock(clock)}
	}})
	tk := NewTask[string]("job", TaskOpts{})
	var ran int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&ran, 1)
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	w.advanceUntil(t, 200*time.Millisecond, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})
	if got := atomic.LoadInt32(&ran); got != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", got)
	}
	keys := dlqKeys(t, w, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		_, ok := e.(converge.MessageDeadLettered)
		return ok
	}); n != 1 {
		t.Fatalf("MessageDeadLettered count = %d, want 1", n)
	}
}

func TestQueueDepthEvents(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	gate := make(chan struct{})
	entered := make(chan struct{})
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		close(entered)
		<-gate
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			qd, ok := e.(converge.QueueDepth)
			return ok && qd.Depth == 1
		}) >= 1
	})
	close(gate)
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			qd, ok := e.(converge.QueueDepth)
			return ok && qd.Depth == 0
		}) >= 1
	})
}

func TestConcurrencyBounds(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	gate := make(chan struct{})
	var mu sync.Mutex
	running := 0
	completed := 0
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		mu.Lock()
		running++
		mu.Unlock()
		<-gate
		mu.Lock()
		running--
		completed++
		mu.Unlock()
		return nil
	}, HandleOpts{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return running == 2
	})
	await(t, func() bool { return w.stats(t, "job").QueueDepth == 3 })
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return running == 2 && w.stats(t, "job").QueueDepth == 3
	})
	close(gate)
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return completed == 4
	})
}

func TestDiscardAcksWithEvent(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return Discard{Reason: "gone"}
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	if n := w.rec.count(func(e converge.Event) bool {
		md, ok := e.(converge.MessageDiscarded)
		return ok && md.Reason == "gone"
	}); n != 1 {
		t.Fatalf("MessageDiscarded count = %d, want 1", n)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err == nil
	}); n != 1 {
		t.Fatalf("RunCompleted{Err: nil} count = %d, want 1", n)
	}
	stats := w.stats(t, "job")
	if stats.ConsecutiveFails != 0 {
		t.Fatalf("ConsecutiveFails = %d, want 0", stats.ConsecutiveFails)
	}
	if stats.LastSuccess.IsZero() {
		t.Fatal("LastSuccess not stamped")
	}
	if keys := dlqKeys(t, w, "job"); len(keys) != 0 {
		t.Fatalf("dlq keys = %v, want none", keys)
	}
}

func TestSnoozeRedeliversWithoutConsumingAttempt(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	var headers map[string]string
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		headers = meta.Headers
		n := len(attempts)
		mu.Unlock()
		if n == 1 {
			return Snooze{In: time.Minute}
		}
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 1
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 1
	})
	w.advanceUntil(t, time.Minute, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	if want := []int{1, 1}; !reflect.DeepEqual(attempts, want) {
		t.Fatalf("attempts = %v, want %v", attempts, want)
	}
	if headers[converge.HeaderSnoozes] != "1" {
		t.Fatalf("snoozes header = %q, want %q", headers[converge.HeaderSnoozes], "1")
	}
}

func TestSnoozeFloorAndBackoffFallback(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return Snooze{In: 0}
	}, HandleOpts{Retry: RetryPolicy{MinBackoff: time.Hour, MaxBackoff: 2 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	w.advanceUntil(t, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 11 })
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			bf, ok := e.(converge.BackoffFallback)
			return ok && bf.Consecutive == 11
		}) == 1
	})
	w.clock.Advance(time.Minute)
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 11 })
	w.advanceUntil(t, 10*time.Minute, func() bool { return atomic.LoadInt32(&runs) >= 12 })
}

func TestWrongSurfaceSignalDeadLetters(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return reconcile.CheckAgain{In: time.Second}
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterWrongSurface
		}) == 1
	})
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	if n := w.rec.count(func(e converge.Event) bool {
		ws, ok := e.(converge.WrongSurfaceSignal)
		return ok && ws.Surface == converge.SurfaceReconcile
	}); n != 1 {
		t.Fatalf("WrongSurfaceSignal count = %d, want 1", n)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 1 {
		t.Fatalf("RunCompleted{Err != nil} count = %d, want 1", n)
	}
	keys := dlqKeys(t, w, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w, keys[0])
	if rec.Reason != converge.DeadLetterWrongSurface.String() {
		t.Fatalf("reason = %q, want %q", rec.Reason, converge.DeadLetterWrongSurface.String())
	}
}

func TestShutdownIsNeutral(t *testing.T) {
	w1 := newWorldWith(t, worldOpts{drainTimeout: 100 * time.Millisecond})
	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{})
	err := Handle(w1.rt, tk, func(ctx context.Context, payload string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- w1.rt.Run(runCtx) }()
	select {
	case <-w1.rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never ready")
	}

	p, err := ProducerFrom(w1.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	var runErr error
	gotDone := false
	deadline := time.Now().Add(2 * time.Second)
	for !gotDone {
		select {
		case runErr = <-done:
			gotDone = true
		default:
		}
		if gotDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned")
		}
		w1.clock.Advance(100 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}
	if runErr != nil {
		t.Fatalf("Run returned %v, want nil", runErr)
	}
	if n := w1.rec.count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("RunCompleted count = %d, want 0", n)
	}

	rec2 := &recorder{}
	rt2, err := converge.New(converge.Options{
		Namespace: "wt",
		MQ:        w1.mq,
		Lease:     inmem.NewLeaseWithClock(w1.clock),
		KV:        w1.kv,
		Observer:  rec2,
		Clock:     w1.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	w2 := &world{rt: rt2, clock: w1.clock, mq: w1.mq, kv: w1.kv, rec: rec2, done: make(chan error, 1)}

	tk2 := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var runs2 int
	var meta2 Meta
	err = Handle(w2.rt, tk2, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		runs2++
		meta2 = meta
		mu.Unlock()
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w2.run(t)
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs2 == 1
	})
	mu.Lock()
	defer mu.Unlock()
	if meta2.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", meta2.Attempt)
	}
}

type countingDelivery struct {
	converge.Delivery
	count *atomic.Int64
}

func (d countingDelivery) Extend(ctx context.Context, visibility time.Duration) error {
	d.count.Add(1)
	return d.Delivery.Extend(ctx, visibility)
}

type countingMQ struct {
	*inmem.MQ
	count atomic.Int64
}

func (m *countingMQ) ConsumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	return m.MQ.ConsumeGroup(ctx, queue, group, func(d converge.Delivery) {
		deliver(countingDelivery{Delivery: d, count: &m.count})
	})
}

type publishCountingMQ struct {
	*inmem.MQ
	publishes atomic.Int64
}

func (m *publishCountingMQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	m.publishes.Add(1)
	return m.MQ.Publish(ctx, queue, msg)
}

func TestShutdownDrainsWithoutRepublishLivelock(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	pmq := &publishCountingMQ{MQ: inmem.NewMQWithClock(clock)}
	kv := inmem.NewKVWithClock(clock)
	rec := &recorder{}
	rt, err := converge.New(converge.Options{
		Namespace:    "wt",
		MQ:           pmq,
		Lease:        inmem.NewLeaseWithClock(clock),
		KV:           kv,
		Observer:     rec,
		Clock:        clock,
		DrainTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	w1 := &world{rt: rt, clock: clock, mq: pmq, kv: kv, rec: rec, done: make(chan error, 1)}

	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{}, 8)
	err = Handle(w1.rt, tk, func(ctx context.Context, payload string) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}, HandleOpts{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- w1.rt.Run(runCtx) }()
	select {
	case <-w1.rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never ready")
	}

	p, err := ProducerFrom(w1.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "one", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler never started")
	}
	if err := tk.Enqueue(context.Background(), p, "two", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return w1.stats(t, "job").QueueDepth == 2 })

	before := pmq.publishes.Load()
	cancel()

	var runErr error
	gotDone := false
	deadline := time.Now().Add(2 * time.Second)
	for !gotDone {
		select {
		case runErr = <-done:
			gotDone = true
		default:
		}
		if gotDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned: drain timer starved by neutral-republish redelivery loop")
		}
		clock.Advance(100 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}
	if runErr != nil {
		t.Fatalf("Run returned %v, want nil", runErr)
	}
	if republishes := pmq.publishes.Load() - before; republishes > 10 {
		t.Fatalf("republishes during shutdown = %d, want O(1)", republishes)
	}

	rec2 := &recorder{}
	rt2, err := converge.New(converge.Options{
		Namespace: "wt",
		MQ:        pmq,
		Lease:     inmem.NewLeaseWithClock(clock),
		KV:        kv,
		Observer:  rec2,
		Clock:     clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	w2 := &world{rt: rt2, clock: clock, mq: pmq, kv: kv, rec: rec2, done: make(chan error, 1)}
	tk2 := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	err = Handle(w2.rt, tk2, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		mu.Unlock()
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w2.run(t)
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	want := []int{1, 1}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("successor attempts = %v, want %v", attempts, want)
	}
}

func TestVisibilityHeartbeatExtends(t *testing.T) {
	var cmq *countingMQ
	w := newWorldWith(t, worldOpts{mq: func(clock *convergetest.Clock) converge.MQ {
		cmq = &countingMQ{MQ: inmem.NewMQWithClock(clock)}
		return cmq
	}})
	tk := NewTask[string]("job", TaskOpts{})
	gate := make(chan struct{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		<-gate
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Visibility: 90 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	var receipt int64
	await(t, func() bool {
		if c := cmq.count.Load(); c > 0 {
			receipt = c
			return true
		}
		return false
	})
	for i := int64(1); i <= 9; i++ {
		want := receipt + i
		w.advanceUntil(t, 30*time.Second, func() bool { return cmq.count.Load() >= want })
	}
	close(gate)
	await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	w.clock.Advance(5 * time.Minute)
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
}

type unknownWorkerSignal struct{}

func (unknownWorkerSignal) Error() string { return "worker: unknown signal" }

func (unknownWorkerSignal) ControlSurface() converge.Surface { return converge.SurfaceWorker }

func TestUnknownWorkerSignalFallsBackToError(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return unknownWorkerSignal{}
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	w.advanceUntil(t, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 2 })
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n == 0 {
		t.Fatal("expected a RunCompleted event with non-nil Err")
	}
}

func TestPointerOutcomeDispatch(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	discardRuns := 0
	var snoozeAttempts []int
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		switch payload {
		case "discard-me":
			mu.Lock()
			discardRuns++
			mu.Unlock()
			return &Discard{Reason: "gone"}
		case "snooze-me":
			mu.Lock()
			snoozeAttempts = append(snoozeAttempts, meta.Attempt)
			n := len(snoozeAttempts)
			mu.Unlock()
			if n == 1 {
				return &Snooze{In: time.Minute}
			}
			return nil
		default:
			return fmt.Errorf("unexpected payload %q", payload)
		}
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "discard-me", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "snooze-me", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return discardRuns == 1 && len(snoozeAttempts) == 1
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return discardRuns == 1 && len(snoozeAttempts) == 1
	})
	if n := w.rec.count(func(e converge.Event) bool {
		md, ok := e.(converge.MessageDiscarded)
		return ok && md.Reason == "gone"
	}); n != 1 {
		t.Fatalf("MessageDiscarded count = %d, want 1", n)
	}
	w.advanceUntil(t, time.Minute, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(snoozeAttempts) == 2
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return discardRuns == 1 && len(snoozeAttempts) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	if want := []int{1, 1}; !reflect.DeepEqual(snoozeAttempts, want) {
		t.Fatalf("snoozeAttempts = %v, want %v", snoozeAttempts, want)
	}
}

func newLeaseWorldPair(t *testing.T) (*world, *world, *inmem.Lease) {
	t.Helper()
	clock := convergetest.NewClock(wstart)
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	lease := inmem.NewLeaseWithClock(clock)
	build := func() *world {
		rec := &recorder{}
		rt, err := converge.New(converge.Options{
			Namespace: "wt",
			MQ:        mq,
			Lease:     lease,
			KV:        kv,
			Observer:  rec,
			Clock:     clock,
		})
		if err != nil {
			t.Fatal(err)
		}
		return &world{rt: rt, clock: clock, mq: mq, kv: kv, rec: rec, done: make(chan error, 1)}
	}
	return build(), build(), lease
}

func leaseAcquired(e converge.Event) bool {
	lt, ok := e.(converge.LeaseTransition)
	return ok && lt.Acquired
}

func TestOnOneReplicaOnlyLeaderConsumes(t *testing.T) {
	wa, wb, _ := newLeaseWorldPair(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var aRuns, bRuns int
	handlerFor := func(counter *int) func(context.Context, string) error {
		return func(ctx context.Context, payload string) error {
			mu.Lock()
			*counter++
			mu.Unlock()
			return nil
		}
	}
	if err := Handle(wa.rt, tk, handlerFor(&aRuns), HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(wb.rt, tk, handlerFor(&bRuns), HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	wa.run(t)
	wb.run(t)

	p, err := ProducerFrom(wa.rt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aRuns+bRuns == 3
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aRuns+bRuns == 3
	})
	mu.Lock()
	leaderOnly := (aRuns == 3 && bRuns == 0) || (aRuns == 0 && bRuns == 3)
	gotA, gotB := aRuns, bRuns
	mu.Unlock()
	if !leaderOnly {
		t.Fatalf("expected exactly one replica to handle all 3 runs, got aRuns=%d bRuns=%d", gotA, gotB)
	}
	if n := wa.rec.count(leaseAcquired) + wb.rec.count(leaseAcquired); n != 1 {
		t.Fatalf("LeaseTransition{Acquired:true} count across both replicas = %d, want 1", n)
	}
}

func TestLeaseLossCancelsInFlight(t *testing.T) {
	wa, wb, lease := newLeaseWorldPair(t)
	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{})
	var startOnce sync.Once
	var mu sync.Mutex
	var attempts []int
	handler := func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		n := len(attempts)
		mu.Unlock()
		if n == 1 {
			startOnce.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	if err := Handle(wa.rt, tk, handler, HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(wb.rt, tk, handler, HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	wa.run(t)
	wb.run(t)

	p, err := ProducerFrom(wa.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	lease.Expire("wt/converge/worker/job/lease")

	wa.advanceUntil(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	if attempts[1] != 1 {
		t.Fatalf("successor's attempt = %d, want 1", attempts[1])
	}
}

func TestOnAllReplicasBroadcast(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	mq := inmem.NewMQWithClock(clock)
	build := func() *world {
		rec := &recorder{}
		rt, err := converge.New(converge.Options{Namespace: "wt", MQ: mq, Observer: rec, Clock: clock})
		if err != nil {
			t.Fatal(err)
		}
		return &world{rt: rt, clock: clock, mq: mq, rec: rec, done: make(chan error, 1)}
	}
	wa, wb := build(), build()

	okTask := NewTask[string]("broadcast-ok", TaskOpts{})
	var okA, okB, okWarmA, okWarmB int32
	if err := Handle(wa.rt, okTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&okWarmA, 1)
			return nil
		}
		atomic.AddInt32(&okA, 1)
		return nil
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(wb.rt, okTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&okWarmB, 1)
			return nil
		}
		atomic.AddInt32(&okB, 1)
		return nil
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}

	failTask := NewTask[string]("broadcast-fail", TaskOpts{})
	var failA, failB, failWarmA, failWarmB int32
	if err := Handle(wa.rt, failTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&failWarmA, 1)
			return nil
		}
		atomic.AddInt32(&failA, 1)
		return errors.New("boom")
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(wb.rt, failTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&failWarmB, 1)
			return nil
		}
		atomic.AddInt32(&failB, 1)
		return errors.New("boom")
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}

	snoozeTask := NewTask[string]("broadcast-snooze", TaskOpts{})
	var snoozeA, snoozeB, snoozeWarmA, snoozeWarmB int32
	if err := Handle(wa.rt, snoozeTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&snoozeWarmA, 1)
			return nil
		}
		atomic.AddInt32(&snoozeA, 1)
		return Snooze{In: time.Second}
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(wb.rt, snoozeTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&snoozeWarmB, 1)
			return nil
		}
		atomic.AddInt32(&snoozeB, 1)
		return Snooze{In: time.Second}
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}

	wa.run(t)
	wb.run(t)

	p, err := NewProducer(mq)
	if err != nil {
		t.Fatal(err)
	}

	awaitLive := func(tk Task[string], a, b *int32) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for atomic.LoadInt32(a) == 0 || atomic.LoadInt32(b) == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("broadcast subscriptions for %q never became live", tk.name)
			}
			if err := tk.Enqueue(context.Background(), p, "warm", EnqueueOpts{}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	awaitLive(okTask, &okWarmA, &okWarmB)
	awaitLive(failTask, &failWarmA, &failWarmB)
	awaitLive(snoozeTask, &snoozeWarmA, &snoozeWarmB)

	if err := okTask.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return atomic.LoadInt32(&okA) == 1 && atomic.LoadInt32(&okB) == 1 })
	assertStable(t, func() bool { return atomic.LoadInt32(&okA) == 1 && atomic.LoadInt32(&okB) == 1 })

	if err := failTask.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return atomic.LoadInt32(&failA) == 1 && atomic.LoadInt32(&failB) == 1 })
	assertStable(t, func() bool { return atomic.LoadInt32(&failA) == 1 && atomic.LoadInt32(&failB) == 1 })
	failRunCompleted := func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Job == "broadcast-fail" && rc.Err != nil
	}
	if n := wa.rec.count(failRunCompleted); n != 1 {
		t.Fatalf("worldA RunCompleted{Err!=nil} count = %d, want 1", n)
	}
	if n := wb.rec.count(failRunCompleted); n != 1 {
		t.Fatalf("worldB RunCompleted{Err!=nil} count = %d, want 1", n)
	}

	if err := snoozeTask.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return atomic.LoadInt32(&snoozeA) == 1 && atomic.LoadInt32(&snoozeB) == 1 })
	assertStable(t, func() bool { return atomic.LoadInt32(&snoozeA) == 1 && atomic.LoadInt32(&snoozeB) == 1 })
	snoozeRunCompleted := func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Job == "broadcast-snooze" && rc.Err != nil
	}
	if n := wa.rec.count(snoozeRunCompleted); n != 1 {
		t.Fatalf("worldA snoozed RunCompleted{Err!=nil} count = %d, want 1", n)
	}
	if n := wb.rec.count(snoozeRunCompleted); n != 1 {
		t.Fatalf("worldB snoozed RunCompleted{Err!=nil} count = %d, want 1", n)
	}
}

func TestPausedConsumesNothing(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	cancel := w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 0 })
	cancel()

	rec2 := &recorder{}
	rt2, err := converge.New(converge.Options{
		Namespace: "wt",
		MQ:        w.mq,
		Lease:     inmem.NewLeaseWithClock(w.clock),
		KV:        w.kv,
		Observer:  rec2,
		Clock:     w.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	w2 := &world{rt: rt2, clock: w.clock, mq: w.mq, kv: w.kv, rec: rec2, done: make(chan error, 1)}
	tk2 := NewTask[string]("job", TaskOpts{})
	var runs2 int32
	err = Handle(w2.rt, tk2, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs2, 1)
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w2.run(t)
	await(t, func() bool { return atomic.LoadInt32(&runs2) == 1 })
}

func TestRateLimitSpacesRuns(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Concurrency: 4, RateLimit: converge.Rate{Events: 1, Per: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	assertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	w.advanceUntil(t, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) == 2 })
	w.advanceUntil(t, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) == 3 })
}

func TestRateLimitWaitIsHeartbeatCovered(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	runs := map[string]int{}
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		mu.Lock()
		runs[payload]++
		mu.Unlock()
		return nil
	}, HandleOpts{Concurrency: 2, Visibility: 30 * time.Second, RateLimit: converge.Rate{Events: 1, Per: 2 * time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "a", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "b", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["a"]+runs["b"] == 1
	})
	w.advanceUntil(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["a"] == 1 && runs["b"] == 1
	})
	for range 15 {
		w.clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if runs["a"] != 1 || runs["b"] != 1 {
		t.Fatalf("runs = %v, want exactly {a:1, b:1}", runs)
	}
}

func TestPanicIsRecoveredAsError(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		n := len(attempts)
		mu.Unlock()
		if n == 1 {
			panic("boom")
		}
		return nil
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	w.advanceUntil(t, 100*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 2
	})
	assertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	want := []int{1, 2}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("attempts = %v, want %v", attempts, want)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Attempt == 1 && rc.Err != nil && strings.Contains(rc.Err.Error(), "boom")
	}); n != 1 {
		t.Fatalf("RunCompleted{Attempt:1, Err containing panic} count = %d, want 1", n)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Attempt == 2 && rc.Err == nil
	}); n != 1 {
		t.Fatalf("RunCompleted{Attempt:2, Err:nil} count = %d, want 1", n)
	}
}

type failExtendHandle struct {
	real converge.LeaseHandle
	done chan struct{}
}

func (h *failExtendHandle) Extend(context.Context, time.Duration) error {
	return errors.New("worker: test: extend always fails")
}

func (h *failExtendHandle) Release(ctx context.Context) error { return h.real.Release(ctx) }

func (h *failExtendHandle) Done() <-chan struct{} { return h.done }

type failExtendLease struct {
	inner converge.Lease
}

func (l *failExtendLease) TryAcquire(ctx context.Context, name string, ttl time.Duration) (converge.LeaseHandle, bool, error) {
	h, ok, err := l.inner.TryAcquire(ctx, name, ttl)
	if err != nil || !ok {
		return h, ok, err
	}
	return &failExtendHandle{real: h, done: make(chan struct{})}, true, nil
}

func TestLeaseExtendFailureFailsFastWhileActive(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	lease := &failExtendLease{inner: inmem.NewLeaseWithClock(clock)}
	rec := &recorder{}
	rt, err := converge.New(converge.Options{
		Namespace: "wt",
		MQ:        mq,
		Lease:     lease,
		KV:        kv,
		Observer:  rec,
		Clock:     clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	w := &world{rt: rt, clock: clock, mq: mq, kv: kv, rec: rec, done: make(chan error, 1)}

	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{})
	canceled := make(chan struct{})
	var startOnce, cancelOnce sync.Once
	err = Handle(w.rt, tk, func(ctx context.Context, payload string) error {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		cancelOnce.Do(func() { close(canceled) })
		return ctx.Err()
	}, HandleOpts{RunMode: converge.OnOneReplica})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	p, err := ProducerFrom(w.rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	w.advanceUntil(t, 3*time.Second, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	})
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			lt, ok := e.(converge.LeaseTransition)
			return ok && !lt.Acquired
		}) >= 1
	})
}
