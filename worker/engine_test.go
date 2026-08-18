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
	mq    *inmem.MQ
	kv    converge.KV
	rec   *recorder
	done  chan error
}

type worldOpts struct {
	kv func(clock *convergetest.Clock) converge.KV
}

func newWorld(t *testing.T) *world { return newWorldWith(t, worldOpts{}) }

func newWorldWith(t *testing.T, o worldOpts) *world {
	t.Helper()
	clock := convergetest.NewClock(wstart)
	mq := inmem.NewMQWithClock(clock)
	var kv converge.KV
	if o.kv != nil {
		kv = o.kv(clock)
	} else {
		kv = inmem.NewKVWithClock(clock)
	}
	rec := &recorder{}
	rt, err := converge.New(converge.Options{
		Namespace: "wt",
		MQ:        mq,
		Lease:     inmem.NewLeaseWithClock(clock),
		KV:        kv,
		Observer:  rec,
		Clock:     clock,
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
