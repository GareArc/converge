package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/reconcile"
)

var wstart = time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

func eventCount(events []converge.Event, match func(converge.Event) bool) int {
	n := 0
	for _, e := range events {
		if match(e) {
			n++
		}
	}
	return n
}

func jobStats(t *testing.T, rt *converge.Runtime, job string) converge.JobStats {
	t.Helper()
	for _, s := range rt.Stats() {
		if s.Job == job {
			return s
		}
	}
	t.Fatalf("no stats for job %q", job)
	return converge.JobStats{}
}

func dlqKeys(t *testing.T, kv converge.KV, job string) []string {
	t.Helper()
	prefix := "wt/converge/worker/" + job + "/dlq/"
	keys, _, err := kv.Scan(context.Background(), prefix, "")
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func dlqRecordAt(t *testing.T, kv converge.KV, key string) DeadLetter {
	t.Helper()
	raw, ok, err := kv.Get(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("get dlq record %q: ok=%v err=%v", key, ok, err)
	}
	var rec DeadLetter
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestHandleRunsAndAcks(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var runs int
	var gotMeta Meta
	var gotPayload string
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
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
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	enqueuedAt := w.Clock.Now()
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 1
	})
	convergetest.AssertStable(t, func() bool {
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
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Surface == converge.SurfaceWorker && rc.Err == nil && rc.Attempt == 1
	}); n != 1 {
		t.Fatalf("successful RunCompleted count = %d, want 1", n)
	}
}

func TestErrorRetriesWithBackoffThenDeadLetters(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 2 })
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 3 })
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 3 })
	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w.KV, keys[0])
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
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		_, ok := e.(converge.MessageDeadLettered)
		return ok
	}); n != 1 {
		t.Fatalf("MessageDeadLettered count = %d, want 1", n)
	}
	stats := jobStats(t, rt, "job")
	if stats.ConsecutiveFails != 3 {
		t.Fatalf("ConsecutiveFails = %d, want 3", stats.ConsecutiveFails)
	}
}

func TestMetaAttemptCountsTransportRedeliveries(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		mu.Unlock()
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 3
	})
	convergetest.AssertStable(t, func() bool {
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
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var ran int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&ran, 1)
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	h := map[string]string{
		converge.HeaderMessageID:     "msg-decode",
		converge.HeaderSchemaVersion: "1",
		converge.HeaderEnqueuedAt:    w.Clock.Now().UTC().Format(time.RFC3339Nano),
		converge.HeaderAttempt:       "0",
	}
	if err := w.MQ.Publish(context.Background(), "job", converge.Message{Kind: "job", Headers: h, Payload: []byte(`{not valid json`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterUndecodable
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&ran) == 0 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 1 {
		t.Fatalf("RunCompleted with non-nil Err count = %d, want 1", n)
	}
	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w.KV, keys[0])
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
		{"attempt header overflow", func(h map[string]string) { h[converge.HeaderAttempt] = strconv.Itoa(math.MaxInt) }, "job", converge.DeadLetterUndecodable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
			rt := w.Build(t)
			tk := NewTask[string]("job", TaskOpts{})
			var ran int32
			err := Handle(rt, tk, func(ctx context.Context, payload string) error {
				atomic.AddInt32(&ran, 1)
				return nil
			}, HandleOpts{})
			if err != nil {
				t.Fatal(err)
			}
			w.Runtime(t)
			h := map[string]string{
				converge.HeaderMessageID:     "msg-" + c.name,
				converge.HeaderSchemaVersion: "1",
				converge.HeaderEnqueuedAt:    w.Clock.Now().UTC().Format(time.RFC3339Nano),
				converge.HeaderAttempt:       "0",
			}
			c.mutate(h)
			if err := w.MQ.Publish(context.Background(), "job", converge.Message{Kind: c.kind, Headers: h, Payload: []byte(`"x"`)}); err != nil {
				t.Fatal(err)
			}
			convergetest.Await(t, func() bool {
				return eventCount(w.Events(), func(e converge.Event) bool {
					dl, ok := e.(converge.MessageDeadLettered)
					return ok && dl.Reason == c.wantReason
				}) == 1
			})
			convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&ran) == 0 })
			if n := eventCount(w.Events(), func(e converge.Event) bool {
				_, ok := e.(converge.RunCompleted)
				return ok
			}); n != 0 {
				t.Fatalf("RunCompleted must not fire, got %d", n)
			}
			keys := dlqKeys(t, w.KV, "job")
			if len(keys) != 1 {
				t.Fatalf("dlq keys = %v, want exactly 1", keys)
			}
			rec := dlqRecordAt(t, w.KV, keys[0])
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
	var fkv *failingDLQKV
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		KV: func(clock *convergetest.Clock) converge.KV {
			fkv = &failingDLQKV{inner: inmem.NewKVWithClock(clock)}
			return fkv
		},
	})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var ran int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&ran, 1)
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock, 200*time.Millisecond, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})
	if got := atomic.LoadInt32(&ran); got != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", got)
	}
	keys := dlqKeys(t, fkv, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		_, ok := e.(converge.MessageDeadLettered)
		return ok
	}); n != 1 {
		t.Fatalf("MessageDeadLettered count = %d, want 1", n)
	}
}

func TestQueueDepthEvents(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	gate := make(chan struct{})
	entered := make(chan struct{})
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		close(entered)
		<-gate
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			qd, ok := e.(converge.QueueDepth)
			return ok && qd.Depth == 1
		}) >= 1
	})
	close(gate)
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			qd, ok := e.(converge.QueueDepth)
			return ok && qd.Depth == 0
		}) >= 1
	})
}

func TestConcurrencyBounds(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	gate := make(chan struct{})
	var mu sync.Mutex
	running := 0
	completed := 0
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
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
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return running == 2
	})
	convergetest.Await(t, func() bool { return jobStats(t, rt, "job").QueueDepth == 3 })
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return running == 2 && jobStats(t, rt, "job").QueueDepth == 3
	})
	close(gate)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return completed == 4
	})
}

func TestDiscardAcksWithEvent(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return Discard{Reason: "gone"}
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		md, ok := e.(converge.MessageDiscarded)
		return ok && md.Reason == "gone"
	}); n != 1 {
		t.Fatalf("MessageDiscarded count = %d, want 1", n)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err == nil
	}); n != 1 {
		t.Fatalf("RunCompleted{Err: nil} count = %d, want 1", n)
	}
	stats := jobStats(t, rt, "job")
	if stats.ConsecutiveFails != 0 {
		t.Fatalf("ConsecutiveFails = %d, want 0", stats.ConsecutiveFails)
	}
	if stats.LastSuccess.IsZero() {
		t.Fatal("LastSuccess not stamped")
	}
	if keys := dlqKeys(t, w.KV, "job"); len(keys) != 0 {
		t.Fatalf("dlq keys = %v, want none", keys)
	}
}

func TestSnoozeRedeliversWithoutConsumingAttempt(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	var headers map[string]string
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
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
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 1
	})
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 1
	})
	convergetest.AdvanceUntil(t, w.Clock, time.Minute, func() bool {
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
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return Snooze{In: 0}
	}, HandleOpts{Retry: RetryPolicy{MinBackoff: time.Hour, MaxBackoff: 2 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 11 })
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			bf, ok := e.(converge.BackoffFallback)
			return ok && bf.Consecutive == 11
		}) == 1
	})
	w.Clock.Advance(time.Minute)
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 11 })
	convergetest.AdvanceUntil(t, w.Clock, 10*time.Minute, func() bool { return atomic.LoadInt32(&runs) >= 12 })
}

func TestSnoozeClampedToMaxAge(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return Snooze{In: time.Hour}
	}, HandleOpts{Retry: RetryPolicy{MaxAge: 5 * time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })

	deadLettered := func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterMaxAge
		}) == 1
	}
	const step = 40 * time.Second
	const maxAdvance = 6 * time.Minute
	var advanced time.Duration
	deadline := time.Now().Add(2 * time.Second)
	for !deadLettered() {
		if advanced >= maxAdvance {
			t.Fatalf("max-age dead-letter not observed within %s of simulated clock advance; snooze was not clamped", maxAdvance)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for max-age dead-letter")
		}
		w.Clock.Advance(step)
		advanced += step
		time.Sleep(2 * time.Millisecond)
	}

	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w.KV, keys[0])
	if rec.Reason != converge.DeadLetterMaxAge.String() {
		t.Fatalf("reason = %q, want %q", rec.Reason, converge.DeadLetterMaxAge.String())
	}
}

func TestSnoozeWithSpentBudgetDeadLettersImmediately(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	entered := make(chan struct{})
	gate := make(chan struct{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		close(entered)
		<-gate
		return Snooze{In: time.Second}
	}, HandleOpts{Retry: RetryPolicy{MaxAge: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	w.Clock.Advance(2 * time.Minute)
	close(gate)

	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })

	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w.KV, keys[0])
	if rec.Reason != converge.DeadLetterMaxAge.String() {
		t.Fatalf("reason = %q, want %q", rec.Reason, converge.DeadLetterMaxAge.String())
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err == nil
	}); n != 1 {
		t.Fatalf("RunCompleted{Err:nil} count = %d, want 1", n)
	}
}

func TestRetryDelayOverflowSafe(t *testing.T) {
	maxBackoff := time.Duration(math.MaxInt64)
	minBackoff := maxBackoff/2 + 1
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{
		Retry: RetryPolicy{MinBackoff: minBackoff, MaxBackoff: maxBackoff},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := e.retryCurve().Delay(5)
	if got <= 0 {
		t.Fatalf("retryCurve().Delay(5) = %v, want positive", got)
	}
	if got > maxBackoff {
		t.Fatalf("retryCurve().Delay(5) = %v, want <= %v", got, maxBackoff)
	}
}

func TestAnonymousMessageIDIsStableContentHash(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var ran int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&ran, 1)
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	anonMsg := func() converge.Message {
		h := map[string]string{
			converge.HeaderSchemaVersion: "1",
			converge.HeaderEnqueuedAt:    w.Clock.Now().UTC().Format(time.RFC3339Nano),
			converge.HeaderAttempt:       "0",
		}
		return converge.Message{Kind: "job", Headers: h, Payload: []byte(`"anon-payload"`)}
	}
	if err := w.MQ.Publish(context.Background(), "job", anonMsg()); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})
	if err := w.MQ.Publish(context.Background(), "job", anonMsg()); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 2
	})

	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	var ids []string
	for _, e := range w.Events() {
		if dl, ok := e.(converge.MessageDeadLettered); ok {
			ids = append(ids, dl.MessageID)
		}
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("MessageDeadLettered MessageIDs = %v, want two identical values", ids)
	}
	if !strings.HasPrefix(ids[0], "anon-") {
		t.Fatalf("MessageID = %q, want prefix %q", ids[0], "anon-")
	}
	if atomic.LoadInt32(&ran) != 2 {
		t.Fatalf("handler ran %d times, want 2", ran)
	}
}

func TestWrongSurfaceSignalDeadLetters(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return reconcile.CheckAgain{In: time.Second}
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterWrongSurface
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		ws, ok := e.(converge.WrongSurfaceSignal)
		return ok && ws.Surface == converge.SurfaceReconcile
	}); n != 1 {
		t.Fatalf("WrongSurfaceSignal count = %d, want 1", n)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 1 {
		t.Fatalf("RunCompleted{Err != nil} count = %d, want 1", n)
	}
	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w.KV, keys[0])
	if rec.Reason != converge.DeadLetterWrongSurface.String() {
		t.Fatalf("reason = %q, want %q", rec.Reason, converge.DeadLetterWrongSurface.String())
	}
}

func TestShutdownIsNeutral(t *testing.T) {
	w1 := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", DrainTimeout: 100 * time.Millisecond})
	rt1 := w1.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{})
	err := Handle(rt1, tk, func(ctx context.Context, payload string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}

	w1.Runtime(t)

	p, err := ProducerFrom(rt1)
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

	if runErr := w1.Stop(t); runErr != nil {
		t.Fatalf("Run returned %v, want nil", runErr)
	}
	if n := eventCount(w1.Events(), func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("RunCompleted count = %d, want 0", n)
	}

	w2 := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		Clock:     w1.Clock,
		MQ:        func(*convergetest.Clock) converge.MQ { return w1.MQ },
		KV:        func(*convergetest.Clock) converge.KV { return w1.KV },
	})
	rt2 := w2.Build(t)

	tk2 := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var runs2 int
	var meta2 Meta
	err = Handle(rt2, tk2, func(ctx context.Context, payload string) error {
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
	w2.Runtime(t)
	convergetest.Await(t, func() bool {
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
	var pmq *publishCountingMQ
	w1 := convergetest.NewWith(t, convergetest.Options{
		Namespace:    "wt",
		DrainTimeout: 100 * time.Millisecond,
		MQ: func(clock *convergetest.Clock) converge.MQ {
			pmq = &publishCountingMQ{MQ: inmem.NewMQWithClock(clock)}
			return pmq
		},
	})
	rt := w1.Build(t)

	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{}, 8)
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}, HandleOpts{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	w1.Runtime(t)

	p, err := ProducerFrom(rt)
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
	convergetest.Await(t, func() bool { return jobStats(t, rt, "job").QueueDepth == 2 })

	before := pmq.publishes.Load()

	if runErr := w1.Stop(t); runErr != nil {
		t.Fatalf("Run returned %v, want nil", runErr)
	}
	if republishes := pmq.publishes.Load() - before; republishes > 10 {
		t.Fatalf("republishes during shutdown = %d, want O(1)", republishes)
	}

	w2 := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		Clock:     w1.Clock,
		MQ:        func(*convergetest.Clock) converge.MQ { return pmq },
		KV:        func(*convergetest.Clock) converge.KV { return w1.KV },
	})
	rt2 := w2.Build(t)
	tk2 := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	err = Handle(rt2, tk2, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		attempts = append(attempts, meta.Attempt)
		mu.Unlock()
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w2.Runtime(t)
	convergetest.Await(t, func() bool {
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
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		MQ: func(clock *convergetest.Clock) converge.MQ {
			cmq = &countingMQ{MQ: inmem.NewMQWithClock(clock)}
			return cmq
		},
	})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	gate := make(chan struct{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		<-gate
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Visibility: 90 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	var receipt int64
	convergetest.Await(t, func() bool {
		if c := cmq.count.Load(); c > 0 {
			receipt = c
			return true
		}
		return false
	})
	for i := int64(1); i <= 9; i++ {
		want := receipt + i
		convergetest.AdvanceUntil(t, w.Clock, 30*time.Second, func() bool { return cmq.count.Load() >= want })
	}
	close(gate)
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	w.Clock.Advance(5 * time.Minute)
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
}

type unknownWorkerSignal struct{}

func (unknownWorkerSignal) Error() string { return "worker: unknown signal" }

func (unknownWorkerSignal) ControlSurface() converge.Surface { return converge.SurfaceWorker }

func TestUnknownWorkerSignalFallsBackToError(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return unknownWorkerSignal{}
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 2 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n == 0 {
		t.Fatal("expected a RunCompleted event with non-nil Err")
	}
}

func TestPointerOutcomeDispatch(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	discardRuns := 0
	var snoozeAttempts []int
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
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
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "discard-me", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "snooze-me", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return discardRuns == 1 && len(snoozeAttempts) == 1
	})
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return discardRuns == 1 && len(snoozeAttempts) == 1
	})
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		md, ok := e.(converge.MessageDiscarded)
		return ok && md.Reason == "gone"
	}); n != 1 {
		t.Fatalf("MessageDiscarded count = %d, want 1", n)
	}
	convergetest.AdvanceUntil(t, w.Clock, time.Minute, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(snoozeAttempts) == 2
	})
	convergetest.AssertStable(t, func() bool {
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

func newLeaseHarnessPair(t *testing.T) (wa, wb *convergetest.Harness, lease *inmem.Lease) {
	t.Helper()
	clock := convergetest.NewClock(wstart)
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	lease = inmem.NewLeaseWithClock(clock)
	build := func() *convergetest.Harness {
		return convergetest.NewWith(t, convergetest.Options{
			Namespace: "wt",
			LeaseTTL:  30 * time.Second,
			Clock:     clock,
			MQ:        func(*convergetest.Clock) converge.MQ { return mq },
			KV:        func(*convergetest.Clock) converge.KV { return kv },
			Lease:     func(*convergetest.Clock) converge.Lease { return lease },
		})
	}
	return build(), build(), lease
}

func leaseAcquired(e converge.Event) bool {
	lt, ok := e.(converge.LeaseTransition)
	return ok && lt.Acquired
}

func TestOnOneReplicaOnlyLeaderConsumes(t *testing.T) {
	wa, wb, _ := newLeaseHarnessPair(t)
	rta := wa.Build(t)
	rtb := wb.Build(t)
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
	if err := Handle(rta, tk, handlerFor(&aRuns), HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(rtb, tk, handlerFor(&bRuns), HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	wa.Runtime(t)
	wb.Runtime(t)

	p, err := ProducerFrom(rta)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return aRuns+bRuns == 3
	})
	convergetest.AssertStable(t, func() bool {
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
	if n := eventCount(wa.Events(), leaseAcquired) + eventCount(wb.Events(), leaseAcquired); n != 1 {
		t.Fatalf("LeaseTransition{Acquired:true} count across both replicas = %d, want 1", n)
	}
}

func TestLeaseLossCancelsInFlight(t *testing.T) {
	wa, wb, lease := newLeaseHarnessPair(t)
	rta := wa.Build(t)
	rtb := wb.Build(t)
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
	if err := Handle(rta, tk, handler, HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(rtb, tk, handler, HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	wa.Runtime(t)
	wb.Runtime(t)

	p, err := ProducerFrom(rta)
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

	convergetest.AdvanceUntil(t, wa.Clock, 3*time.Second, func() bool {
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
	build := func() (*convergetest.Harness, *converge.Runtime) {
		h := convergetest.NewWith(t, convergetest.Options{
			Namespace: "wt",
			Clock:     clock,
			MQ:        func(*convergetest.Clock) converge.MQ { return mq },
		})
		rt := h.Build(t)
		return h, rt
	}
	wa, rta := build()
	wb, rtb := build()

	okTask := NewTask[string]("broadcast-ok", TaskOpts{})
	var okA, okB, okWarmA, okWarmB int32
	if err := Handle(rta, okTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&okWarmA, 1)
			return nil
		}
		atomic.AddInt32(&okA, 1)
		return nil
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(rtb, okTask, func(ctx context.Context, payload string) error {
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
	if err := Handle(rta, failTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&failWarmA, 1)
			return nil
		}
		atomic.AddInt32(&failA, 1)
		return errors.New("boom")
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(rtb, failTask, func(ctx context.Context, payload string) error {
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
	if err := Handle(rta, snoozeTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&snoozeWarmA, 1)
			return nil
		}
		atomic.AddInt32(&snoozeA, 1)
		return Snooze{In: time.Second}
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(rtb, snoozeTask, func(ctx context.Context, payload string) error {
		if payload == "warm" {
			atomic.AddInt32(&snoozeWarmB, 1)
			return nil
		}
		atomic.AddInt32(&snoozeB, 1)
		return Snooze{In: time.Second}
	}, HandleOpts{RunMode: converge.OnAllReplicas}); err != nil {
		t.Fatal(err)
	}

	wa.Runtime(t)
	wb.Runtime(t)

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
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&okA) == 1 && atomic.LoadInt32(&okB) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&okA) == 1 && atomic.LoadInt32(&okB) == 1 })

	if err := failTask.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&failA) == 1 && atomic.LoadInt32(&failB) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&failA) == 1 && atomic.LoadInt32(&failB) == 1 })
	failRunCompleted := func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Job == "broadcast-fail" && rc.Err != nil
	}
	if n := eventCount(wa.Events(), failRunCompleted); n != 1 {
		t.Fatalf("worldA RunCompleted{Err!=nil} count = %d, want 1", n)
	}
	if n := eventCount(wb.Events(), failRunCompleted); n != 1 {
		t.Fatalf("worldB RunCompleted{Err!=nil} count = %d, want 1", n)
	}

	if err := snoozeTask.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&snoozeA) == 1 && atomic.LoadInt32(&snoozeB) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&snoozeA) == 1 && atomic.LoadInt32(&snoozeB) == 1 })
	snoozeRunCompleted := func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Job == "broadcast-snooze" && rc.Err != nil
	}
	if n := eventCount(wa.Events(), snoozeRunCompleted); n != 1 {
		t.Fatalf("worldA snoozed RunCompleted{Err!=nil} count = %d, want 1", n)
	}
	if n := eventCount(wb.Events(), snoozeRunCompleted); n != 1 {
		t.Fatalf("worldB snoozed RunCompleted{Err!=nil} count = %d, want 1", n)
	}
}

func TestPausedConsumesNothing(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 0 })
	if runErr := w.Stop(t); runErr != nil {
		t.Fatalf("Run returned %v, want nil", runErr)
	}

	w2 := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		Clock:     w.Clock,
		MQ:        func(*convergetest.Clock) converge.MQ { return w.MQ },
		KV:        func(*convergetest.Clock) converge.KV { return w.KV },
	})
	rt2 := w2.Build(t)
	tk2 := NewTask[string]("job", TaskOpts{})
	var runs2 int32
	err = Handle(rt2, tk2, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs2, 1)
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w2.Runtime(t)
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs2) == 1 })
}

func TestInitialPausedResumesAndStartsConsuming(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	var runs int32
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(ctx context.Context, payload []byte) error {
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	tk := NewTask[string]("job", TaskOpts{})
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 0 })

	e.SetPaused(false)
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
}

func TestPauseMidStreamStopsDeliveryThenResumeDeliversBacklog(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var runs int
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(ctx context.Context, payload []byte) error {
		mu.Lock()
		first := runs == 0
		mu.Unlock()
		if first {
			once.Do(func() { close(entered) })
			<-release
		}
		mu.Lock()
		runs++
		mu.Unlock()
		return nil
	}, HandleOpts{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	tk := NewTask[string]("job", TaskOpts{})
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "one", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler never started")
	}

	e.SetPaused(true)
	close(release)
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })

	if err := tk.Enqueue(context.Background(), p, "two", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })

	e.SetPaused(false)
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 2 })
}

func TestPauseWithBlockedHandlerDrainTimeoutCancelsHandlerCtx(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", DrainTimeout: 100 * time.Millisecond})
	rt := w.Build(t)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	var startOnce, cancelOnce sync.Once
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(ctx context.Context, payload []byte) error {
		startOnce.Do(func() { close(entered) })
		<-ctx.Done()
		cancelOnce.Do(func() { close(canceled) })
		return ctx.Err()
	}, HandleOpts{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	tk := NewTask[string]("job", TaskOpts{})
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	e.SetPaused(true)
	convergetest.AdvanceUntil(t, w.Clock, 20*time.Millisecond, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	})
}

func TestOnOneReplicaPauseReleasesLease(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{RunMode: converge.OnOneReplica})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	convergetest.Await(t, func() bool { return eventCount(w.Events(), leaseAcquired) == 1 })

	e.SetPaused(true)
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(ev converge.Event) bool {
			lt, ok := ev.(converge.LeaseTransition)
			return ok && !lt.Acquired
		}) >= 1
	})

	lh, ok, err := w.Lease.TryAcquire(context.Background(), "wt/converge/worker/job/lease", time.Second)
	if err != nil || !ok {
		t.Fatalf("lease must be free after pause: ok=%v err=%v", ok, err)
	}
	lh.Release(context.Background())
}

func TestResumeDuringDrainDoesNotInterruptThenStartsNextCycle(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", DrainTimeout: 200 * time.Millisecond})
	rt := w.Build(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var runs int
	var gotErr error
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(ctx context.Context, payload []byte) error {
		mu.Lock()
		first := runs == 0
		mu.Unlock()
		if first {
			once.Do(func() { close(entered) })
			<-release
		}
		mu.Lock()
		runs++
		gotErr = ctx.Err()
		mu.Unlock()
		return nil
	}, HandleOpts{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	tk := NewTask[string]("job", TaskOpts{})
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "one", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler never started")
	}

	e.SetPaused(true)
	e.SetPaused(false)
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 0 })

	close(release)
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })
	mu.Lock()
	if gotErr != nil {
		t.Fatalf("in-flight handler ctx must not be canceled by a resume landing mid-drain, got %v", gotErr)
	}
	mu.Unlock()

	if err := tk.Enqueue(context.Background(), p, "two", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 2 })
}

func TestSetPausedSameValueIsNoOp(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{RunMode: converge.OnOneReplica})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	convergetest.Await(t, func() bool { return eventCount(w.Events(), leaseAcquired) == 1 })

	e.SetPaused(false)
	if e.Info().Paused {
		t.Fatal("same-value SetPaused(false) must not pause")
	}
	convergetest.AssertStable(t, func() bool {
		return eventCount(w.Events(), leaseAcquired) == 1 && eventCount(w.Events(), func(ev converge.Event) bool {
			lt, ok := ev.(converge.LeaseTransition)
			return ok && !lt.Acquired
		}) == 0
	})
}

func TestInfoPausedReflectsLiveState(t *testing.T) {
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if e.Info().Paused {
		t.Fatal("Info().Paused must start false for an unpaused config")
	}
	e.SetPaused(true)
	if !e.Info().Paused {
		t.Fatal("Info().Paused must reflect a live SetPaused(true)")
	}
	e.SetPaused(false)
	if e.Info().Paused {
		t.Fatal("Info().Paused must reflect a live SetPaused(false)")
	}
}

func TestRateLimitSpacesRuns(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Concurrency: 4, RateLimit: converge.Rate{Events: 1, Per: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) == 2 })
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) == 3 })
}

func TestRateLimitWaitIsHeartbeatCovered(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	runs := map[string]int{}
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		mu.Lock()
		runs[payload]++
		mu.Unlock()
		return nil
	}, HandleOpts{Concurrency: 2, Visibility: 30 * time.Second, RateLimit: converge.Rate{Events: 1, Per: 2 * time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "a", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "b", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["a"]+runs["b"] == 1
	})
	convergetest.AdvanceUntil(t, w.Clock, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["a"] == 1 && runs["b"] == 1
	})
	for range 15 {
		w.Clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if runs["a"] != 1 || runs["b"] != 1 {
		t.Fatalf("runs = %v, want exactly {a:1, b:1}", runs)
	}
}

func TestPanicIsRecoveredAsError(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var attempts []int
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
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
	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock, 100*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 2
	})
	convergetest.AssertStable(t, func() bool {
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
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Attempt == 1 && rc.Err != nil && strings.Contains(rc.Err.Error(), "boom")
	}); n != 1 {
		t.Fatalf("RunCompleted{Attempt:1, Err containing panic} count = %d, want 1", n)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
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
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		LeaseTTL:  30 * time.Second,
		Lease: func(clock *convergetest.Clock) converge.Lease {
			return &failExtendLease{inner: inmem.NewLeaseWithClock(clock)}
		},
	})
	rt := w.Build(t)

	tk := NewTask[string]("job", TaskOpts{})
	started := make(chan struct{})
	canceled := make(chan struct{})
	var startOnce, cancelOnce sync.Once
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		cancelOnce.Do(func() { close(canceled) })
		return ctx.Err()
	}, HandleOpts{RunMode: converge.OnOneReplica})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p, err := ProducerFrom(rt)
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

	convergetest.AdvanceUntil(t, w.Clock, 3*time.Second, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	})
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			lt, ok := e.(converge.LeaseTransition)
			return ok && !lt.Acquired
		}) >= 1
	})
}

func TestQuietFlipsFalseWhileHandlerInFlight(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	gate := make(chan struct{})
	entered := make(chan struct{})
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(ctx context.Context, payload []byte) error {
		close(entered)
		<-gate
		return nil
	}, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, e); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	if !e.Quiet() {
		t.Fatal("engine must start quiet")
	}
	tk := NewTask[string]("job", TaskOpts{})
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	if e.Quiet() {
		t.Fatal("must not be quiet while a handler is in flight")
	}
	close(gate)
	convergetest.Await(t, e.Quiet)
}

func TestHintIsAReconcileVerb(t *testing.T) {
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Hint("x"); err == nil || !strings.Contains(err.Error(), "reconcile verb") {
		t.Fatalf("Hint error = %v, want mention of the reconcile surface", err)
	}
}

func TestRunPassNowIsAReconcileVerb(t *testing.T) {
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RunPassNow(context.Background()); err == nil || !strings.Contains(err.Error(), "reconcile verb") {
		t.Fatalf("RunPassNow error = %v, want mention of the reconcile surface", err)
	}
}
