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
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/reconcile"
)

var wstart = time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

const wns = "wt"

func wInbox(job string) string { return keys.Inbox(wns, job) }

func wProducer(t *testing.T, mq converge.MQ, clock converge.Clock) *converge.Producer {
	t.Helper()
	return mustProducerWith(t, mq, converge.ProducerOpts{Namespace: wns, Clock: clock})
}

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

func shelfKeys(t *testing.T, kv converge.KV, job string) []string {
	t.Helper()
	prefix := "wt/converge/worker/" + job + "/shelf/"
	keys, _, err := kv.Scan(context.Background(), prefix, "")
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func shelfRecordAt(t *testing.T, kv converge.KV, key string) ShelvedMessage {
	t.Helper()
	raw, ok, err := kv.Get(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("get shelf record %q: ok=%v err=%v", key, ok, err)
	}
	var rec ShelvedMessage
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
	p := wProducer(t, w.MQ, w.Clock())
	enqueuedAt := w.Clock().Now()
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
		return ok && rc.Outcome == converge.Succeeded && rc.Attempt == 1
	}); n != 1 {
		t.Fatalf("successful RunCompleted count = %d, want 1", n)
	}
}

func TestInfoReportsTheNamespacedInboxOnceBound(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	if err := Handle(rt, tk, func(context.Context, string) error { return nil }, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	raw, err := hook.Inspect(rt)
	if err != nil {
		t.Fatal(err)
	}
	infos, ok := raw.([]converge.JobInfo)
	if !ok || len(infos) != 1 {
		t.Fatalf("Inspect = %v, want exactly one JobInfo", raw)
	}
	if infos[0].Queue != wInbox("job") {
		t.Fatalf("Queue = %q, want %q", infos[0].Queue, wInbox("job"))
	}
}

func TestErrorRetriesWithBackoffThenShelves(t *testing.T) {
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 2 })
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 3 })
	convergetest.Await(t, func() bool { return len(shelfKeys(t, w.KV, "job")) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 3 })
	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	rec := shelfRecordAt(t, w.KV, keys[0])
	if rec.Attempt != 3 {
		t.Fatalf("shelf record attempt = %d, want 3", rec.Attempt)
	}
	if rec.Reason != reasonMaxAttempts {
		t.Fatalf("shelf record reason = %q, want %q", rec.Reason, reasonMaxAttempts)
	}
	if rec.Error == "" {
		t.Fatal("shelf record error must be non-empty")
	}
	var payload string
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload != "hello" {
		t.Fatalf("shelf record payload = %q, want %q", payload, "hello")
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Retrying
	}); n != 2 {
		t.Fatalf("RunCompleted{Outcome: Retrying} count = %d, want 2", n)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Shelved
	}); n != 1 {
		t.Fatalf("RunCompleted{Outcome: Shelved} count = %d, want 1", n)
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool {
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

func TestDecodeFailureShelvesImmediately(t *testing.T) {
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
		converge.HeaderEnqueuedAt:    w.Clock().Now().UTC().Format(time.RFC3339Nano),
		converge.HeaderAttempt:       "0",
	}
	if err := w.MQ.Publish(context.Background(), wInbox("job"), converge.Message{Kind: "job", Headers: h, Payload: []byte(`{not valid json`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Shelved
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&ran) == 0 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 1 {
		t.Fatalf("RunCompleted with non-nil Err count = %d, want 1", n)
	}
	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	rec := shelfRecordAt(t, w.KV, keys[0])
	if rec.Reason != reasonUndecodable {
		t.Fatalf("reason = %q, want %q", rec.Reason, reasonUndecodable)
	}
}

type failingSetKV struct {
	converge.KV
	err error
}

func (kv failingSetKV) Set(context.Context, string, []byte, time.Duration) error { return kv.err }

func TestShelfWriteFailureIsReportedWithItsOwnCause(t *testing.T) {
	shelfDown := errors.New("kv unavailable")
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		KV: func(c *convergetest.Clock) converge.KV {
			return failingSetKV{KV: inmem.NewKVWithClock(c), err: shelfDown}
		},
	})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	if err := Handle(rt, tk, func(context.Context, string) error { return nil }, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	headers := map[string]string{
		converge.HeaderMessageID:     "msg-1",
		converge.HeaderSchemaVersion: "2",
		converge.HeaderEnqueuedAt:    w.Clock().Now().UTC().Format(time.RFC3339Nano),
		converge.HeaderAttempt:       "0",
	}
	if err := w.MQ.Publish(context.Background(), wInbox("job"), converge.Message{Kind: tk.Name(), Headers: headers, Payload: []byte(`"x"`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Retrying && errors.Is(rc.Err, shelfDown)
		}) >= 1
	})
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil && strings.Contains(rc.Err.Error(), reasonSchemaVersion)
	}); n != 0 {
		t.Fatalf("%d events blamed %q for a retry caused by a failed shelf write", n, reasonSchemaVersion)
	}
}

func TestReceiptGuardsShelving(t *testing.T) {
	type tc struct {
		name       string
		mutate     func(h map[string]string)
		wantReason string
	}
	cases := []tc{
		{"missing schema header", func(h map[string]string) { delete(h, converge.HeaderSchemaVersion) }, reasonSchemaVersion},
		{"wrong schema version", func(h map[string]string) { h[converge.HeaderSchemaVersion] = "2" }, reasonSchemaVersion},
		{"unparseable attempt", func(h map[string]string) { h[converge.HeaderAttempt] = "nope" }, reasonUndecodable},
		{"attempt header overflow", func(h map[string]string) { h[converge.HeaderAttempt] = strconv.Itoa(math.MaxInt) }, reasonUndecodable},
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
				converge.HeaderEnqueuedAt:    w.Clock().Now().UTC().Format(time.RFC3339Nano),
				converge.HeaderAttempt:       "0",
			}
			c.mutate(h)
			if err := w.MQ.Publish(context.Background(), wInbox("job"), converge.Message{Kind: tk.Name(), Headers: h, Payload: []byte(`"x"`)}); err != nil {
				t.Fatal(err)
			}
			convergetest.Await(t, func() bool {
				return eventCount(w.Events(), func(e converge.Event) bool {
					rc, ok := e.(converge.RunCompleted)
					return ok && rc.Outcome == converge.Shelved
				}) == 1
			})
			convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&ran) == 0 })
			if n := eventCount(w.Events(), func(e converge.Event) bool {
				rc, ok := e.(converge.RunCompleted)
				return ok && rc.Outcome != converge.Shelved
			}); n != 0 {
				t.Fatalf("only RunCompleted{Outcome: Shelved} may fire for a guard rejection, got %d other outcomes", n)
			}
			keys := shelfKeys(t, w.KV, "job")
			if len(keys) != 1 {
				t.Fatalf("shelf keys = %v, want exactly 1", keys)
			}
			rec := shelfRecordAt(t, w.KV, keys[0])
			if rec.Reason != c.wantReason {
				t.Fatalf("reason = %q, want %q", rec.Reason, c.wantReason)
			}
		})
	}
}

func TestForeignKindInTheInboxStillRuns(t *testing.T) {
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
		converge.HeaderMessageID:     "msg-foreign-kind",
		converge.HeaderSchemaVersion: "1",
		converge.HeaderEnqueuedAt:    w.Clock().Now().UTC().Format(time.RFC3339Nano),
		converge.HeaderAttempt:       "0",
	}
	msg := converge.Message{Kind: "some-other-name", Headers: h, Payload: []byte(`"x"`)}
	if err := w.MQ.Publish(context.Background(), wInbox("job"), msg); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&ran) == 1 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Shelved
	}); n != 0 {
		t.Fatalf("RunCompleted{Outcome: Shelved} count = %d, want 0; the inbox is the job, so Kind is not routing", n)
	}
}

type failingShelfKV struct {
	inner converge.KV

	mu     sync.Mutex
	failed bool
}

func (k *failingShelfKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return k.inner.Get(ctx, key)
}

func (k *failingShelfKV) SetCAS(ctx context.Context, key string, old, new []byte) (bool, error) {
	return k.inner.SetCAS(ctx, key, old, new)
}

func (k *failingShelfKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	k.mu.Lock()
	if !k.failed && strings.Contains(key, "/shelf/") {
		k.failed = true
		k.mu.Unlock()
		return errors.New("kv write failed")
	}
	k.mu.Unlock()
	return k.inner.Set(ctx, key, val, ttl)
}

func (k *failingShelfKV) Delete(ctx context.Context, key string) error {
	return k.inner.Delete(ctx, key)
}

func (k *failingShelfKV) Scan(ctx context.Context, prefix, cursor string) ([]string, string, error) {
	return k.inner.Scan(ctx, prefix, cursor)
}

func TestShelvingKVFailureNacksAndRecovers(t *testing.T) {
	var fkv *failingShelfKV
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		KV: func(clock *convergetest.Clock) converge.KV {
			fkv = &failingShelfKV{inner: inmem.NewKVWithClock(clock)}
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), 200*time.Millisecond, func() bool {
		return len(shelfKeys(t, fkv, "job")) == 1
	})
	if got := atomic.LoadInt32(&ran); got != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", got)
	}
	keys := shelfKeys(t, fkv, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
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
	p := wProducer(t, w.MQ, w.Clock())
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
	convergetest.Await(t, func() bool { return jobStats(t, rt, "job").InFlight == 3 })
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return running == 2 && jobStats(t, rt, "job").InFlight == 3
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Discarded && rc.Err != nil && strings.Contains(rc.Err.Error(), "gone")
	}); n != 1 {
		t.Fatalf("RunCompleted{Outcome: Discarded} carrying the discard reason: count = %d, want 1; a discard writes no record anywhere else", n)
	}
	stats := jobStats(t, rt, "job")
	if stats.ConsecutiveFails != 0 {
		t.Fatalf("ConsecutiveFails = %d, want 0", stats.ConsecutiveFails)
	}
	if stats.LastSuccess.IsZero() {
		t.Fatal("LastSuccess not stamped")
	}
	if keys := shelfKeys(t, w.KV, "job"); len(keys) != 0 {
		t.Fatalf("shelf keys = %v, want none", keys)
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
	p := wProducer(t, w.MQ, w.Clock())
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
	convergetest.AdvanceUntil(t, w.Clock(), time.Minute, func() bool {
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

func TestSnoozeDelayEscalatesAfterNoBackoffCap(t *testing.T) {
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 11 })
	w.Clock().Advance(time.Minute)
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 11 })
	convergetest.AdvanceUntil(t, w.Clock(), 10*time.Minute, func() bool { return atomic.LoadInt32(&runs) >= 12 })
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })

	shelved := func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Shelved
		}) == 1
	}
	const step = 40 * time.Second
	const maxAdvance = 6 * time.Minute
	var advanced time.Duration
	deadline := time.Now().Add(2 * time.Second)
	for !shelved() {
		if advanced >= maxAdvance {
			t.Fatalf("max-age shelving not observed within %s of simulated clock advance; snooze was not clamped", maxAdvance)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for max-age shelving")
		}
		w.Clock().Advance(step)
		advanced += step
		time.Sleep(2 * time.Millisecond)
	}

	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	rec := shelfRecordAt(t, w.KV, keys[0])
	if rec.Reason != reasonMaxAge {
		t.Fatalf("reason = %q, want %q", rec.Reason, reasonMaxAge)
	}
}

func TestSnoozeWithSpentBudgetShelvesImmediately(t *testing.T) {
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	w.Clock().Advance(2 * time.Minute)
	close(gate)

	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Shelved
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })

	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	rec := shelfRecordAt(t, w.KV, keys[0])
	if rec.Reason != reasonMaxAge {
		t.Fatalf("reason = %q, want %q", rec.Reason, reasonMaxAge)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Shelved
	}); n != 1 {
		t.Fatalf("RunCompleted{Outcome: Shelved} count = %d, want 1", n)
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
			converge.HeaderEnqueuedAt:    w.Clock().Now().UTC().Format(time.RFC3339Nano),
			converge.HeaderAttempt:       "0",
		}
		return converge.Message{Kind: "job", Headers: h, Payload: []byte(`"anon-payload"`)}
	}
	if err := w.MQ.Publish(context.Background(), wInbox("job"), anonMsg()); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Shelved
		}) == 1
	})
	if err := w.MQ.Publish(context.Background(), wInbox("job"), anonMsg()); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Shelved
		}) == 2
	})

	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	var ids []string
	for _, e := range w.Events() {
		if rc, ok := e.(converge.RunCompleted); ok && rc.Outcome == converge.Shelved {
			ids = append(ids, rc.ID)
		}
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("RunCompleted{Outcome: Shelved} IDs = %v, want two identical values", ids)
	}
	if !strings.HasPrefix(ids[0], "anon-") {
		t.Fatalf("MessageID = %q, want prefix %q", ids[0], "anon-")
	}
	if atomic.LoadInt32(&ran) != 2 {
		t.Fatalf("handler ran %d times, want 2", ran)
	}
}

func TestWrongSurfaceSignalShelves(t *testing.T) {
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Outcome == converge.Shelved
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Shelved && rc.Err != nil && strings.Contains(rc.Err.Error(), "check again")
	}); n != 1 {
		t.Fatalf("RunCompleted{Outcome: Shelved} carrying the wrong-surface signal as Err count = %d, want 1", n)
	}
	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	rec := shelfRecordAt(t, w.KV, keys[0])
	if rec.Reason != reasonWrongSurface {
		t.Fatalf("reason = %q, want %q", rec.Reason, reasonWrongSurface)
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

	p := wProducer(t, w1.MQ, w1.Clock())
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
		Clock:     w1.Clock(),
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

	p := wProducer(t, pmq, w1.Clock())
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
	convergetest.Await(t, func() bool { return jobStats(t, rt, "job").InFlight == 2 })

	before := pmq.publishes.Load()

	if runErr := w1.Stop(t); runErr != nil {
		t.Fatalf("Run returned %v, want nil", runErr)
	}
	if republishes := pmq.publishes.Load() - before; republishes > 10 {
		t.Fatalf("republishes during shutdown = %d, want O(1)", republishes)
	}

	w2 := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		Clock:     w1.Clock(),
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
	timeout := 90 * time.Second
	interval := (timeout + visibilityMargin) / 3
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		<-gate
		atomic.AddInt32(&runs, 1)
		return nil
	}, HandleOpts{Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, cmq, w.Clock())
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
	convergetest.AssertStable(t, func() bool { return cmq.count.Load() == receipt })
	w.Clock().Advance(interval - time.Second)
	convergetest.AssertStable(t, func() bool { return cmq.count.Load() == receipt })
	for i := int64(1); i <= 9; i++ {
		want := receipt + i
		convergetest.AdvanceUntil(t, w.Clock(), interval, func() bool { return cmq.count.Load() >= want })
	}
	close(gate)
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	w.Clock().Advance(5 * time.Minute)
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) >= 2 })
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
	p := wProducer(t, w.MQ, w.Clock())
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
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Outcome == converge.Discarded
	}); n != 1 {
		t.Fatalf("RunCompleted{Outcome: Discarded} count = %d, want 1", n)
	}
	convergetest.AdvanceUntil(t, w.Clock(), time.Minute, func() bool {
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

func newLeaseHarnessPair(t *testing.T) (wa, wb *convergetest.Harness, mq *inmem.MQ, lease *inmem.Lease) {
	t.Helper()
	clock := convergetest.NewClock(wstart)
	mq = inmem.NewMQWithClock(clock)
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
	return build(), build(), mq, lease
}

func leaseAcquired(e converge.Event) bool {
	lt, ok := e.(converge.LeaseChanged)
	return ok && lt.Held
}

func TestOnOneReplicaOnlyLeaderConsumes(t *testing.T) {
	wa, wb, mq, _ := newLeaseHarnessPair(t)
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

	p := wProducer(t, mq, wa.Clock())
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
		t.Fatalf("LeaseChanged{Held:true} count across both replicas = %d, want 1", n)
	}
}

func TestLeaseLossCancelsInFlight(t *testing.T) {
	wa, wb, mq, lease := newLeaseHarnessPair(t)
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

	p := wProducer(t, mq, wa.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	lease.Expire("wt/converge/worker/job/lease")

	convergetest.AdvanceUntil(t, wa.Clock(), 3*time.Second, func() bool {
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

	p := wProducer(t, mq, clock)

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
		return ok && rc.Job == "broadcast-fail" && rc.Outcome == converge.Discarded &&
			rc.Err != nil && strings.Contains(rc.Err.Error(), "boom")
	}
	if n := eventCount(wa.Events(), failRunCompleted); n != 1 {
		t.Fatalf("worldA failed RunCompleted{Outcome: Discarded} count = %d, want 1: nothing redelivers a failed broadcast", n)
	}
	if n := eventCount(wb.Events(), failRunCompleted); n != 1 {
		t.Fatalf("worldB failed RunCompleted{Outcome: Discarded} count = %d, want 1: nothing redelivers a failed broadcast", n)
	}

	if err := snoozeTask.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&snoozeA) == 1 && atomic.LoadInt32(&snoozeB) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&snoozeA) == 1 && atomic.LoadInt32(&snoozeB) == 1 })
	snoozeRunCompleted := func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Job == "broadcast-snooze" && rc.Outcome == converge.Discarded
	}
	if n := eventCount(wa.Events(), snoozeRunCompleted); n != 1 {
		t.Fatalf("worldA snoozed RunCompleted{Outcome: Discarded} count = %d, want 1: nothing redelivers a broadcast snooze", n)
	}
	if n := eventCount(wb.Events(), snoozeRunCompleted); n != 1 {
		t.Fatalf("worldB snoozed RunCompleted{Outcome: Discarded} count = %d, want 1: nothing redelivers a broadcast snooze", n)
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
	p := wProducer(t, w.MQ, w.Clock())
	for i := 0; i < 3; i++ {
		if err := tk.Enqueue(context.Background(), p, fmt.Sprintf("m%d", i), EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == 1 })
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) == 2 })
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool { return atomic.LoadInt32(&runs) == 3 })
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
	}, HandleOpts{Concurrency: 2, Timeout: 30 * time.Second, RateLimit: converge.Rate{Events: 1, Per: 2 * time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, w.MQ, w.Clock())
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
	convergetest.AdvanceUntil(t, w.Clock(), 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["a"] == 1 && runs["b"] == 1
	})
	for range 15 {
		w.Clock().Advance(10 * time.Second)
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), 100*time.Millisecond, func() bool {
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
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	convergetest.AdvanceUntil(t, w.Clock(), 3*time.Second, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	})
	convergetest.Await(t, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			lt, ok := e.(converge.LeaseChanged)
			return ok && !lt.Held
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
	p := wProducer(t, w.MQ, w.Clock())
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

func TestNotifyIsAReconcileVerb(t *testing.T) {
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Notify("x"); err == nil || !strings.Contains(err.Error(), "reconcile verb") {
		t.Fatalf("Notify error = %v, want mention of the reconcile surface", err)
	}
}

func TestRunPassNowIsAReconcileVerb(t *testing.T) {
	e, err := newEngine(taskInfo{name: "job", queue: "job", version: 1}, func(context.Context, []byte) error { return nil }, HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "only reconcile jobs sweep") {
		t.Fatalf("Sweep error = %v, want mention that only reconcile jobs sweep", err)
	}
}

func TestTimeoutCancelsTheRun(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	task := NewTask[string]("slow", TaskOpts{})
	deadline := make(chan error, 1)
	if err := Handle(rt, task, func(ctx context.Context, _ string) error {
		<-ctx.Done()
		deadline <- ctx.Err()
		return ctx.Err()
	}, HandleOpts{Timeout: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	if err := task.Enqueue(context.Background(), p, "x", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, h.Clock(), 10*time.Second, func() bool { return len(deadline) > 0 })
	if err := <-deadline; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run ended with %v, want context.DeadlineExceeded", err)
	}
}

func TestBacklogKnownAfterPollingInbox(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{LeaseTTL: 30 * time.Second})
	rt := h.Build(t)
	task := NewTask[string]("job", TaskOpts{})
	if err := Handle(rt, task, func(context.Context, string) error { return nil }, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	if err := task.Enqueue(context.Background(), p, "x", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	s := awaitFreshBacklog(t, h, rt, "job")
	if !s.BacklogKnown || s.Backlog != 0 {
		t.Fatalf("Stats = %+v, want a known backlog of 0: a message that was consumed and acked is not backlog", s)
	}
}

func TestBacklogCountsADeliveredButUnackedMessage(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{LeaseTTL: 30 * time.Second})
	rt := h.Build(t)
	task := NewTask[string]("job", TaskOpts{})
	release := make(chan struct{})
	running := make(chan struct{}, 1)
	if err := Handle(rt, task, func(ctx context.Context, _ string) error {
		running <- struct{}{}
		<-release
		return nil
	}, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	if err := task.Enqueue(context.Background(), p, "x", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
	if s := awaitFreshBacklog(t, h, rt, "job"); !s.BacklogKnown || s.Backlog != 1 {
		t.Fatalf("Stats = %+v, want a known backlog of 1: a message delivered and not yet acked is still outstanding work", s)
	}
	close(release)
	h.Drain(t)
	if s := awaitFreshBacklog(t, h, rt, "job"); !s.BacklogKnown || s.Backlog != 0 {
		t.Fatalf("Stats = %+v, want a known backlog of 0 once the message is acked", s)
	}
}

func awaitFreshBacklog(t *testing.T, h *convergetest.Harness, rt *converge.Runtime, job string) converge.JobStats {
	t.Helper()
	before := jobStats(t, rt, job).BacklogAt
	convergetest.AdvanceUntil(t, h.Clock(), 5*time.Second, func() bool {
		s := jobStats(t, rt, job)
		return s.BacklogKnown && s.BacklogAt.After(before)
	})
	return jobStats(t, rt, job)
}

func TestShelveStopsAfterOneAttempt(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	task := NewTask[string]("charge", TaskOpts{})
	var attempts atomic.Int64
	if err := Handle(rt, task, func(context.Context, string) error {
		attempts.Add(1)
		return Shelve{Reason: "card revoked"}
	}, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	if err := task.Enqueue(context.Background(), p, "o-1", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	shelf, err := ShelfFrom(rt, "charge")
	if err != nil {
		t.Fatal(err)
	}
	recs, err := shelf.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Reason != "card revoked" {
		t.Fatalf("shelved messages = %+v, want one with reason %q", recs, "card revoked")
	}
}

func TestDeliberateShelveDoesNotStampAnErrorItDoesNotHave(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	task := NewTask[string]("charge", TaskOpts{})
	if err := Handle(rt, task, func(_ context.Context, id string) error {
		if id == "o-1" {
			return errors.New("gateway timeout")
		}
		return Shelve{Reason: "card revoked"}
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	if err := task.Enqueue(context.Background(), p, "o-1", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	first := jobStats(t, rt, "charge")
	if first.LastError == nil || first.LastError.Error() != "gateway timeout" {
		t.Fatalf("LastError = %v, want gateway timeout", first.LastError)
	}
	if err := task.Enqueue(context.Background(), p, "o-2", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return jobStats(t, rt, "charge").ConsecutiveFails > first.ConsecutiveFails })
	after := jobStats(t, rt, "charge")
	if after.LastError == nil || after.LastError.Error() != "gateway timeout" {
		t.Fatalf("LastError = %v, want the real error to survive a deliberate shelving", after.LastError)
	}
	if !after.LastErrorAt.Equal(first.LastErrorAt) {
		t.Fatalf("LastErrorAt moved to %v from %v; a Shelve carries no error to timestamp", after.LastErrorAt, first.LastErrorAt)
	}
}

func TestShelveOnBroadcastWithoutKVDropsInsteadOfPanicking(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: wns,
		KV:        func(*convergetest.Clock) converge.KV { return nil },
	})
	rt := w.Build(t)
	tk := NewTask[string]("broadcast-shelve", TaskOpts{})
	var warm, shelves atomic.Int64
	err := Handle(rt, tk, func(_ context.Context, payload string) error {
		if payload == "warm" {
			warm.Add(1)
			return nil
		}
		shelves.Add(1)
		return Shelve{Reason: "card revoked"}
	}, HandleOpts{RunMode: converge.OnAllReplicas})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, w.MQ, w.Clock())
	deadline := time.Now().Add(2 * time.Second)
	for warm.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("broadcast subscription never became live")
		}
		if err := tk.Enqueue(context.Background(), p, "warm", EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return shelves.Load() == 1 })
	convergetest.AssertStable(t, func() bool { return shelves.Load() == 1 })
	if s := jobStats(t, rt, "broadcast-shelve"); s.ShelvedKnown || s.Shelved != 0 {
		t.Fatalf("Stats = %+v, want an unknown shelf depth when there is no shelf to read", s)
	}
}

func TestShelvedReportsTheCurrentShelfDepth(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", LeaseTTL: 30 * time.Second})
	rt := h.Build(t)
	task := NewTask[string]("job", TaskOpts{})
	if err := Handle(rt, task, func(context.Context, string) error {
		return Shelve{Reason: "card revoked"}
	}, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "wt"})
	for _, id := range []string{"o-1", "o-2"} {
		if err := task.Enqueue(context.Background(), p, id, EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	h.Drain(t)
	convergetest.AdvanceUntil(t, h.Clock(), 5*time.Second, func() bool {
		s := jobStats(t, rt, "job")
		return s.ShelvedKnown && s.Shelved == 2
	})
	if s := jobStats(t, rt, "job"); s.ShelvedAt.IsZero() {
		t.Fatalf("Stats = %+v, want the shelf depth dated by the scan that read it", s)
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := shelf.PurgeAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, h.Clock(), 5*time.Second, func() bool {
		s := jobStats(t, rt, "job")
		return s.ShelvedKnown && s.Shelved == 0
	})
	if s := jobStats(t, rt, "job"); s.Shelved != 0 {
		t.Fatalf("Shelved = %d after the shelf was purged, want 0: Shelved is a depth, not a run count", s.Shelved)
	}
}

func TestDestroyedJobStopsReportingABacklog(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", LeaseTTL: 30 * time.Second})
	rt := w.Build(t)
	tk := NewTask[string]("migration", TaskOpts{})
	cutover := w.Clock().Now().Add(time.Hour)
	err := Handle(rt, tk, func(context.Context, string) error { return nil },
		HandleOpts{RunMode: converge.OnOneReplica, Until: converge.Deadline(cutover)})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	convergetest.Await(t, func() bool { return jobStats(t, rt, "migration").BacklogKnown })

	w.Clock().Advance(2 * time.Hour)
	convergetest.Await(t, func() bool { return jobStats(t, rt, "migration").State == converge.Destroyed })
	convergetest.Await(t, func() bool {
		s := jobStats(t, rt, "migration")
		return !s.BacklogKnown && s.BacklogAt.IsZero() && !s.ShelvedKnown && s.ShelvedAt.IsZero()
	})
}

func TestDeadlineDestroysTheJob(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", LeaseTTL: 30 * time.Second})
	rt := w.Build(t)
	tk := NewTask[string]("migration", TaskOpts{})
	var runs atomic.Int64
	cutover := w.Clock().Now().Add(time.Hour)
	err := Handle(rt, tk, func(context.Context, string) error {
		runs.Add(1)
		return nil
	}, HandleOpts{RunMode: converge.OnOneReplica, Until: converge.Deadline(cutover)})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "before", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return runs.Load() == 1 })

	w.Clock().Advance(2 * time.Hour)
	convergetest.Await(t, func() bool { return jobStats(t, rt, "migration").State == converge.Destroyed })

	before := runs.Load()
	if err := tk.Enqueue(context.Background(), p, "after", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { return runs.Load() == before })
}

func TestStopKeyDestroysTheJob(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt", LeaseTTL: 30 * time.Second})
	rt := w.Build(t)
	tk := NewTask[string]("migration", TaskOpts{})
	var runs atomic.Int64
	stopKey := keys.Tombstone("wt", "migration")
	err := Handle(rt, tk, func(context.Context, string) error {
		runs.Add(1)
		return nil
	}, HandleOpts{RunMode: converge.OnOneReplica, Until: converge.StopKey(stopKey)})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "before", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return runs.Load() == 1 })

	if err := w.KV.Set(context.Background(), stopKey, []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	w.Clock().Advance(20 * time.Second)
	convergetest.Await(t, func() bool { return jobStats(t, rt, "migration").State == converge.Destroyed })

	before := runs.Load()
	if err := tk.Enqueue(context.Background(), p, "after", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { return runs.Load() == before })
}

func TestDeadlineDestroysACompetingWorker(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: wns})
	rt := w.Build(t)
	tk := NewTask[string]("migration", TaskOpts{})
	var runs atomic.Int64
	cutover := w.Clock().Now().Add(time.Hour)
	err := Handle(rt, tk, func(context.Context, string) error {
		runs.Add(1)
		return nil
	}, HandleOpts{Until: converge.Deadline(cutover)})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	if mode := jobStats(t, rt, "migration").RunMode; mode != converge.Competing {
		t.Fatalf("RunMode = %v, want the worker default %v", mode, converge.Competing)
	}
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "before", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return runs.Load() == 1 })

	convergetest.AdvanceUntil(t, w.Clock(), destroyCheckInterval, func() bool {
		return jobStats(t, rt, "migration").State == converge.Destroyed
	})

	before := runs.Load()
	if err := tk.Enqueue(context.Background(), p, "after", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { return runs.Load() == before })
}

func TestJobDestroyedReportsTheConditionThatFired(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: wns})
	rt := w.Build(t)
	tk := NewTask[string]("migration", TaskOpts{})
	cutover := w.Clock().Now().Add(365 * 24 * time.Hour)
	err := Handle(rt, tk, func(context.Context, string) error { return nil },
		HandleOpts{Until: converge.Deadline(cutover)})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	tombstone := keys.Tombstone(wns, "migration")
	if err := w.KV.Set(context.Background(), tombstone, []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), destroyCheckInterval, func() bool {
		return jobStats(t, rt, "migration").State == converge.Destroyed
	})

	var causes []string
	for _, e := range w.Events() {
		if d, ok := e.(converge.JobDestroyed); ok && d.Job == "migration" {
			causes = append(causes, d.Cause.String())
		}
	}
	want := converge.StopKey(tombstone).String()
	if len(causes) != 1 || causes[0] != want {
		t.Fatalf("JobDestroyed causes = %v, want exactly one %q", causes, want)
	}
}

func TestDestructionCancelsInFlightRunsWithoutDraining(t *testing.T) {
	drain := 365 * 24 * time.Hour
	w := convergetest.NewWith(t, convergetest.Options{Namespace: wns, DrainTimeout: drain})
	rt := w.Build(t)
	tk := NewTask[string]("migration", TaskOpts{})
	stopKey := keys.Tombstone(wns, "migration")
	entered := make(chan struct{}, 1)
	ended := make(chan error, 1)
	err := Handle(rt, tk, func(ctx context.Context, _ string) error {
		entered <- struct{}{}
		<-ctx.Done()
		ended <- ctx.Err()
		return ctx.Err()
	}, HandleOpts{Until: converge.StopKey(stopKey)})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	startedAt := w.Clock().Now()
	p := wProducer(t, w.MQ, w.Clock())
	if err := tk.Enqueue(context.Background(), p, "x", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return len(entered) > 0 })

	if err := w.KV.Set(context.Background(), stopKey, []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, w.Clock(), destroyCheckInterval, func() bool { return len(ended) > 0 })
	if got := <-ended; !errors.Is(got, context.Canceled) {
		t.Fatalf("in-flight run ended with %v, want context.Canceled", got)
	}
	if elapsed := w.Clock().Now().Sub(startedAt); elapsed >= drain {
		t.Fatalf("run was cancelled after %s, want cancellation without waiting out DrainTimeout %s", elapsed, drain)
	}
	if state := jobStats(t, rt, "migration").State; state != converge.Destroyed {
		t.Fatalf("State = %v, want %v", state, converge.Destroyed)
	}
}

func TestOutcomesAreReported(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	task := NewTask[string]("mixed", TaskOpts{})
	if err := Handle(rt, task, func(_ context.Context, s string) error {
		switch s {
		case "ok":
			return nil
		case "gone":
			return Discard{Reason: "unsubscribed"}
		default:
			return Shelve{Reason: "bad"}
		}
	}, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p, _ := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	for _, s := range []string{"ok", "gone", "bad"} {
		if err := task.Enqueue(context.Background(), p, s, EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	h.Drain(t)
	want := map[converge.Outcome]int{converge.Succeeded: 1, converge.Discarded: 1, converge.Shelved: 1}
	got := map[converge.Outcome]int{}
	for _, e := range h.Events() {
		if rc, ok := e.(converge.RunCompleted); ok {
			got[rc.Outcome]++
		}
	}
	for outcome, n := range want {
		if got[outcome] != n {
			t.Fatalf("%s = %d, want %d (all: %v)", outcome, got[outcome], n, got)
		}
	}
}

func TestFailingPrunesExpiredEntriesOnRead(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	e := &engine{deps: converge.JobDeps{Clock: clock}}
	e.markRetrying("a", time.Second)
	e.markRetrying("b", time.Minute)
	if got := e.failing(); got != 2 {
		t.Fatalf("failing = %d, want 2", got)
	}
	clock.Advance(time.Second)
	if got := e.failing(); got != 1 {
		t.Fatalf("failing after first expiry = %d, want 1", got)
	}
	if _, ok := e.retrying["a"]; ok {
		t.Fatal("expired entry must be pruned from the map, not just excluded from the count")
	}
}

func TestFailingCapsAtBoundAndDropsNewWrites(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	e := &engine{deps: converge.JobDeps{Clock: clock}}
	for i := 0; i < workerRetryingBound; i++ {
		e.markRetrying(strconv.Itoa(i), time.Hour)
	}
	if got := e.failing(); got != workerRetryingBound {
		t.Fatalf("failing = %d, want the bound %d", got, workerRetryingBound)
	}
	e.markRetrying("overflow", time.Hour)
	if got := e.failing(); got != workerRetryingBound {
		t.Fatalf("failing after overflow write = %d, want still %d (dropped)", got, workerRetryingBound)
	}
	if _, ok := e.retrying["overflow"]; ok {
		t.Fatal("write past the bound must be dropped, not recorded")
	}
}

func TestStatsBeforeRunReportsInsteadOfPanicking(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: wns})
	rt := w.Build(t)
	tk := NewTask[string]("unstarted", TaskOpts{})
	if err := Handle(rt, tk, func(ctx context.Context, s string) error { return nil }, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	stats := rt.Stats()
	if len(stats) != 1 {
		t.Fatalf("got %d stats, want 1", len(stats))
	}
	if stats[0].State != converge.NotStarted {
		t.Fatalf("state is %v, want NotStarted", stats[0].State)
	}
	if stats[0].Failing != 0 {
		t.Fatalf("failing is %d, want 0", stats[0].Failing)
	}
}
