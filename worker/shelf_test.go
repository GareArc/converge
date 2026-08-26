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

func TestShelfListAndGetSeeRealShelvedMessage(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		return errors.New("boom")
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, w.MQ, w.Clock)
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return len(shelfKeys(t, w.KV, "job")) == 1 })

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	list, err := shelf.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("List = %v, want exactly 1 entry", list)
	}
	entry := list[0]
	if entry.Task != "job" {
		t.Fatalf("Task = %q, want %q", entry.Task, "job")
	}
	if entry.Queue != wInbox("job") {
		t.Fatalf("Queue = %q, want %q", entry.Queue, wInbox("job"))
	}
	if entry.Reason != reasonMaxAttempts {
		t.Fatalf("Reason = %q, want %q", entry.Reason, reasonMaxAttempts)
	}
	if entry.Error == "" {
		t.Fatal("want non-empty Error")
	}
	if entry.MessageID == "" {
		t.Fatal("want non-empty MessageID")
	}
	var payload string
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload != "hello" {
		t.Fatalf("payload = %q, want %q", payload, "hello")
	}

	got, err := shelf.Get(context.Background(), entry.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, entry) {
		t.Fatalf("Get = %+v, want %+v", got, entry)
	}
}

func TestShelfGetAbsentReturnsNotFound(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	_, err = shelf.Get(context.Background(), "nope")
	if !errors.Is(err, ErrNotShelved) {
		t.Fatalf("err = %v, want ErrNotShelved", err)
	}
}

func TestShelfRequeueAbsentReturnsNotFound(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	err = shelf.Requeue(context.Background(), "nope")
	if !errors.Is(err, ErrNotShelved) {
		t.Fatalf("err = %v, want ErrNotShelved", err)
	}
}

func TestShelfListSurfacesUndecodableRecord(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	key := "wt/converge/worker/job/shelf/badid"
	if err := w.KV.Set(context.Background(), key, []byte(`{not valid json`), 0); err != nil {
		t.Fatal(err)
	}
	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	list, err := shelf.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("List = %v, want exactly 1 entry", list)
	}
	entry := list[0]
	if entry.MessageID != "badid" {
		t.Fatalf("MessageID = %q, want %q", entry.MessageID, "badid")
	}
	if entry.Reason != reasonUndecodableRecord {
		t.Fatalf("Reason = %q, want %q", entry.Reason, reasonUndecodableRecord)
	}
	if entry.Error == "" {
		t.Fatal("want non-empty decode Error")
	}

	got, err := shelf.Get(context.Background(), "badid")
	if err != nil {
		t.Fatal(err)
	}
	if got.Reason != reasonUndecodableRecord || got.MessageID != "badid" {
		t.Fatalf("Get = %+v, want an undecodable-record entry", got)
	}
}

func TestShelfRequeueFullLoopSucceeds(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var metas []Meta
	var fail atomic.Bool
	fail.Store(true)
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		metas = append(metas, meta)
		mu.Unlock()
		if fail.Load() {
			return errors.New("boom")
		}
		return nil
	}, HandleOpts{Retry: RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)
	p := wProducer(t, w.MQ, w.Clock)
	if err := tk.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return len(shelfKeys(t, w.KV, "job")) == 1 })

	keys := shelfKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("shelf keys = %v, want exactly 1", keys)
	}
	rec := shelfRecordAt(t, w.KV, keys[0])

	mu.Lock()
	firstMessageID := metas[0].MessageID
	mu.Unlock()
	if rec.MessageID != firstMessageID {
		t.Fatalf("shelf record message id = %q, want %q", rec.MessageID, firstMessageID)
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(false)
	w.Clock.Advance(time.Hour)
	wantEnqueuedAt := w.Clock.Now()
	if err := shelf.Requeue(context.Background(), rec.MessageID); err != nil {
		t.Fatal(err)
	}

	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(metas) == 2
	})
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(metas) == 2
	})
	mu.Lock()
	second := metas[1]
	mu.Unlock()
	if second.MessageID != firstMessageID {
		t.Fatalf("requeued message id = %q, want %q", second.MessageID, firstMessageID)
	}
	if second.Attempt != 1 {
		t.Fatalf("requeued attempt = %d, want 1 (attempt header must be re-seeded)", second.Attempt)
	}
	if !second.EnqueuedAt.Equal(wantEnqueuedAt) {
		t.Fatalf("requeued enqueued-at = %v, want %v", second.EnqueuedAt, wantEnqueuedAt)
	}
	if _, ok := second.Headers[converge.HeaderSnoozes]; ok {
		t.Fatal("snoozes header must be stripped on requeue")
	}
	if got := second.Headers[converge.HeaderAttempt]; got != "0" {
		t.Fatalf("attempt header on requeue = %q, want %q (requeue resets to base attempt, not absence)", got, "0")
	}
	if keys := shelfKeys(t, w.KV, "job"); len(keys) != 0 {
		t.Fatalf("shelf keys after requeue = %v, want none", keys)
	}
	if n := eventCount(w.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err == nil
	}); n != 1 {
		t.Fatalf("successful RunCompleted count = %d, want 1", n)
	}
}

func TestShelfRequeueWithoutMessageIDStaysAnonymous(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	rec := ShelvedMessage{
		Task:       "job",
		Queue:      wInbox("job"),
		MessageID:  "anon-deadbeef",
		Attempt:    1,
		Reason:     reasonMaxAttempts,
		Error:      "boom",
		EnqueuedAt: w.Clock.Now(),
		ShelvedAt:  w.Clock.Now(),
		Headers: map[string]string{
			converge.HeaderSchemaVersion: "1",
			converge.HeaderEnqueuedAt:    w.Clock.Now().UTC().Format(time.RFC3339Nano),
			converge.HeaderAttempt:       "0",
		},
		Payload: []byte(`"hello"`),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	key := "wt/converge/worker/job/shelf/" + rec.MessageID
	if err := w.KV.Set(context.Background(), key, raw, 0); err != nil {
		t.Fatal(err)
	}

	ch := startConsumer(t, w.MQ, wInbox("job"))
	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := shelf.Requeue(context.Background(), rec.MessageID); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
	m := (<-ch).Message()
	if _, ok := m.Headers[converge.HeaderMessageID]; ok {
		t.Fatalf("republished message has message-id header %q, want none", m.Headers[converge.HeaderMessageID])
	}
	if got := m.Headers[converge.HeaderAttempt]; got != "0" {
		t.Fatalf("attempt header = %q, want %q (requeue resets to base attempt, not absence)", got, "0")
	}
	if got := string(m.Payload); got != `"hello"` {
		t.Fatalf("payload = %q, want %q", got, `"hello"`)
	}
	if keys := shelfKeys(t, w.KV, "job"); len(keys) != 0 {
		t.Fatalf("shelf keys after requeue = %v, want none", keys)
	}
}

type duplicateScanKV struct {
	inner converge.KV
	calls int
}

func (k *duplicateScanKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return k.inner.Get(ctx, key)
}

func (k *duplicateScanKV) SetCAS(ctx context.Context, key string, old, new []byte) (bool, error) {
	return k.inner.SetCAS(ctx, key, old, new)
}

func (k *duplicateScanKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return k.inner.Set(ctx, key, val, ttl)
}

func (k *duplicateScanKV) Delete(ctx context.Context, key string) error {
	return k.inner.Delete(ctx, key)
}

func (k *duplicateScanKV) Scan(ctx context.Context, prefix, cursor string) ([]string, string, error) {
	k.calls++
	keys, _, err := k.inner.Scan(ctx, prefix, "")
	if err != nil {
		return nil, "", err
	}
	if len(keys) == 0 {
		return nil, "", nil
	}
	if k.calls == 1 {
		return keys[:1], "page2", nil
	}
	return keys[:1], "", nil
}

func TestShelfListDedupesAcrossScanPages(t *testing.T) {
	var dkv *duplicateScanKV
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		KV: func(clock *convergetest.Clock) converge.KV {
			dkv = &duplicateScanKV{inner: inmem.NewKVWithClock(clock)}
			return dkv
		},
	})
	rt := w.Build(t)
	ctx := context.Background()
	rec := ShelvedMessage{
		Task:       "job",
		Queue:      wInbox("job"),
		MessageID:  "id-0",
		Attempt:    1,
		Reason:     reasonMaxAttempts,
		EnqueuedAt: w.Clock.Now(),
		ShelvedAt:  w.Clock.Now(),
		Payload:    []byte(`"hello"`),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	key := "wt/converge/worker/job/shelf/id-0"
	if err := dkv.Set(ctx, key, raw, 0); err != nil {
		t.Fatal(err)
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	list, err := shelf.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("List = %v, want exactly 1 entry (Scan returned the same key on two consecutive pages)", list)
	}
}

func TestShelfPurgeAbsentIsNil(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := shelf.Purge(context.Background(), "nope"); err != nil {
		t.Fatalf("Purge absent = %v, want nil", err)
	}
}

func TestShelfPurgeAllRemovesEverything(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		rec := ShelvedMessage{
			Task:       "job",
			Queue:      wInbox("job"),
			MessageID:  fmt.Sprintf("id-%d", i),
			Attempt:    1,
			Reason:     reasonMaxAttempts,
			EnqueuedAt: w.Clock.Now(),
			ShelvedAt:  w.Clock.Now(),
			Payload:    []byte(`"hello"`),
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("wt/converge/worker/job/shelf/id-%d", i)
		if err := w.KV.Set(ctx, key, raw, 0); err != nil {
			t.Fatal(err)
		}
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := shelf.PurgeAll(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := shelfKeys(t, w.KV, "job"); len(keys) != 0 {
		t.Fatalf("shelf keys after PurgeAll = %v, want none", keys)
	}
}

func TestShelfFromNilRuntimeErrors(t *testing.T) {
	shelf, err := ShelfFrom(nil, "job")
	if err == nil || shelf != nil {
		t.Fatalf("shelf, err = %v, %v, want error and nil Shelf", shelf, err)
	}
}

func TestShelfFromUnbuiltRuntimeErrors(t *testing.T) {
	shelf, err := ShelfFrom(&converge.Runtime{}, "job")
	if err == nil || shelf != nil {
		t.Fatalf("shelf, err = %v, %v, want error and nil Shelf", shelf, err)
	}
}

func TestShelfFromEmptyJobErrors(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	shelf, err := ShelfFrom(rt, "")
	if err == nil || shelf != nil {
		t.Fatalf("shelf, err = %v, %v, want error and nil Shelf", shelf, err)
	}
}

func TestShelfFromUnregisteredJobSucceeds(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	shelf, err := ShelfFrom(rt, "ghost-job")
	if err != nil {
		t.Fatal(err)
	}
	if shelf == nil {
		t.Fatal("want non-nil Shelf")
	}
	list, err := shelf.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("List = %v, want empty", list)
	}
}

func TestShelfRequeuePublishFailureLeavesRecordIntact(t *testing.T) {
	var cmq *convergetest.MQ
	w := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		MQ: func(clock *convergetest.Clock) converge.MQ {
			cmq = convergetest.WrapMQ(inmem.NewMQWithClock(clock))
			return cmq
		},
	})
	rt := w.Build(t)
	rec := ShelvedMessage{
		Task:       "job",
		Queue:      wInbox("job"),
		MessageID:  "msg-1",
		Attempt:    1,
		Reason:     reasonMaxAttempts,
		Error:      "boom",
		EnqueuedAt: w.Clock.Now(),
		ShelvedAt:  w.Clock.Now(),
		Headers: map[string]string{
			converge.HeaderMessageID:     "msg-1",
			converge.HeaderSchemaVersion: "1",
			converge.HeaderEnqueuedAt:    w.Clock.Now().UTC().Format(time.RFC3339Nano),
			converge.HeaderAttempt:       "0",
		},
		Payload: []byte(`"hello"`),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	key := "wt/converge/worker/job/shelf/" + rec.MessageID
	if err := w.KV.Set(context.Background(), key, raw, 0); err != nil {
		t.Fatal(err)
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	publishErr := errors.New("transient publish failure")
	cmq.FailNextPublish(publishErr)

	err = shelf.Requeue(context.Background(), rec.MessageID)
	if !errors.Is(err, publishErr) {
		t.Fatalf("Requeue err = %v, want %v", err, publishErr)
	}

	got, err := shelf.Get(context.Background(), rec.MessageID)
	if err != nil {
		t.Fatalf("record missing after failed publish: %v", err)
	}
	if got.MessageID != rec.MessageID {
		t.Fatalf("got.MessageID = %q, want %q", got.MessageID, rec.MessageID)
	}
	if n := len(cmq.Published(wInbox("job"))); n != 0 {
		t.Fatalf("Published(job) = %d messages, want 0 (publish must have failed before the record was deleted)", n)
	}
}

func TestShelfListAndPurgeAllFollowScanCursorPastOnePage(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	ctx := context.Background()
	const total = 150
	for i := 0; i < total; i++ {
		rec := ShelvedMessage{
			Task:       "job",
			Queue:      wInbox("job"),
			MessageID:  fmt.Sprintf("id-%03d", i),
			Attempt:    1,
			Reason:     reasonMaxAttempts,
			EnqueuedAt: w.Clock.Now(),
			ShelvedAt:  w.Clock.Now(),
			Payload:    []byte(`"hello"`),
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("wt/converge/worker/job/shelf/id-%03d", i)
		if err := w.KV.Set(ctx, key, raw, 0); err != nil {
			t.Fatal(err)
		}
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	list, err := shelf.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != total {
		t.Fatalf("List returned %d entries, want %d (cursor must be followed past the inmem Scan page size)", len(list), total)
	}
	seen := map[string]bool{}
	for _, rec := range list {
		seen[rec.MessageID] = true
	}
	if len(seen) != total {
		t.Fatalf("List returned %d distinct message ids, want %d", len(seen), total)
	}

	if err := shelf.PurgeAll(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := shelfKeys(t, w.KV, "job"); len(keys) != 0 {
		t.Fatalf("shelf keys after PurgeAll = %v, want none (cursor must be followed past the inmem Scan page size)", keys)
	}
}

type failingDeleteKV struct {
	inner     converge.KV
	failAfter int

	mu      sync.Mutex
	deletes int
}

func (k *failingDeleteKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return k.inner.Get(ctx, key)
}

func (k *failingDeleteKV) SetCAS(ctx context.Context, key string, old, new []byte) (bool, error) {
	return k.inner.SetCAS(ctx, key, old, new)
}

func (k *failingDeleteKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return k.inner.Set(ctx, key, val, ttl)
}

func (k *failingDeleteKV) Delete(ctx context.Context, key string) error {
	k.mu.Lock()
	k.deletes++
	n := k.deletes
	k.mu.Unlock()
	if n == k.failAfter {
		return errors.New("kv delete failed")
	}
	return k.inner.Delete(ctx, key)
}

func (k *failingDeleteKV) Scan(ctx context.Context, prefix, cursor string) ([]string, string, error) {
	return k.inner.Scan(ctx, prefix, cursor)
}

func TestShelfPurgeAllPartialFailureLeavesRemainder(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	fk := &failingDeleteKV{inner: inmem.NewKVWithClock(clock), failAfter: 2}
	rt, err := converge.New(converge.Options{Namespace: "wt", KV: fk, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		rec := ShelvedMessage{
			Task:       "job",
			Queue:      wInbox("job"),
			MessageID:  fmt.Sprintf("id-%d", i),
			Attempt:    1,
			Reason:     reasonMaxAttempts,
			EnqueuedAt: clock.Now(),
			ShelvedAt:  clock.Now(),
			Payload:    []byte(`"hello"`),
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("wt/converge/worker/job/shelf/id-%d", i)
		if err := fk.Set(ctx, key, raw, 0); err != nil {
			t.Fatal(err)
		}
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := shelf.PurgeAll(ctx); err == nil {
		t.Fatal("want a non-nil error from PurgeAll on a mid-sweep delete failure")
	}
	if keys := shelfKeys(t, fk, "job"); len(keys) != 2 {
		t.Fatalf("shelf keys after partial PurgeAll = %v, want exactly 2 remaining (one delete before the injected failure)", keys)
	}
}

func TestShelfRequeueNoMQErrors(t *testing.T) {
	clock := convergetest.NewClock(wstart)
	kv := inmem.NewKVWithClock(clock)
	rt, err := converge.New(converge.Options{Namespace: "wt", KV: kv, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rec := ShelvedMessage{
		Task:       "job",
		Queue:      wInbox("job"),
		MessageID:  "msg-1",
		Attempt:    1,
		Reason:     reasonMaxAttempts,
		EnqueuedAt: clock.Now(),
		ShelvedAt:  clock.Now(),
		Payload:    []byte(`"hello"`),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, "wt/converge/worker/job/shelf/msg-1", raw, 0); err != nil {
		t.Fatal(err)
	}

	shelf, err := ShelfFrom(rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	err = shelf.Requeue(ctx, "msg-1")
	if err == nil || !strings.Contains(err.Error(), "Options.MQ") {
		t.Fatalf("err = %v, want mention of no MQ", err)
	}

	got, err := shelf.Get(ctx, "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != "msg-1" {
		t.Fatalf("record must remain after a no-MQ requeue error, got %+v", got)
	}
}
