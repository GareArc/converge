package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

func TestDLQListAndGetSeeRealDeadLetter(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
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
	await(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})

	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	list, err := dlq.List(context.Background())
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
	if entry.Queue != "job" {
		t.Fatalf("Queue = %q, want %q", entry.Queue, "job")
	}
	if entry.Reason != converge.DeadLetterMaxAttempts.String() {
		t.Fatalf("Reason = %q, want %q", entry.Reason, converge.DeadLetterMaxAttempts.String())
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

	got, err := dlq.Get(context.Background(), entry.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, entry) {
		t.Fatalf("Get = %+v, want %+v", got, entry)
	}
}

func TestDLQGetAbsentReturnsNotFound(t *testing.T) {
	w := newWorld(t)
	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	_, err = dlq.Get(context.Background(), "nope")
	if !errors.Is(err, ErrDeadLetterNotFound) {
		t.Fatalf("err = %v, want ErrDeadLetterNotFound", err)
	}
}

func TestDLQRequeueAbsentReturnsNotFound(t *testing.T) {
	w := newWorld(t)
	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	err = dlq.Requeue(context.Background(), "nope")
	if !errors.Is(err, ErrDeadLetterNotFound) {
		t.Fatalf("err = %v, want ErrDeadLetterNotFound", err)
	}
}

func TestDLQListSurfacesUndecodableRecord(t *testing.T) {
	w := newWorld(t)
	key := "wt/converge/worker/job/dlq/badid"
	if err := w.kv.Set(context.Background(), key, []byte(`{not valid json`), 0); err != nil {
		t.Fatal(err)
	}
	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	list, err := dlq.List(context.Background())
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

	got, err := dlq.Get(context.Background(), "badid")
	if err != nil {
		t.Fatal(err)
	}
	if got.Reason != reasonUndecodableRecord || got.MessageID != "badid" {
		t.Fatalf("Get = %+v, want an undecodable-record entry", got)
	}
}

func TestDLQRequeueFullLoopSucceeds(t *testing.T) {
	w := newWorld(t)
	tk := NewTask[string]("job", TaskOpts{})
	var mu sync.Mutex
	var metas []Meta
	var fail atomic.Bool
	fail.Store(true)
	err := Handle(w.rt, tk, func(ctx context.Context, payload string) error {
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
			_, ok := e.(converge.MessageDeadLettered)
			return ok
		}) == 1
	})

	keys := dlqKeys(t, w, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w, keys[0])

	mu.Lock()
	firstMessageID := metas[0].MessageID
	mu.Unlock()
	if rec.MessageID != firstMessageID {
		t.Fatalf("dlq record message id = %q, want %q", rec.MessageID, firstMessageID)
	}

	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(false)
	w.clock.Advance(time.Hour)
	wantEnqueuedAt := w.clock.Now()
	if err := dlq.Requeue(context.Background(), rec.MessageID); err != nil {
		t.Fatal(err)
	}

	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(metas) == 2
	})
	assertStable(t, func() bool {
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
		t.Fatalf("requeued attempt = %d, want 1 (attempt header must be stripped)", second.Attempt)
	}
	if !second.EnqueuedAt.Equal(wantEnqueuedAt) {
		t.Fatalf("requeued enqueued-at = %v, want %v", second.EnqueuedAt, wantEnqueuedAt)
	}
	if _, ok := second.Headers[converge.HeaderSnoozes]; ok {
		t.Fatal("snoozes header must be stripped on requeue")
	}
	if _, ok := second.Headers[converge.HeaderAttempt]; ok {
		t.Fatal("attempt header must be stripped on requeue")
	}
	if keys := dlqKeys(t, w, "job"); len(keys) != 0 {
		t.Fatalf("dlq keys after requeue = %v, want none", keys)
	}
	if n := w.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err == nil
	}); n != 1 {
		t.Fatalf("successful RunCompleted count = %d, want 1", n)
	}
}

func TestDLQRequeueWithoutMessageIDStaysAnonymous(t *testing.T) {
	w := newWorld(t)
	rec := dlqRecord{
		Task:           "job",
		Queue:          "job",
		MessageID:      "anon-deadbeef",
		Attempt:        1,
		Reason:         converge.DeadLetterMaxAttempts.String(),
		Error:          "boom",
		EnqueuedAt:     w.clock.Now(),
		DeadLetteredAt: w.clock.Now(),
		Headers: map[string]string{
			converge.HeaderSchemaVersion: "1",
			converge.HeaderEnqueuedAt:    w.clock.Now().UTC().Format(time.RFC3339Nano),
			converge.HeaderAttempt:       "0",
		},
		Payload: []byte(`"hello"`),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	key := "wt/converge/worker/job/dlq/" + rec.MessageID
	if err := w.kv.Set(context.Background(), key, raw, 0); err != nil {
		t.Fatal(err)
	}

	ch := startConsumer(t, w.mq, "job")
	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := dlq.Requeue(context.Background(), rec.MessageID); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return len(ch) >= 1 })
	m := (<-ch).Message()
	if _, ok := m.Headers[converge.HeaderMessageID]; ok {
		t.Fatalf("republished message has message-id header %q, want none", m.Headers[converge.HeaderMessageID])
	}
	if _, ok := m.Headers[converge.HeaderAttempt]; ok {
		t.Fatal("attempt header must be stripped")
	}
	if got := string(m.Payload); got != `"hello"` {
		t.Fatalf("payload = %q, want %q", got, `"hello"`)
	}
	if keys := dlqKeys(t, w, "job"); len(keys) != 0 {
		t.Fatalf("dlq keys after requeue = %v, want none", keys)
	}
}

func TestDLQPurgeAbsentIsNil(t *testing.T) {
	w := newWorld(t)
	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	if err := dlq.Purge(context.Background(), "nope"); err != nil {
		t.Fatalf("Purge absent = %v, want nil", err)
	}
}

func TestDLQPurgeAllReturnsCount(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		rec := dlqRecord{
			Task:           "job",
			Queue:          "job",
			MessageID:      fmt.Sprintf("id-%d", i),
			Attempt:        1,
			Reason:         converge.DeadLetterMaxAttempts.String(),
			EnqueuedAt:     w.clock.Now(),
			DeadLetteredAt: w.clock.Now(),
			Payload:        []byte(`"hello"`),
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("wt/converge/worker/job/dlq/id-%d", i)
		if err := w.kv.Set(ctx, key, raw, 0); err != nil {
			t.Fatal(err)
		}
	}

	dlq, err := DLQFrom(w.rt, "job")
	if err != nil {
		t.Fatal(err)
	}
	n, err := dlq.PurgeAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("PurgeAll count = %d, want 3", n)
	}
	if keys := dlqKeys(t, w, "job"); len(keys) != 0 {
		t.Fatalf("dlq keys after PurgeAll = %v, want none", keys)
	}
}

func TestDLQFromNilRuntimeErrors(t *testing.T) {
	var rt *converge.Runtime
	dlq, err := DLQFrom(rt, "job")
	if err == nil || dlq != nil {
		t.Fatalf("dlq, err = %v, %v, want error and nil DLQ", dlq, err)
	}
}

func TestDLQFromEmptyJobErrors(t *testing.T) {
	w := newWorld(t)
	dlq, err := DLQFrom(w.rt, "")
	if err == nil || dlq != nil {
		t.Fatalf("dlq, err = %v, %v, want error and nil DLQ", dlq, err)
	}
}

func TestDLQFromUnregisteredJobSucceeds(t *testing.T) {
	w := newWorld(t)
	dlq, err := DLQFrom(w.rt, "ghost-job")
	if err != nil {
		t.Fatal(err)
	}
	if dlq == nil {
		t.Fatal("want non-nil DLQ")
	}
	list, err := dlq.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("List = %v, want empty", list)
	}
}
