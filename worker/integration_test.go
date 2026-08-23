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
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

type invitePayload struct {
	Email string `json:"email"`
	Team  string `json:"team"`
}

const (
	cleanInviteEmail     = "clean@example.com"
	rejectedInviteEmail  = "rejected@example.com"
	throttledInviteEmail = "throttled@example.com"
	hardFailInviteEmail  = "hardfail@example.com"
)

type inviteMailer struct {
	mu    sync.Mutex
	calls map[string]int
}

func newInviteMailer() *inviteMailer { return &inviteMailer{calls: map[string]int{}} }

func (m *inviteMailer) send(email string) {
	m.mu.Lock()
	m.calls[email]++
	m.mu.Unlock()
}

func (m *inviteMailer) count(email string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[email]
}

func TestScenarioCEndToEnd(t *testing.T) {
	var mq converge.MQ
	consumer := convergetest.NewWith(t, convergetest.Options{
		Namespace: "wt",
		MQ: func(clock *convergetest.Clock) converge.MQ {
			mq = inmem.NewMQWithClock(clock)
			return mq
		},
	})
	consumerRt := consumer.Build(t)

	producerRt, err := converge.New(converge.Options{MQ: mq, Clock: consumer.Clock})
	if err != nil {
		t.Fatal(err)
	}

	tk := NewTask[invitePayload]("send-invite", TaskOpts{Version: 1})
	mailer := newInviteMailer()

	var mu sync.Mutex
	messageIDs := map[string]string{}
	var throttleAttempts []int
	var hardFailRuns, escalations int32

	err = Handle(consumerRt, tk, func(ctx context.Context, payload invitePayload) error {
		meta, _ := MetaFromContext(ctx)
		mu.Lock()
		messageIDs[payload.Email] = meta.MessageID
		mu.Unlock()
		switch payload.Email {
		case cleanInviteEmail:
			mailer.send(payload.Email)
			return nil
		case rejectedInviteEmail:
			return Discard{Reason: "rejected address"}
		case throttledInviteEmail:
			mu.Lock()
			throttleAttempts = append(throttleAttempts, meta.Attempt)
			n := len(throttleAttempts)
			mu.Unlock()
			if n == 1 {
				return Snooze{In: 30 * time.Second}
			}
			mailer.send(payload.Email)
			return nil
		case hardFailInviteEmail:
			atomic.AddInt32(&hardFailRuns, 1)
			if meta.Attempt == meta.MaxAttempts {
				atomic.AddInt32(&escalations, 1)
			}
			return errors.New("smtp: rejected by relay")
		default:
			return fmt.Errorf("worker: test: unexpected email %q", payload.Email)
		}
	}, HandleOpts{
		Retry:       RetryPolicy{MaxAttempts: 3, MinBackoff: time.Second},
		Concurrency: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.Runtime(t)

	producer, err := ProducerFrom(producerRt)
	if err != nil {
		t.Fatal(err)
	}

	if err := tk.Enqueue(context.Background(), producer, invitePayload{Email: cleanInviteEmail, Team: "eng"}, EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), producer, invitePayload{Email: rejectedInviteEmail, Team: "eng"}, EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return mailer.count(cleanInviteEmail) == 1 })
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := messageIDs[rejectedInviteEmail]
		return ok
	})
	convergetest.AssertStable(t, func() bool { return mailer.count(cleanInviteEmail) == 1 })

	mu.Lock()
	cleanID, rejectID := messageIDs[cleanInviteEmail], messageIDs[rejectedInviteEmail]
	mu.Unlock()

	if n := eventCount(consumer.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.ID == cleanID && rc.Err == nil
	}); n != 1 {
		t.Fatalf("clean send RunCompleted{Err:nil} count = %d, want 1", n)
	}
	if n := eventCount(consumer.Events(), func(e converge.Event) bool {
		md, ok := e.(converge.MessageDiscarded)
		return ok && md.MessageID == rejectID && md.Reason == "rejected address"
	}); n != 1 {
		t.Fatalf("MessageDiscarded count = %d, want 1", n)
	}
	if got := mailer.count(rejectedInviteEmail); got != 0 {
		t.Fatalf("mailer called %d times for rejected address, want 0", got)
	}

	if err := tk.Enqueue(context.Background(), producer, invitePayload{Email: throttledInviteEmail, Team: "eng"}, EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(throttleAttempts) == 1
	})
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(throttleAttempts) == 1
	})
	convergetest.AdvanceUntil(t, consumer.Clock, 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(throttleAttempts) == 2
	})
	mu.Lock()
	gotThrottle := append([]int(nil), throttleAttempts...)
	mu.Unlock()
	if want := []int{1, 1}; !reflect.DeepEqual(gotThrottle, want) {
		t.Fatalf("throttled attempts = %v, want %v", gotThrottle, want)
	}
	convergetest.Await(t, func() bool { return mailer.count(throttledInviteEmail) == 1 })

	if err := tk.Enqueue(context.Background(), producer, invitePayload{Email: hardFailInviteEmail, Team: "eng"}, EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, consumer.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&hardFailRuns) >= 2 })
	convergetest.AdvanceUntil(t, consumer.Clock, 100*time.Millisecond, func() bool { return atomic.LoadInt32(&hardFailRuns) >= 3 })
	convergetest.Await(t, func() bool {
		return eventCount(consumer.Events(), func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterMaxAttempts
		}) == 1
	})
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&hardFailRuns) == 3 })
	if got := atomic.LoadInt32(&escalations); got != 1 {
		t.Fatalf("escalations = %d, want 1", got)
	}

	keys := dlqKeys(t, consumer.KV, "send-invite")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, consumer.KV, keys[0])
	if rec.Reason != converge.DeadLetterMaxAttempts.String() {
		t.Fatalf("dlq reason = %q, want %q", rec.Reason, converge.DeadLetterMaxAttempts.String())
	}
	wantPayload := invitePayload{Email: hardFailInviteEmail, Team: "eng"}
	var gotPayload invitePayload
	if err := json.Unmarshal(rec.Payload, &gotPayload); err != nil {
		t.Fatal(err)
	}
	if gotPayload != wantPayload {
		t.Fatalf("dlq payload = %+v, want %+v", gotPayload, wantPayload)
	}
}

func TestMaxAgeCapsPerpetualSnoozer(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	tk := NewTask[string]("job", TaskOpts{})
	var runs int32
	err := Handle(rt, tk, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&runs, 1)
		return Snooze{In: time.Minute}
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

	convergetest.AdvanceUntil(t, w.Clock, time.Minute, func() bool {
		return eventCount(w.Events(), func(e converge.Event) bool {
			dl, ok := e.(converge.MessageDeadLettered)
			return ok && dl.Reason == converge.DeadLetterMaxAge
		}) == 1
	})

	if got := atomic.LoadInt32(&runs); got > 6 {
		t.Fatalf("handler ran %d times, want <= 6", got)
	}
	stopped := atomic.LoadInt32(&runs)
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&runs) == stopped })

	keys := dlqKeys(t, w.KV, "job")
	if len(keys) != 1 {
		t.Fatalf("dlq keys = %v, want exactly 1", keys)
	}
	rec := dlqRecordAt(t, w.KV, keys[0])
	if rec.Reason != converge.DeadLetterMaxAge.String() {
		t.Fatalf("reason = %q, want %q", rec.Reason, converge.DeadLetterMaxAge.String())
	}
	if rec.Attempt != 1 {
		t.Fatalf("dlq record attempt = %d, want 1", rec.Attempt)
	}
}

func TestProducerFromResolvesHandlerBinding(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "wt"})
	rt := w.Build(t)
	jobMQ := inmem.NewMQWithClock(w.Clock)

	bound := NewTask[string]("bound", TaskOpts{})
	var boundRuns int32
	err := Handle(rt, bound, func(ctx context.Context, payload string) error {
		atomic.AddInt32(&boundRuns, 1)
		return nil
	}, HandleOpts{MQ: jobMQ})
	if err != nil {
		t.Fatal(err)
	}

	unbound := NewTask[string]("unbound", TaskOpts{})

	w.Runtime(t)
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}

	if err := bound.Enqueue(context.Background(), p, "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return atomic.LoadInt32(&boundRuns) == 1 })
	convergetest.AssertStable(t, func() bool { return atomic.LoadInt32(&boundRuns) == 1 })

	if err := unbound.Enqueue(context.Background(), p, "world", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := make(chan converge.Delivery, 1)
	go w.MQ.Consume(ctx, "unbound", func(d converge.Delivery) {
		select {
		case delivered <- d:
		default:
		}
	})
	var d converge.Delivery
	select {
	case d = <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery on default mq")
	}
	m := d.Message()
	if m.Kind != "unbound" {
		t.Fatalf("kind = %q, want %q", m.Kind, "unbound")
	}
	var payload string
	if err := json.Unmarshal(m.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload != "world" {
		t.Fatalf("payload = %q, want %q", payload, "world")
	}
	if err := d.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
}
