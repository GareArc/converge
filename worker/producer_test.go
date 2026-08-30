package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

func assertNoDelivery(t *testing.T, ch <-chan converge.Delivery, wait time.Duration) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("unexpected delivery: %+v", d.Message())
	case <-time.After(wait):
	}
}

func startConsumer(t *testing.T, mq converge.MQ, queue string) <-chan converge.Delivery {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan converge.Delivery, 16)
	go func() {
		mq.Consume(ctx, queue, func(d converge.Delivery) {
			ch <- d
			d.Ack(ctx)
		})
	}()
	return ch
}

func requireHexID(t *testing.T, id string) {
	t.Helper()
	if len(id) != 32 {
		t.Fatalf("message-id length = %d, want 32", len(id))
	}
	if _, err := hex.DecodeString(id); err != nil {
		t.Fatalf("message-id %q is not hex: %v", id, err)
	}
}

func mustProducer[T any](t *testing.T, tk Task[T], s converge.Scope) *Producer[T] {
	t.Helper()
	p, err := tk.NewProducer(s)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return p
}

type publishConsumeOnlyMQ struct {
	inner converge.MQ
}

func (m publishConsumeOnlyMQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	return m.inner.Publish(ctx, queue, msg)
}

func (m publishConsumeOnlyMQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return m.inner.Consume(ctx, queue, deliver)
}

func TestEnqueuePublishesMessage(t *testing.T) {
	mq := inmem.NewMQ()
	type payload struct {
		Name string
	}
	tk := NewTask[payload]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq})
	ch := startConsumer(t, mq, tk.QueueName(""))
	ctx := context.Background()

	if err := p.Enqueue(ctx, payload{Name: "Alice"}, EnqueueOpts{Headers: map[string]string{"x-custom": "1"}}); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	if err := p.Enqueue(ctx, payload{Name: "Bob"}, EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}

	convergetest.Await(t, func() bool { return len(ch) >= 2 })
	first := (<-ch).Message()
	second := (<-ch).Message()

	if first.Kind != "send-invite" {
		t.Fatalf("Kind = %q, want %q", first.Kind, "send-invite")
	}
	var decoded payload
	if err := json.Unmarshal(first.Payload, &decoded); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if decoded.Name != "Alice" {
		t.Fatalf("payload = %+v, want Alice", decoded)
	}
	requireHexID(t, first.Headers[converge.HeaderMessageID])
	requireHexID(t, second.Headers[converge.HeaderMessageID])
	if first.Headers[converge.HeaderMessageID] == second.Headers[converge.HeaderMessageID] {
		t.Fatal("message-id not unique across enqueues")
	}
	if first.Headers[converge.HeaderSchemaVersion] != "1" {
		t.Fatalf("schema-version = %q, want 1", first.Headers[converge.HeaderSchemaVersion])
	}
	if _, err := time.Parse(time.RFC3339Nano, first.Headers[converge.HeaderEnqueuedAt]); err != nil {
		t.Fatalf("enqueued-at not RFC3339Nano: %v", err)
	}
	if first.Headers[converge.HeaderAttempt] != "0" {
		t.Fatalf("attempt = %q, want 0", first.Headers[converge.HeaderAttempt])
	}
	if first.Headers["x-custom"] != "1" {
		t.Fatalf("user header x-custom = %q, want 1", first.Headers["x-custom"])
	}
}

func TestEnqueueAddressesTheDerivedQueue(t *testing.T) {
	mq := inmem.NewMQ()
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq, Namespace: "acme"})
	if p.Queue() != "acme/converge/queue/send-invite" {
		t.Fatalf("Queue() = %q", p.Queue())
	}
	ch := startConsumer(t, mq, "acme/converge/queue/send-invite")
	if err := p.Enqueue(context.Background(), "hi", EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
	bare := startConsumer(t, mq, "send-invite")
	assertNoDelivery(t, bare, 50*time.Millisecond)
}

func TestEnqueueAddressesTheDeclaredQueueVerbatim(t *testing.T) {
	mq := inmem.NewMQ()
	tk := NewTask[string]("credential-rotate", TaskOpts{Queue: "dify:credential:rotate"})
	p := mustProducer(t, tk, converge.Scope{MQ: mq, Namespace: "acme"})
	if p.Queue() != "dify:credential:rotate" {
		t.Fatalf("Queue() = %q, want the declared name untouched", p.Queue())
	}
	ch := startConsumer(t, mq, "dify:credential:rotate")
	if err := p.Enqueue(context.Background(), "hi", EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
	derived := startConsumer(t, mq, "acme/converge/queue/credential-rotate")
	assertNoDelivery(t, derived, 50*time.Millisecond)
}

func TestEnqueueWithNoNamespaceDerivesAnUnnamespacedQueue(t *testing.T) {
	mq := inmem.NewMQ()
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq})
	if p.Queue() != "converge/queue/send-invite" {
		t.Fatalf("Queue() = %q", p.Queue())
	}
}

func TestEnqueueSchemaVersionFromTaskOpts(t *testing.T) {
	mq := inmem.NewMQ()
	tk := NewTask[int]("bump", TaskOpts{Version: 5})
	p := mustProducer(t, tk, converge.Scope{MQ: mq})
	ch := startConsumer(t, mq, tk.QueueName(""))
	if err := p.Enqueue(context.Background(), 1, EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
	m := (<-ch).Message()
	if m.Headers[converge.HeaderSchemaVersion] != "5" {
		t.Fatalf("schema-version = %q, want 5", m.Headers[converge.HeaderSchemaVersion])
	}
}

func TestEnqueueReservedHeaderPrefixRejected(t *testing.T) {
	mq := inmem.NewMQ()
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq})
	ch := startConsumer(t, mq, tk.QueueName(""))
	err := p.Enqueue(context.Background(), "x", EnqueueOpts{Headers: map[string]string{converge.HeaderPrefix + "x": "1"}})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err = %v, want mention of reserved", err)
	}
	assertNoDelivery(t, ch, 50*time.Millisecond)
}

func TestEnqueueDelay(t *testing.T) {
	clock := convergetest.NewClock(time.Now())
	mq := inmem.NewMQWithClock(clock)
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq, Clock: clock})
	ch := startConsumer(t, mq, tk.QueueName(""))
	if err := p.Enqueue(context.Background(), "hi", EnqueueOpts{Delay: time.Minute}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertNoDelivery(t, ch, 50*time.Millisecond)
	clock.Advance(time.Minute)
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
}

func TestEnqueueDelayWithoutDelayedPublisher(t *testing.T) {
	mq := publishConsumeOnlyMQ{inner: inmem.NewMQ()}
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq})
	err := p.Enqueue(context.Background(), "hi", EnqueueOpts{Delay: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "DelayedPublisher") {
		t.Fatalf("err = %v, want mention of DelayedPublisher", err)
	}
}

func TestEnqueueNegativeDelay(t *testing.T) {
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: inmem.NewMQ()})
	err := p.Enqueue(context.Background(), "hi", EnqueueOpts{Delay: -time.Second})
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("err = %v, want mention of negative", err)
	}
}

func TestNewProducerNeedsScopeMQ(t *testing.T) {
	tk := NewTask[string]("send-invite", TaskOpts{})
	p, err := tk.NewProducer(converge.Scope{Namespace: "acme"})
	if err == nil || p != nil || !strings.Contains(err.Error(), "Scope.MQ") {
		t.Fatalf("NewProducer = %v, %v; want nil and an error naming Scope.MQ", p, err)
	}
}

func TestNewProducerSurfacesTaskMisconstruction(t *testing.T) {
	tk := NewTask[string]("", TaskOpts{})
	_, err := tk.NewProducer(converge.Scope{MQ: inmem.NewMQ()})
	if err == nil || !errors.Is(err, tk.err) {
		t.Fatalf("err = %v, want wrapping %v", err, tk.err)
	}
}

func TestZeroTaskIsRefusedByNewProducer(t *testing.T) {
	var zero Task[string]
	p, err := zero.NewProducer(converge.Scope{MQ: inmem.NewMQ()})
	const want = "worker: NewProducer: worker: Task is required; build one with NewTask"
	if err == nil || err.Error() != want || p != nil {
		t.Fatalf("NewProducer = %v, %q; want nil and %q", p, err, want)
	}
}

func TestUnbuiltProducerErrorsInsteadOfPanicking(t *testing.T) {
	var nilProducer *Producer[string]
	for name, p := range map[string]*Producer[string]{"nil pointer": nilProducer, "zero value": {}} {
		t.Run(name, func(t *testing.T) {
			if err := p.Enqueue(context.Background(), "hi", EnqueueOpts{}); err == nil || !strings.Contains(err.Error(), "NewProducer") {
				t.Fatalf("err = %v, want it to point at Task.NewProducer", err)
			}
			if p.Queue() != "" {
				t.Fatalf("Queue() on an unbuilt producer = %q, want empty", p.Queue())
			}
		})
	}
}

func TestEnqueueStampsTheScopeClock(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))
	mq := inmem.NewMQWithClock(clock)
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq, Clock: clock})
	ch := startConsumer(t, mq, tk.QueueName(""))
	if err := p.Enqueue(context.Background(), "hi", EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
	m := (<-ch).Message()
	want := clock.Now().UTC().Format(time.RFC3339Nano)
	if m.Headers[converge.HeaderEnqueuedAt] != want {
		t.Fatalf("enqueued-at = %q, want %q", m.Headers[converge.HeaderEnqueuedAt], want)
	}
}

func TestEnqueueDefaultsToTheSystemClock(t *testing.T) {
	mq := inmem.NewMQ()
	tk := NewTask[string]("send-invite", TaskOpts{})
	p := mustProducer(t, tk, converge.Scope{MQ: mq})
	ch := startConsumer(t, mq, tk.QueueName(""))
	before := time.Now().Add(-time.Second)
	if err := p.Enqueue(context.Background(), "hi", EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	convergetest.Await(t, func() bool { return len(ch) >= 1 })
	stamped, err := time.Parse(time.RFC3339Nano, (<-ch).Message().Headers[converge.HeaderEnqueuedAt])
	if err != nil || stamped.Before(before) {
		t.Fatalf("enqueued-at = %v, %v; want a wall-clock stamp", stamped, err)
	}
}

func TestEnqueueFromAProducerWithNoRuntime(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	task := NewTask[string]("greet", TaskOpts{})
	got := make(chan string, 1)
	if err := Handle(rt, task, func(_ context.Context, s string) error {
		got <- s
		return nil
	}, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p := mustProducer(t, task, converge.Scope{MQ: h.MQ, Namespace: "test"})
	if err := p.Enqueue(context.Background(), "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "hello" {
			t.Fatalf("payload = %q, want %q", s, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
}

func TestHandleConsumesTheDeclaredQueue(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{Namespace: "acme"})
	rt := h.Build(t)
	task := NewTask[string]("rotate", TaskOpts{Queue: "dify:credential:rotate"})
	got := make(chan string, 1)
	if err := Handle(rt, task, func(_ context.Context, s string) error {
		got <- s
		return nil
	}, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	p := mustProducer(t, task, rt.Scope())
	if err := p.Enqueue(context.Background(), "hello", EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "hello" {
			t.Fatalf("payload = %q", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran: the engine must consume the declared queue")
	}
	if n := len(h.MQ.Published("dify:credential:rotate")); n != 1 {
		t.Fatalf("published to the declared queue = %d, want 1", n)
	}
}

func TestTwoTasksOnOneQueueAreRefusedAtRegistration(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	a := NewTask[string]("a", TaskOpts{Queue: "shared"})
	b := NewTask[string]("b", TaskOpts{Queue: "shared"})
	fn := func(context.Context, string) error { return nil }
	if err := Handle(rt, a, fn, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	err := Handle(rt, b, fn, HandleOpts{})
	if err == nil || !strings.Contains(err.Error(), `queue "shared"`) {
		t.Fatalf("err = %v, want a refusal naming queue \"shared\"", err)
	}
}
