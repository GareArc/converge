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

func await(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

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
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	type payload struct {
		Name string
	}
	tk := NewTask[payload]("send-invite", TaskOpts{})
	ch := startConsumer(t, mq, tk.queue)
	ctx := context.Background()

	if err := tk.Enqueue(ctx, p, payload{Name: "Alice"}, EnqueueOpts{Headers: map[string]string{"x-custom": "1"}}); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	if err := tk.Enqueue(ctx, p, payload{Name: "Bob"}, EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}

	await(t, func() bool { return len(ch) >= 2 })
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

func TestEnqueueSchemaVersionFromTaskOpts(t *testing.T) {
	mq := inmem.NewMQ()
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	tk := NewTask[int]("bump", TaskOpts{Version: 5})
	ch := startConsumer(t, mq, tk.queue)

	if err := tk.Enqueue(context.Background(), p, 1, EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	await(t, func() bool { return len(ch) >= 1 })
	m := (<-ch).Message()
	if m.Headers[converge.HeaderSchemaVersion] != "5" {
		t.Fatalf("schema-version = %q, want 5", m.Headers[converge.HeaderSchemaVersion])
	}
}

func TestEnqueueReservedHeaderPrefixRejected(t *testing.T) {
	mq := inmem.NewMQ()
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	tk := NewTask[string]("send-invite", TaskOpts{})
	ch := startConsumer(t, mq, tk.queue)

	err = tk.Enqueue(context.Background(), p, "x", EnqueueOpts{Headers: map[string]string{converge.HeaderPrefix + "x": "1"}})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err = %v, want mention of reserved", err)
	}
	assertNoDelivery(t, ch, 50*time.Millisecond)
}

func TestEnqueueDelay(t *testing.T) {
	clock := convergetest.NewClock(time.Now())
	mq := inmem.NewMQWithClock(clock)
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	tk := NewTask[string]("send-invite", TaskOpts{})
	ch := startConsumer(t, mq, tk.queue)

	if err := tk.Enqueue(context.Background(), p, "hi", EnqueueOpts{Delay: time.Minute}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertNoDelivery(t, ch, 50*time.Millisecond)
	clock.Advance(time.Minute)
	await(t, func() bool { return len(ch) >= 1 })
}

func TestEnqueueDelayWithoutDelayedPublisher(t *testing.T) {
	mq := publishConsumeOnlyMQ{inner: inmem.NewMQ()}
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	tk := NewTask[string]("send-invite", TaskOpts{})

	err = tk.Enqueue(context.Background(), p, "hi", EnqueueOpts{Delay: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "DelayedPublisher") {
		t.Fatalf("err = %v, want mention of DelayedPublisher", err)
	}
}

func TestEnqueueNegativeDelay(t *testing.T) {
	mq := inmem.NewMQ()
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	tk := NewTask[string]("send-invite", TaskOpts{})

	err = tk.Enqueue(context.Background(), p, "hi", EnqueueOpts{Delay: -time.Second})
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("err = %v, want mention of negative", err)
	}
}

func TestNewProducerNilMQ(t *testing.T) {
	p, err := NewProducer(nil)
	if err == nil || p != nil {
		t.Fatalf("p, err = %v, %v, want error and nil producer", p, err)
	}
}

func TestProducerFromNilRuntime(t *testing.T) {
	p, err := ProducerFrom(nil)
	if err == nil || p != nil {
		t.Fatalf("p, err = %v, %v, want error and nil producer", p, err)
	}
}

func TestEnqueueMisconstructedTask(t *testing.T) {
	mq := inmem.NewMQ()
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	tk := NewTask[string]("", TaskOpts{})

	err = tk.Enqueue(context.Background(), p, "hi", EnqueueOpts{})
	if err == nil || !errors.Is(err, tk.err) {
		t.Fatalf("err = %v, want wrapping %v", err, tk.err)
	}
}

func TestNewProducerAlwaysUsesGivenMQ(t *testing.T) {
	mq := inmem.NewMQ()
	p, err := NewProducer(mq)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if p.mq != converge.MQ(mq) {
		t.Fatalf("p.mq = %v, want %v", p.mq, mq)
	}
	if p.queueMQ != nil {
		t.Fatal("p.queueMQ should be nil for a Producer built with NewProducer")
	}
}

func TestProducerFromUsesOptionsMQAndClock(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))
	mq := inmem.NewMQWithClock(clock)
	rt, err := converge.New(converge.Options{MQ: mq, Clock: clock})
	if err != nil {
		t.Fatalf("converge.New: %v", err)
	}
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatalf("ProducerFrom: %v", err)
	}
	tk := NewTask[string]("send-invite", TaskOpts{})
	ch := startConsumer(t, mq, tk.queue)

	if err := tk.Enqueue(context.Background(), p, "hi", EnqueueOpts{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	await(t, func() bool { return len(ch) >= 1 })
	m := (<-ch).Message()
	want := clock.Now().UTC().Format(time.RFC3339Nano)
	if m.Headers[converge.HeaderEnqueuedAt] != want {
		t.Fatalf("enqueued-at = %q, want %q", m.Headers[converge.HeaderEnqueuedAt], want)
	}
}

func TestProducerFromNoMQNoBindingErrors(t *testing.T) {
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatalf("converge.New: %v", err)
	}
	p, err := ProducerFrom(rt)
	if err != nil {
		t.Fatalf("ProducerFrom: %v", err)
	}
	tk := NewTask[string]("send-invite", TaskOpts{Queue: "invites"})

	err = tk.Enqueue(context.Background(), p, "hi", EnqueueOpts{})
	if err == nil || !strings.Contains(err.Error(), "invites") {
		t.Fatalf("err = %v, want mention of queue name", err)
	}
}
