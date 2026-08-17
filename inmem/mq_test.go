package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
	"github.com/GareArc/converge/inmem"
)

var (
	_ converge.MQ                = (*inmem.MQ)(nil)
	_ converge.GroupConsumer     = (*inmem.MQ)(nil)
	_ converge.BroadcastConsumer = (*inmem.MQ)(nil)
	_ converge.DelayedPublisher  = (*inmem.MQ)(nil)
)

func TestMQContract(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	portcheck.MQ(t,
		func(t *testing.T) converge.MQ { return inmem.NewMQWithClock(clock) },
		portcheck.MQOptions{Advance: clock.Advance, Visibility: inmem.DefaultVisibility},
	)
}

func TestStaleDeliveryCannotDisturbSuccessor(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	mq := inmem.NewMQWithClock(clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan converge.Delivery, 16)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	if err := mq.Publish(ctx, "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	d1 := recv(t, got)
	clock.Advance(inmem.DefaultVisibility + time.Second)
	d2 := recv(t, got)
	if d2.Attempt() != 2 {
		t.Fatalf("Attempt = %d, want 2", d2.Attempt())
	}
	if d1.Attempt() != 1 {
		t.Fatalf("stale delivery's Attempt must stay 1, got %d", d1.Attempt())
	}
	if err := d1.Extend(ctx, time.Minute); err == nil {
		t.Fatal("stale Extend must error")
	}
	if err := d1.Nack(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := d1.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d2.Nack(ctx, 0); err != nil {
		t.Fatal(err)
	}
	d3 := recv(t, got)
	if d3.Attempt() != 3 {
		t.Fatalf("successor's Nack must still redeliver; Attempt = %d, want 3", d3.Attempt())
	}
	d3.Ack(ctx)
}

func recv(t *testing.T, ch chan converge.Delivery) converge.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return nil
	}
}

func TestPublishedMessageIsIsolatedFromCaller(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	mq := inmem.NewMQWithClock(clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan converge.Delivery, 16)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	payload := []byte("original")
	headers := map[string]string{"k": "v"}
	if err := mq.Publish(ctx, "q", converge.Message{Headers: headers, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	headers["k"] = "mutated"
	d := recv(t, got)
	m := d.Message()
	if string(m.Payload) != "original" || m.Headers["k"] != "v" {
		t.Fatalf("delivery sees caller mutations: %q %q", m.Payload, m.Headers["k"])
	}
	m.Payload[0] = 'Y'
	if string(d.Message().Payload) != "original" {
		t.Fatal("consumer mutation leaked back into the store")
	}
	d.Ack(ctx)
}
