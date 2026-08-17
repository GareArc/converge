package portcheck

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

type MQOptions struct {
	Advance    func(d time.Duration) // nil skips subtests that need time control
	Visibility time.Duration         // the impl's initial visibility window; 0 skips reclaim
}

func MQ(t *testing.T, open func(t *testing.T) converge.MQ, o MQOptions) {
	t.Run("publish then consume then ack", func(t *testing.T) {
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Kind: "k", Payload: []byte("a")})
		d := recvDelivery(t, got)
		if string(d.Message().Payload) != "a" || d.Message().Kind != "k" {
			t.Fatalf("wrong message: %+v", d.Message())
		}
		if d.Attempt() != 1 {
			t.Fatalf("Attempt = %d, want 1", d.Attempt())
		}
		if err := d.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		assertNoDelivery(t, got)
	})

	t.Run("backlog before first consumer is kept", func(t *testing.T) {
		mq := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("early")})
		got := make(chan converge.Delivery, 16)
		go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
		d := recvDelivery(t, got)
		if string(d.Message().Payload) != "early" {
			t.Fatalf("got %q", d.Message().Payload)
		}
		d.Ack(ctx)
	})

	t.Run("nack redelivers with attempt 2", func(t *testing.T) {
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		d := recvDelivery(t, got)
		if err := d.Nack(ctx, 0); err != nil {
			t.Fatal(err)
		}
		d2 := recvDelivery(t, got)
		if d2.Attempt() != 2 {
			t.Fatalf("Attempt = %d, want 2", d2.Attempt())
		}
		d2.Ack(ctx)
	})

	t.Run("nack delay holds redelivery", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		d := recvDelivery(t, got)
		d.Nack(ctx, time.Hour)
		assertNoDelivery(t, got)
		o.Advance(time.Hour + time.Second)
		recvDelivery(t, got).Ack(ctx)
	})

	t.Run("competing consumers each message once", func(t *testing.T) {
		mq := open(t)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		got := make(chan converge.Delivery, 64)
		for range 2 {
			go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
		}
		const n = 20
		for i := range n {
			mustPublish(t, mq, "q", converge.Message{Payload: []byte{byte(i)}})
		}
		seen := map[byte]int{}
		for range n {
			d := recvDelivery(t, got)
			seen[d.Message().Payload[0]]++
			d.Ack(ctx)
		}
		assertNoDelivery(t, got)
		for b, c := range seen {
			if c != 1 {
				t.Fatalf("message %d delivered %d times", b, c)
			}
		}
	})

	t.Run("unacked delivery is reclaimed after visibility", func(t *testing.T) {
		if o.Advance == nil || o.Visibility == 0 {
			t.Skip("needs clock control and a known visibility window")
		}
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		recvDelivery(t, got) // taken, never acked
		o.Advance(o.Visibility + time.Second)
		d2 := recvDelivery(t, got)
		if d2.Attempt() != 2 {
			t.Fatalf("reclaimed Attempt = %d, want 2", d2.Attempt())
		}
		d2.Ack(ctx)
	})

	t.Run("extend defers reclaim", func(t *testing.T) {
		if o.Advance == nil || o.Visibility == 0 {
			t.Skip("needs clock control and a known visibility window")
		}
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		d := recvDelivery(t, got)
		if err := d.Extend(ctx, 3*o.Visibility); err != nil {
			t.Fatal(err)
		}
		o.Advance(o.Visibility + time.Second)
		assertNoDelivery(t, got)
		d.Ack(ctx)
	})
}

func startConsumer(t *testing.T, open func(t *testing.T) converge.MQ) (converge.MQ, chan converge.Delivery, context.Context) {
	t.Helper()
	mq := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan converge.Delivery, 64)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	return mq, got, ctx
}

func mustPublish(t *testing.T, mq converge.MQ, queue string, m converge.Message) {
	t.Helper()
	if err := mq.Publish(context.Background(), queue, m); err != nil {
		t.Fatal(err)
	}
}

func recvDelivery(t *testing.T, ch chan converge.Delivery) converge.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return nil
	}
}

func assertNoDelivery(t *testing.T, ch chan converge.Delivery) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("unexpected delivery: %+v", d.Message())
	case <-time.After(50 * time.Millisecond):
	}
}
