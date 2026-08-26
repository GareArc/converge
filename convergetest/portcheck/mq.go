package portcheck

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

type MQOptions struct {
	Advance    func(d time.Duration)
	Visibility time.Duration
	Retention  time.Duration
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
		recvDelivery(t, got)
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

	t.Run("extend after own ack errors", func(t *testing.T) {
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		d := recvDelivery(t, got)
		if err := d.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if err := d.Extend(ctx, time.Minute); err == nil {
			t.Fatal("Extend after Ack must error")
		}
	})

	t.Run("stale extend does not postpone the successor's redelivery", func(t *testing.T) {
		if o.Advance == nil || o.Visibility == 0 {
			t.Skip("needs clock control and a known visibility window")
		}
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		stale := recvDelivery(t, got)
		o.Advance(o.Visibility + time.Second)
		live := recvDelivery(t, got)
		if live.Attempt() != 2 {
			t.Fatalf("reclaimed Attempt = %d, want 2", live.Attempt())
		}
		if err := stale.Extend(ctx, 10*o.Visibility); err == nil {
			t.Fatal("Extend on a stale handle must report the loss")
		}
		o.Advance(o.Visibility + time.Second)
		next := recvDelivery(t, got)
		if next.Attempt() != 3 {
			t.Fatalf("Attempt after a stale extend = %d, want 3", next.Attempt())
		}
		if err := next.Ack(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("stale ack does not settle the successor", func(t *testing.T) {
		if o.Advance == nil || o.Visibility == 0 {
			t.Skip("needs clock control and a known visibility window")
		}
		mq, got, ctx := startConsumer(t, open)
		mustPublish(t, mq, "q", converge.Message{Payload: []byte("a")})
		stale := recvDelivery(t, got)
		o.Advance(o.Visibility + time.Second)
		live := recvDelivery(t, got)
		if live.Attempt() != 2 {
			t.Fatalf("reclaimed Attempt = %d, want 2", live.Attempt())
		}
		if err := stale.Ack(ctx); err != nil {
			t.Fatalf("Ack on a stale handle = %v, want nil", err)
		}
		o.Advance(o.Visibility + time.Second)
		next := recvDelivery(t, got)
		if next.Attempt() != 3 {
			t.Fatalf("Attempt after a stale ack = %d, want 3", next.Attempt())
		}
		if err := next.Ack(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("named groups each receive every message", func(t *testing.T) {
		base := open(t)
		gc, ok := base.(converge.GroupConsumer)
		if !ok {
			t.Skip("no GroupConsumer capability")
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		gotA := make(chan converge.Delivery, 16)
		gotB := make(chan converge.Delivery, 16)
		go gc.ConsumeGroup(ctx, "q", "a", func(d converge.Delivery) { gotA <- d })
		go gc.ConsumeGroup(ctx, "q", "b", func(d converge.Delivery) { gotB <- d })
		mustPublish(t, base, "q", converge.Message{Payload: []byte("x")})
		recvDelivery(t, gotA).Ack(ctx)
		recvDelivery(t, gotB).Ack(ctx)
	})

	t.Run("named group created later receives the backlog", func(t *testing.T) {
		base := open(t)
		gc, ok := base.(converge.GroupConsumer)
		if !ok {
			t.Skip("no GroupConsumer capability")
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		mustPublish(t, base, "q", converge.Message{Payload: []byte("early")})
		got := make(chan converge.Delivery, 16)
		go gc.ConsumeGroup(ctx, "q", "late", func(d converge.Delivery) { got <- d })
		d := recvDelivery(t, got)
		if string(d.Message().Payload) != "early" {
			t.Fatalf("got %q; groups must receive the pre-creation backlog", d.Message().Payload)
		}
		d.Ack(ctx)
	})

	t.Run("broadcast reaches every subscriber, later subscribers miss earlier messages", func(t *testing.T) {
		base := open(t)
		bc, ok := base.(converge.BroadcastConsumer)
		if !ok {
			t.Skip("no BroadcastConsumer capability")
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		mustPublish(t, base, "q", converge.Message{Payload: []byte("before")})
		gotA := make(chan converge.Delivery, 64)
		gotB := make(chan converge.Delivery, 64)
		go bc.ConsumeBroadcast(ctx, "q", func(d converge.Delivery) { gotA <- d })
		go bc.ConsumeBroadcast(ctx, "q", func(d converge.Delivery) { gotB <- d })
		awaitBroadcastAttach(t, base, "q", gotA, gotB)
		mustPublish(t, base, "q", converge.Message{Payload: []byte("after")})
		for _, ch := range []chan converge.Delivery{gotA, gotB} {
			d := recvBroadcast(t, ch)
			if string(d.Message().Payload) != "after" {
				t.Fatalf("got %q; pre-subscribe messages must not be delivered", d.Message().Payload)
			}
			if d.Attempt() != 1 {
				t.Fatalf("broadcast Attempt = %d, want always 1", d.Attempt())
			}
		}
		assertNoDelivery(t, gotA)
	})

	t.Run("delayed publish holds until due", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		base := open(t)
		dp, ok := base.(converge.DelayedPublisher)
		if !ok {
			t.Skip("no DelayedPublisher capability")
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		got := make(chan converge.Delivery, 16)
		go base.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
		if err := dp.PublishDelayed(ctx, "q", converge.Message{Payload: []byte("later")}, time.Hour); err != nil {
			t.Fatal(err)
		}
		assertNoDelivery(t, got)
		o.Advance(time.Hour + time.Second)
		recvDelivery(t, got).Ack(ctx)
	})

	t.Run("cancel stops consuming even with deliverable messages pending", func(t *testing.T) {
		t.Run("Consume", func(t *testing.T) {
			mq := open(t)
			assertConsumeStopsOnCancel(t, "q", func(ctx context.Context, deliver func(converge.Delivery)) error {
				return mq.Consume(ctx, "q", deliver)
			}, func(m converge.Message) { mustPublish(t, mq, "q", m) })
		})
		t.Run("ConsumeGroup", func(t *testing.T) {
			base := open(t)
			gc, ok := base.(converge.GroupConsumer)
			if !ok {
				t.Skip("no GroupConsumer capability")
			}
			assertConsumeStopsOnCancel(t, "q", func(ctx context.Context, deliver func(converge.Delivery)) error {
				return gc.ConsumeGroup(ctx, "q", "g", deliver)
			}, func(m converge.Message) { mustPublish(t, base, "q", m) })
		})
		t.Run("ConsumeBroadcast", func(t *testing.T) {
			base := open(t)
			bc, ok := base.(converge.BroadcastConsumer)
			if !ok {
				t.Skip("no BroadcastConsumer capability")
			}
			assertBroadcastStopsOnCancel(t, base, bc)
		})
	})

	t.Run("RetentionDropsOldEntries", func(t *testing.T) {
		if o.Advance == nil || o.Retention == 0 {
			t.Skip("adapter does not expose retention")
		}
		mq := open(t)
		mustPublish(t, mq, queue, converge.Message{Kind: probeKind, Payload: []byte("stale")})
		o.Advance(o.Retention + time.Minute)
		mq2, ch, ctx := startConsumer(t, open)
		_ = mq2
		_ = ctx
		assertNoDelivery(t, ch)
	})

	t.Run("BacklogCountsUnconsumed", func(t *testing.T) {
		mq := open(t)
		br, ok := mq.(converge.BacklogReporter)
		if !ok {
			t.Skip("adapter does not report backlog")
		}
		for i := 0; i < 3; i++ {
			mustPublish(t, mq, queue, converge.Message{Kind: probeKind, Payload: []byte{byte(i)}})
		}
		n, err := br.Backlog(context.Background(), queue)
		if err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("backlog = %d, want 3", n)
		}
	})
}

func assertConsumeStopsOnCancel(t *testing.T, queue string, run func(ctx context.Context, deliver func(converge.Delivery)) error, publish func(converge.Message)) {
	t.Helper()
	for i := range 5 {
		publish(converge.Message{Payload: []byte{byte(i)}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var delivered atomic.Int64
	var cancelOnce sync.Once
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, func(d converge.Delivery) {
			delivered.Add(1)
			cancelOnce.Do(cancel)
		})
	}()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Consume returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after cancel while messages were still deliverable")
	}
	if n := delivered.Load(); n != 1 {
		t.Fatalf("deliver invoked %d times; want exactly 1 (none after Consume returned)", n)
	}
	assertNoFurtherDelivery(t, &delivered, 1)
}

func assertBroadcastStopsOnCancel(t *testing.T, mq converge.MQ, bc converge.BroadcastConsumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	attached := make(chan struct{})
	var attachOnce sync.Once
	var delivered atomic.Int64
	var cancelOnce sync.Once
	stopped := make(chan error, 1)
	go func() {
		stopped <- bc.ConsumeBroadcast(ctx, "q", func(d converge.Delivery) {
			if d.Message().Kind == probeKind {
				attachOnce.Do(func() { close(attached) })
				return
			}
			delivered.Add(1)
			cancelOnce.Do(cancel)
		})
	}()
	awaitAttach := func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			select {
			case <-attached:
				return
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("broadcast subscriber never attached")
			}
			mustPublish(t, mq, "q", converge.Message{Kind: probeKind})
			time.Sleep(2 * time.Millisecond)
		}
	}
	awaitAttach()
	for i := range 3 {
		mustPublish(t, mq, "q", converge.Message{Payload: []byte{byte(i)}})
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ConsumeBroadcast returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeBroadcast did not return after cancel while messages were still deliverable")
	}
	if n := delivered.Load(); n != 1 {
		t.Fatalf("deliver invoked %d times; want exactly 1 (none after ConsumeBroadcast returned)", n)
	}
	assertNoFurtherDelivery(t, &delivered, 1)
}

func assertNoFurtherDelivery(t *testing.T, delivered *atomic.Int64, before int64) {
	t.Helper()
	deadline := time.Now().Add(50 * time.Millisecond)
	for {
		if n := delivered.Load(); n != before {
			t.Fatalf("deliver invoked after return: count went from %d to %d", before, n)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
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

const probeKind = "converge.portcheck.probe"

const queue = "q"

func awaitBroadcastAttach(t *testing.T, mq converge.MQ, queue string, subs ...chan converge.Delivery) {
	t.Helper()
	attached := make([]bool, len(subs))
	deadline := time.Now().Add(2 * time.Second)
	for {
		remaining := 0
		for i, ch := range subs {
			if attached[i] {
				continue
			}
			select {
			case d := <-ch:
				if d.Message().Kind != probeKind {
					t.Fatalf("pre-subscribe message delivered during attach: %q", d.Message().Payload)
				}
				attached[i] = true
			default:
				remaining++
			}
		}
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("broadcast subscribers never attached")
		}
		mustPublish(t, mq, queue, converge.Message{Kind: probeKind})
		time.Sleep(2 * time.Millisecond)
	}
}

func recvBroadcast(t *testing.T, ch chan converge.Delivery) converge.Delivery {
	t.Helper()
	for {
		d := recvDelivery(t, ch)
		if d.Message().Kind != probeKind {
			return d
		}
	}
}
