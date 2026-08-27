package inmem_test

import (
	"context"
	"sync"
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
	const retention = time.Hour
	var mu sync.Mutex
	stores := map[string]*inmem.MQ{}
	open := func(t *testing.T) converge.MQ {
		mu.Lock()
		defer mu.Unlock()
		if mq, ok := stores[t.Name()]; ok {
			return mq
		}
		mq := inmem.NewMQWithOpts(inmem.Options{Clock: clock, Retention: retention})
		stores[t.Name()] = mq
		return mq
	}
	portcheck.MQ(t, open, portcheck.MQOptions{Advance: clock.Advance, Visibility: inmem.DefaultVisibility, Retention: retention, TracksGroupBacklog: true})
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

func newIdleMQ(t *testing.T) (*inmem.MQ, *convergetest.Clock) {
	t.Helper()
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	return inmem.NewMQWithClock(clock), clock
}

func attachIdleGroup(t *testing.T, mq *inmem.MQ, queue, group string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := mq.ConsumeGroup(ctx, queue, group, func(converge.Delivery) {
		t.Fatal("must not deliver through an already-cancelled context")
	})
	if err != context.Canceled {
		t.Fatalf("ConsumeGroup with cancelled ctx = %v, want context.Canceled", err)
	}
}

func attachFrozenDelivery(t *testing.T, mq *inmem.MQ, queue string) converge.Delivery {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan converge.Delivery, 1)
	done := make(chan struct{})
	go func() {
		mq.Consume(ctx, queue, func(d converge.Delivery) {
			got <- d
			cancel()
		})
		close(done)
	}()
	if err := mq.Publish(context.Background(), queue, converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	d := recv(t, got)
	<-done
	return d
}

func TestMQIdle(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "fresh MQ is idle",
			run: func(t *testing.T) {
				mq, _ := newIdleMQ(t)
				if !mq.Idle() {
					t.Fatal("fresh MQ must be idle")
				}
			},
		},
		{
			name: "backlog with no groups or subs is idle",
			run: func(t *testing.T) {
				mq, _ := newIdleMQ(t)
				if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
					t.Fatal(err)
				}
				if !mq.Idle() {
					t.Fatal("backlog with no groups or subs must be idle")
				}
			},
		},
		{
			name: "group with deliverable pending is not idle",
			run: func(t *testing.T) {
				mq, _ := newIdleMQ(t)
				attachIdleGroup(t, mq, "q", "g")
				if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
					t.Fatal(err)
				}
				if mq.Idle() {
					t.Fatal("deliverable pending message in a group must not be idle")
				}
			},
		},
		{
			name: "unexpired in-flight is not idle",
			run: func(t *testing.T) {
				mq, _ := newIdleMQ(t)
				attachFrozenDelivery(t, mq, "q")
				if mq.Idle() {
					t.Fatal("unexpired in-flight delivery must not be idle")
				}
			},
		},
		{
			name: "expired in-flight is not idle without mutating",
			run: func(t *testing.T) {
				mq, clock := newIdleMQ(t)
				attachFrozenDelivery(t, mq, "q")
				clock.Advance(inmem.DefaultVisibility + time.Second)
				if mq.Idle() {
					t.Fatal("expired in-flight delivery must not be idle")
				}
				if mq.Idle() {
					t.Fatal("Idle must not mutate state between calls")
				}
			},
		},
		{
			name: "after ack the group is idle again",
			run: func(t *testing.T) {
				mq, _ := newIdleMQ(t)
				d := attachFrozenDelivery(t, mq, "q")
				if err := d.Ack(context.Background()); err != nil {
					t.Fatal(err)
				}
				if !mq.Idle() {
					t.Fatal("after ack with nothing else pending, must be idle")
				}
			},
		},
		{
			name: "delayed message is idle until its notBefore, then not idle",
			run: func(t *testing.T) {
				mq, clock := newIdleMQ(t)
				attachIdleGroup(t, mq, "q", "g")
				err := mq.PublishDelayed(context.Background(), "q", converge.Message{Payload: []byte("later")}, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				if !mq.Idle() {
					t.Fatal("delayed message with future availableAt must be idle")
				}
				clock.Advance(time.Hour + time.Second)
				if mq.Idle() {
					t.Fatal("delayed message past its notBefore must not be idle")
				}
			},
		},
		{
			name: "broadcast sub with pending is not idle",
			run: func(t *testing.T) {
				mq, _ := newIdleMQ(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				first := make(chan struct{})
				block := make(chan struct{})
				var once sync.Once
				go mq.ConsumeBroadcast(ctx, "q", func(converge.Delivery) {
					once.Do(func() { close(first) })
					<-block
				})
				probeCtx, stopProbes := context.WithCancel(context.Background())
				probesDone := make(chan struct{})
				go func() {
					defer close(probesDone)
					for probeCtx.Err() == nil {
						mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("probe")})
						time.Sleep(2 * time.Millisecond)
					}
				}()
				select {
				case <-first:
				case <-time.After(2 * time.Second):
					stopProbes()
					t.Fatal("broadcast subscriber never attached")
				}
				stopProbes()
				<-probesDone
				err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("pending")})
				if err != nil {
					t.Fatal(err)
				}
				if mq.Idle() {
					t.Fatal("broadcast sub with pending message must not be idle")
				}
				close(block)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
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
