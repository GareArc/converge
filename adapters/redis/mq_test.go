package convredis_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
	"github.com/redis/go-redis/v9"
)

const (
	testGroup      = "converge"
	testStreamKey  = "convredis:s:q"
	testPendingKey = "convredis:p:q:" + testGroup
)

func TestMQPortMiniredis(t *testing.T) {
	var advance func(d time.Duration)
	portcheck.MQ(t, func(t *testing.T) converge.MQ {
		client, clock, adv := openMini(t)
		advance = adv
		return convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock, Visibility: time.Minute})
	}, portcheck.MQOptions{Advance: func(d time.Duration) { advance(d) }, Visibility: time.Minute})
}

func TestMQPortRealRedis(t *testing.T) {
	portcheck.MQ(t, func(t *testing.T) converge.MQ {
		return convredis.NewStreamsMQ(openReal(t), convredis.StreamsOpts{})
	}, portcheck.MQOptions{})
}

func TestStreamsMQDeliveryCarriesHeadersAndEnqueuedAt(t *testing.T) {
	f := newStreamsMQ(t)
	msg := converge.Message{
		Kind:    "k",
		Headers: map[string]string{converge.HeaderMessageID: "m-1"},
		Payload: []byte("p"),
	}
	f.publish(t, msg)
	d := recvDelivery(t, f.consume(t))
	if got := d.Message().Headers[converge.HeaderMessageID]; got != "m-1" {
		t.Fatalf("header = %q, want m-1", got)
	}
	if !d.EnqueuedAt().Equal(f.clock.Now()) {
		t.Fatalf("EnqueuedAt = %v, want the publishing clock time %v", d.EnqueuedAt(), f.clock.Now())
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamsMQExtendAfterAck(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	d := recvDelivery(t, f.consume(t))
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Extend(f.ctx, time.Minute); !errors.Is(err, convredis.ErrSettled) {
		t.Fatalf("Extend after Ack = %v, want ErrSettled", err)
	}
}

func TestStreamsMQReadBatchSurvivesCancelMidBatch(t *testing.T) {
	f := newStreamsMQ(t)
	const batch = 4
	for i := range batch {
		f.publish(t, converge.Message{Payload: []byte{byte(i)}})
	}

	cctx, stop := context.WithCancel(f.ctx)
	first := make(chan converge.Delivery, 1)
	stopped := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(stopped)
		f.mq.Consume(cctx, "q", func(d converge.Delivery) {
			once.Do(func() {
				first <- d
				stop()
			})
		})
	}()
	delivered := recvDelivery(t, first)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after cancel")
	}
	if err := delivered.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}

	f.advance(time.Minute + time.Second)
	got := f.consume(t)
	seen := map[byte]bool{}
	for range batch - 1 {
		d := recvDelivery(t, got)
		seen[d.Message().Payload[0]] = true
		if err := d.Ack(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != batch-1 {
		t.Fatalf("reclaimed %d distinct messages, want %d", len(seen), batch-1)
	}
	if seen[delivered.Message().Payload[0]] {
		t.Fatal("acked message was redelivered")
	}
}

func TestStreamsMQReclaimsPendingEntryThatWasNeverTracked(t *testing.T) {
	f := newStreamsMQ(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	if err := f.client.XGroupCreateMkStream(f.ctx, testStreamKey, testGroup, "0").Err(); err != nil {
		t.Fatal(err)
	}
	streams, err := f.client.XReadGroup(f.ctx, &redis.XReadGroupArgs{
		Group:    testGroup,
		Consumer: "rogue",
		Streams:  []string{testStreamKey, ">"},
		Count:    1,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	id := streams[0].Messages[0].ID
	if n, err := f.client.ZCard(f.ctx, testPendingKey).Result(); err != nil || n != 0 {
		t.Fatalf("pending set holds %d entries (err %v), want an untracked pending entry", n, err)
	}

	got := f.consume(t)
	f.awaitTracked(t, id)
	f.advance(time.Minute + time.Second)
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "a" {
		t.Fatalf("got %q, want a", d.Message().Payload)
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamsMQRecoversFromDeletedStream(t *testing.T) {
	f := newStreamsMQ(t)
	got := f.consume(t)
	f.publish(t, converge.Message{Payload: []byte("a")})
	if err := recvDelivery(t, got).Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Del(f.ctx, testStreamKey).Err(); err != nil {
		t.Fatal(err)
	}
	f.publish(t, converge.Message{Payload: []byte("b")})
	d := recvDelivery(t, got)
	if string(d.Message().Payload) != "b" {
		t.Fatalf("got %q, want b", d.Message().Payload)
	}
	if err := d.Ack(f.ctx); err != nil {
		t.Fatal(err)
	}
}

type streamsMQFixture struct {
	mq      converge.MQ
	client  *redis.Client
	clock   *convergetest.Clock
	advance func(d time.Duration)
	ctx     context.Context
}

func newStreamsMQ(t *testing.T) *streamsMQFixture {
	t.Helper()
	client, clock, advance := openMini(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &streamsMQFixture{
		mq:      convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock, Visibility: time.Minute}),
		client:  client,
		clock:   clock,
		advance: advance,
		ctx:     ctx,
	}
}

func (f *streamsMQFixture) publish(t *testing.T, msg converge.Message) {
	t.Helper()
	if err := f.mq.Publish(f.ctx, "q", msg); err != nil {
		t.Fatal(err)
	}
}

func (f *streamsMQFixture) consume(t *testing.T) chan converge.Delivery {
	t.Helper()
	got := make(chan converge.Delivery, 16)
	go f.mq.Consume(f.ctx, "q", func(d converge.Delivery) { got <- d })
	return got
}

func (f *streamsMQFixture) awaitTracked(t *testing.T, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := f.client.ZScore(f.ctx, testPendingKey, id).Err(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("entry %s was never tracked in %s", id, testPendingKey)
		}
		time.Sleep(2 * time.Millisecond)
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
