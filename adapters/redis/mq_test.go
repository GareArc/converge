package convredis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
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
	mq, clock, ctx := startStreamsMQ(t)
	msg := converge.Message{
		Kind:    "k",
		Headers: map[string]string{converge.HeaderMessageID: "m-1"},
		Payload: []byte("p"),
	}
	if err := mq.Publish(ctx, "q", msg); err != nil {
		t.Fatal(err)
	}
	d := firstDelivery(t, ctx, mq)
	if got := d.Message().Headers[converge.HeaderMessageID]; got != "m-1" {
		t.Fatalf("header = %q, want m-1", got)
	}
	if !d.EnqueuedAt().Equal(clock.Now()) {
		t.Fatalf("EnqueuedAt = %v, want the publishing clock time %v", d.EnqueuedAt(), clock.Now())
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamsMQExtendAfterAck(t *testing.T) {
	mq, _, ctx := startStreamsMQ(t)
	if err := mq.Publish(ctx, "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	d := firstDelivery(t, ctx, mq)
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Extend(ctx, time.Minute); !errors.Is(err, convredis.ErrSettled) {
		t.Fatalf("Extend after Ack = %v, want ErrSettled", err)
	}
}

func startStreamsMQ(t *testing.T) (converge.MQ, *convergetest.Clock, context.Context) {
	t.Helper()
	client, clock, _ := openMini(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return convredis.NewStreamsMQ(client, convredis.StreamsOpts{Clock: clock, Visibility: time.Minute}), clock, ctx
}

func firstDelivery(t *testing.T, ctx context.Context, mq converge.MQ) converge.Delivery {
	t.Helper()
	got := make(chan converge.Delivery, 1)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	select {
	case d := <-got:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return nil
	}
}
