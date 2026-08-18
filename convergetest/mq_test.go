package convergetest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

func TestFailNextPublishFailsExactlyOnce(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	boom := errors.New("boom")
	m.FailNextPublish(boom)
	if err := m.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); !errors.Is(err, boom) {
		t.Fatalf("Publish error = %v, want %v", err, boom)
	}
	if err := m.Publish(context.Background(), "q", converge.Message{Payload: []byte("b")}); err != nil {
		t.Fatalf("second Publish should succeed, got %v", err)
	}
	got := m.Published("q")
	if len(got) != 1 || string(got[0].Payload) != "b" {
		t.Fatalf("Published = %+v, want only the successful publish", got)
	}
}

func TestFailNextPublishAppliesToPublishDelayed(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	boom := errors.New("boom")
	m.FailNextPublish(boom)
	err := m.PublishDelayed(context.Background(), "q", converge.Message{Payload: []byte("a")}, time.Minute)
	if !errors.Is(err, boom) {
		t.Fatalf("PublishDelayed error = %v, want %v", err, boom)
	}
	err = m.PublishDelayed(context.Background(), "q", converge.Message{Payload: []byte("b")}, time.Minute)
	if err != nil {
		t.Fatalf("second PublishDelayed should succeed, got %v", err)
	}
	got := m.Published("q")
	if len(got) != 1 || string(got[0].Payload) != "b" {
		t.Fatalf("Published = %+v, want only the successful publish", got)
	}
}

func TestFailNextPublishIsSharedAcrossPublishMethods(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	boom := errors.New("boom")
	m.FailNextPublish(boom)
	err := m.PublishDelayed(context.Background(), "q", converge.Message{Payload: []byte("a")}, time.Minute)
	if !errors.Is(err, boom) {
		t.Fatalf("PublishDelayed error = %v, want %v", err, boom)
	}
	if err := m.Publish(context.Background(), "q", converge.Message{Payload: []byte("b")}); err != nil {
		t.Fatalf("Publish after the armed failure was consumed should succeed, got %v", err)
	}
}

func TestPublishedRecordsOrderAndIsADefensiveCopy(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	headers := map[string]string{"k": "v"}
	payload := []byte("first")
	if err := m.Publish(context.Background(), "q", converge.Message{Headers: headers, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := m.Publish(context.Background(), "q", converge.Message{Payload: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	headers["k"] = "mutated"
	payload[0] = 'X'
	got := m.Published("q")
	if len(got) != 2 || string(got[0].Payload) != "first" || got[0].Headers["k"] != "v" || string(got[1].Payload) != "second" {
		t.Fatalf("Published = %+v", got)
	}
	got[0].Payload[0] = 'Y'
	got[0].Headers["k"] = "clobbered"
	second := m.Published("q")
	if string(second[0].Payload) != "first" || second[0].Headers["k"] != "v" {
		t.Fatalf("mutating a Published result leaked back: %+v", second[0])
	}
}

func TestPublishedIsEmptyForUnknownQueue(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	if got := m.Published("missing"); len(got) != 0 {
		t.Fatalf("Published(missing) = %+v, want empty", got)
	}
}

func TestConsumeDelegatesToBase(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan converge.Delivery, 1)
	go m.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	if err := m.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if string(d.Message().Payload) != "a" {
			t.Fatalf("got %q", d.Message().Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestIdlePassthrough(t *testing.T) {
	m := convergetest.WrapMQ(inmem.NewMQ())
	if !m.Idle() {
		t.Fatal("fresh wrapped MQ must be idle")
	}
	if err := m.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if !m.Idle() {
		t.Fatal("backlog with no consumer must be idle")
	}
}
