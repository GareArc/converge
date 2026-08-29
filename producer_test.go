package converge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/GareArc/converge/internal/notice"
)

func mustProducer(t *testing.T, mq MQ, o ProducerOpts) *Producer {
	t.Helper()
	p, err := NewProducer(mq, o)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return p
}

func TestNewProducerNeedsAnMQ(t *testing.T) {
	p, err := NewProducer(nil, ProducerOpts{})
	if err == nil || p != nil {
		t.Fatalf("p, err = %v, %v, want a nil producer and an error", p, err)
	}
}

func TestNewProducerDefaultsToTheSystemClock(t *testing.T) {
	p := mustProducer(t, &recordingMQ{}, ProducerOpts{})
	if _, ok := p.clock.(systemClock); !ok {
		t.Fatalf("clock = %T, want systemClock", p.clock)
	}
}

func TestNotifyPublishesToTheNamespacedNotificationsChannel(t *testing.T) {
	mq := &recordingMQ{}
	p := mustProducer(t, mq, ProducerOpts{Namespace: "svc"})
	if err := p.Notify(context.Background(), "merchants", "m-42"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	sent := mq.sent()
	if len(sent) != 1 {
		t.Fatalf("published %d messages, want 1", len(sent))
	}
	if sent[0].queue != "svc/converge/notifications/merchants" {
		t.Fatalf("queue = %q, want %q", sent[0].queue, "svc/converge/notifications/merchants")
	}
	if sent[0].msg.Kind != notice.Kind {
		t.Fatalf("Kind = %q, want %q", sent[0].msg.Kind, notice.Kind)
	}
	n, err := notice.Decode(sent[0].msg.Payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if n.ID != "m-42" {
		t.Fatalf("id = %q, want %q", n.ID, "m-42")
	}
}

func TestNotifyWithNoNamespaceStillNamespacesTheLibrary(t *testing.T) {
	mq := &recordingMQ{}
	p := mustProducer(t, mq, ProducerOpts{})
	if err := p.Notify(context.Background(), "merchants", ""); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	sent := mq.sent()
	if len(sent) != 1 || sent[0].queue != "converge/notifications/merchants" {
		t.Fatalf("published = %+v, want one message on converge/notifications/merchants", sent)
	}
	n, err := notice.Decode(sent[0].msg.Payload)
	if err != nil || !n.All {
		t.Fatalf("n, err = %+v, %v, want All true and no error", n, err)
	}
}

func TestNotifyNeedsAJobName(t *testing.T) {
	mq := &recordingMQ{}
	p := mustProducer(t, mq, ProducerOpts{})
	err := p.Notify(context.Background(), "", "m-42")
	if err == nil || !strings.Contains(err.Error(), "job name") {
		t.Fatalf("err = %v, want mention of a missing job name", err)
	}
	if n := len(mq.sent()); n != 0 {
		t.Fatalf("published %d messages, want 0", n)
	}
}

func TestNotifySurfacesPublishFailure(t *testing.T) {
	boom := errors.New("boom")
	mq := &recordingMQ{err: boom}
	p := mustProducer(t, mq, ProducerOpts{})
	if err := p.Notify(context.Background(), "merchants", "m-42"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestUnbuiltProducerErrorsInsteadOfPanicking(t *testing.T) {
	var nilProducer *Producer
	cases := map[string]*Producer{
		"nil pointer":     nilProducer,
		"zero value":      {},
		"composite empty": new(Producer),
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			if err := p.Notify(context.Background(), "merchants", "m-42"); err == nil {
				t.Fatal("Notify on an unbuilt producer must error")
			}
		})
	}
}
