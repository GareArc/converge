package converge

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/notice"
)

type recordingMQ struct {
	mu        sync.Mutex
	published []recordedPublish
	err       error
}

type recordedPublish struct {
	queue string
	msg   Message
	delay time.Duration
}

func (m *recordingMQ) Publish(ctx context.Context, queue string, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, recordedPublish{queue: queue, msg: msg})
	return nil
}

func (m *recordingMQ) Consume(ctx context.Context, queue string, deliver func(Delivery)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (m *recordingMQ) sent() []recordedPublish {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedPublish(nil), m.published...)
}

type delayingMQ struct{ *recordingMQ }

func (m delayingMQ) PublishDelayed(ctx context.Context, queue string, msg Message, delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, recordedPublish{queue: queue, msg: msg, delay: delay})
	return nil
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time                         { return c.t }
func (c fixedClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

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

func TestNotifyPublishesToTheNamespacedInbox(t *testing.T) {
	mq := &recordingMQ{}
	p := mustProducer(t, mq, ProducerOpts{Namespace: "svc"})
	if err := p.Notify(context.Background(), "merchants", "m-42"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	sent := mq.sent()
	if len(sent) != 1 {
		t.Fatalf("published %d messages, want 1", len(sent))
	}
	if sent[0].queue != "svc/converge/inbox/merchants" {
		t.Fatalf("queue = %q, want %q", sent[0].queue, "svc/converge/inbox/merchants")
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
	if len(sent) != 1 || sent[0].queue != "converge/inbox/merchants" {
		t.Fatalf("published = %+v, want one message on converge/inbox/merchants", sent)
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
			if err := p.send(context.Background(), "merchants", Message{}, 0); err == nil {
				t.Fatal("send on an unbuilt producer must error")
			}
		})
	}
}

func TestSendUsesDelayedPublisherWhenDelayed(t *testing.T) {
	mq := delayingMQ{recordingMQ: &recordingMQ{}}
	p := mustProducer(t, mq, ProducerOpts{Namespace: "svc"})
	if err := p.send(context.Background(), "greet", Message{Kind: "greet"}, time.Minute); err != nil {
		t.Fatalf("send: %v", err)
	}
	sent := mq.sent()
	if len(sent) != 1 || sent[0].delay != time.Minute {
		t.Fatalf("published = %+v, want one delayed publish of 1m", sent)
	}
	if sent[0].queue != "svc/converge/inbox/greet" {
		t.Fatalf("queue = %q, want the namespaced inbox", sent[0].queue)
	}
}

func TestSendWithoutDelayedPublisherIsAClearError(t *testing.T) {
	p := mustProducer(t, &recordingMQ{}, ProducerOpts{})
	err := p.send(context.Background(), "greet", Message{}, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "DelayedPublisher") {
		t.Fatalf("err = %v, want mention of DelayedPublisher", err)
	}
}

func TestProducerSendHookRejectsForeignTypes(t *testing.T) {
	ctx := context.Background()
	if err := hook.ProducerSend("not a producer", ctx, "greet", Message{}, 0); err == nil {
		t.Fatal("a foreign producer type must be rejected")
	}
	p := mustProducer(t, &recordingMQ{}, ProducerOpts{})
	if err := hook.ProducerSend(p, ctx, "greet", "not a message", 0); err == nil {
		t.Fatal("a foreign message type must be rejected")
	}
	var nilProducer *Producer
	if err := hook.ProducerSend(nilProducer, ctx, "greet", Message{}, 0); err == nil {
		t.Fatal("a typed-nil producer must be rejected")
	}
}

func TestProducerNowHookRejectsForeignTypes(t *testing.T) {
	if _, ok := hook.ProducerNow("not a producer"); ok {
		t.Fatal("a foreign producer type must not report a time")
	}
	var nilProducer *Producer
	if _, ok := hook.ProducerNow(nilProducer); ok {
		t.Fatal("a typed-nil producer must not report a time")
	}
	if _, ok := hook.ProducerNow(&Producer{}); ok {
		t.Fatal("an unbuilt producer must not report a time")
	}
	at := time.Date(2026, 8, 25, 1, 2, 3, 0, time.UTC)
	p := mustProducer(t, &recordingMQ{}, ProducerOpts{Clock: fixedClock{t: at}})
	got, ok := hook.ProducerNow(p)
	if !ok || !got.Equal(at) {
		t.Fatalf("ProducerNow = %v, %v, want %v, true", got, ok, at)
	}
}
