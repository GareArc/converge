package reconcile

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/notice"
)

type captureMQ struct {
	mu   sync.Mutex
	sent []struct {
		queue string
		msg   converge.Message
	}
	err error
}

func (m *captureMQ) Publish(_ context.Context, queue string, msg converge.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.sent = append(m.sent, struct {
		queue string
		msg   converge.Message
	}{queue, msg})
	return nil
}

func (m *captureMQ) Consume(ctx context.Context, _ string, _ func(converge.Delivery)) error {
	<-ctx.Done()
	return ctx.Err()
}

func mustNotifier(t *testing.T, j Job, s converge.Scope) *Notifier {
	t.Helper()
	n, err := j.NewProducer(s)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	return n
}

func TestNotifyPublishesToTheDerivedChannel(t *testing.T) {
	mq := &captureMQ{}
	n := mustNotifier(t, NewJob("merchants", JobOpts{}), converge.Scope{MQ: mq, Namespace: "svc"})
	if n.Notifications() != "svc/converge/notifications/merchants" {
		t.Fatalf("Notifications() = %q", n.Notifications())
	}
	if err := n.Notify(context.Background(), "m-42"); err != nil {
		t.Fatal(err)
	}
	if len(mq.sent) != 1 || mq.sent[0].queue != "svc/converge/notifications/merchants" {
		t.Fatalf("published = %+v", mq.sent)
	}
	if mq.sent[0].msg.Kind != notice.Kind || string(mq.sent[0].msg.Payload) != `{"id":"m-42"}` {
		t.Fatalf("message = %+v, want kind %q and the documented payload", mq.sent[0].msg, notice.Kind)
	}
}

func TestNotifyPublishesToTheDeclaredChannelVerbatim(t *testing.T) {
	mq := &captureMQ{}
	n := mustNotifier(t, NewJob("merchants", JobOpts{Notifications: "dify:merchants"}), converge.Scope{MQ: mq, Namespace: "svc"})
	if err := n.Notify(context.Background(), "m-42"); err != nil {
		t.Fatal(err)
	}
	if mq.sent[0].queue != "dify:merchants" {
		t.Fatalf("queue = %q, want the declared name untouched", mq.sent[0].queue)
	}
}

func TestNotifyAllIsTheDocumentedWireForm(t *testing.T) {
	mq := &captureMQ{}
	n := mustNotifier(t, NewJob("merchants", JobOpts{}), converge.Scope{MQ: mq})
	if err := n.NotifyAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(mq.sent) != 1 || mq.sent[0].queue != "converge/notifications/merchants" || string(mq.sent[0].msg.Payload) != `{"all":true}` {
		t.Fatalf("published = %+v", mq.sent)
	}
}

func TestNotifyRefusesAnEmptyID(t *testing.T) {
	mq := &captureMQ{}
	n := mustNotifier(t, NewJob("merchants", JobOpts{}), converge.Scope{MQ: mq})
	err := n.Notify(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "NotifyAll") {
		t.Fatalf("err = %v, want a refusal pointing at NotifyAll", err)
	}
	if len(mq.sent) != 0 {
		t.Fatalf("published %d messages, want 0", len(mq.sent))
	}
}

func TestNewProducerNeedsScopeMQAndAValidJob(t *testing.T) {
	if _, err := NewJob("merchants", JobOpts{}).NewProducer(converge.Scope{}); err == nil || !strings.Contains(err.Error(), "Scope.MQ") {
		t.Fatalf("err = %v, want mention of Scope.MQ", err)
	}
	bad := NewJob("", JobOpts{})
	if _, err := bad.NewProducer(converge.Scope{MQ: &captureMQ{}}); err == nil || !errors.Is(err, bad.err) {
		t.Fatalf("err = %v, want wrapping %v", err, bad.err)
	}
	if _, err := (Job{}).NewProducer(converge.Scope{MQ: &captureMQ{}}); err == nil || !strings.Contains(err.Error(), "NewJob") {
		t.Fatalf("err = %v, want a zero Job refused", err)
	}
}

func TestNotifySurfacesPublishFailure(t *testing.T) {
	boom := errors.New("boom")
	n := mustNotifier(t, NewJob("merchants", JobOpts{}), converge.Scope{MQ: &captureMQ{err: boom}})
	if err := n.Notify(context.Background(), "m-42"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestUnbuiltNotifierErrorsInsteadOfPanicking(t *testing.T) {
	var nilNotifier *Notifier
	for name, n := range map[string]*Notifier{"nil pointer": nilNotifier, "zero value": {}} {
		t.Run(name, func(t *testing.T) {
			if err := n.Notify(context.Background(), "m-42"); err == nil {
				t.Fatal("Notify on an unbuilt notifier must error")
			}
			if err := n.NotifyAll(context.Background()); err == nil {
				t.Fatal("NotifyAll on an unbuilt notifier must error")
			}
			if n.Notifications() != "" {
				t.Fatalf("Notifications() = %q, want empty", n.Notifications())
			}
		})
	}
}
