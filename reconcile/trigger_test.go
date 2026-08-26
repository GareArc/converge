package reconcile

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/notice"
)

type funcTrigger struct {
	run func(ctx context.Context, wake func(ID)) error
}

func (t funcTrigger) Run(ctx context.Context, wake func(ID)) error { return t.run(ctx, wake) }

func TestCustomTriggerWakesIDs(t *testing.T) {
	var mu sync.Mutex
	var got []ID
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, id)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fired := make(chan struct{})
	trig := funcTrigger{run: func(ctx context.Context, wake func(ID)) error {
		wake("ws_1")
		close(fired)
		<-ctx.Done()
		return ctx.Err()
	}}
	go te.e.runTrigger(ctx, 0, trig)
	<-fired
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1 && got[0] == "ws_1"
	})
}

func TestCustomTriggerIsRestartedAfterFailure(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var mu sync.Mutex
	starts := 0
	trig := funcTrigger{run: func(ctx context.Context, wake func(ID)) error {
		mu.Lock()
		starts++
		mu.Unlock()
		return errors.New("source died")
	}}
	go te.e.runTrigger(ctx, 0, trig)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return starts >= 1
	})
	advanceUntil(t, te, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return starts >= 2
	})
}

func TestCustomTriggerRestartBackoffResetsAfterHealthyRun(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var mu sync.Mutex
	starts := 0
	trig := funcTrigger{run: func(ctx context.Context, wake func(ID)) error {
		mu.Lock()
		starts++
		n := starts
		mu.Unlock()
		if n == 2 {
			te.clock.Advance(triggerRestartMax)
		}
		return errors.New("source died")
	}}
	go te.e.runTrigger(ctx, 0, trig)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return starts >= 1
	})
	advanceUntil(t, te, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return starts >= 2
	})
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return starts < 3
	})
	te.clock.Advance(time.Second)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return starts >= 3
	})
}

func TestCustomTriggerStopsOnCancel(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	trig := funcTrigger{run: func(ctx context.Context, wake func(ID)) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	go func() {
		te.e.runTrigger(ctx, 0, trig)
		close(returned)
	}()
	cancel()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("supervision did not stop on cancel")
	}
}

func TestNotificationsBindResolvesOwnInboxAndCapabilities(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	mq := inmem.NewMQWithClock(clock)
	e := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	e.deps = converge.JobDeps{MQ: mq, Namespace: "acme", Clock: clock, Observer: &convergetest.Recorder{}}
	trig := Notifications(NotificationsOpts{}).(*notificationTrigger)
	if err := trig.bind(e); err != nil {
		t.Fatal(err)
	}
	if trig.broadcast || trig.mq != converge.MQ(mq) {
		t.Fatalf("bind = %v %T", trig.broadcast, trig.mq)
	}
	if want := keys.Inbox("acme", "job"); trig.queue != want {
		t.Fatalf("queue = %q, want %q", trig.queue, want)
	}
	all := &engine{cfg: config{name: "job", runMode: converge.OnAllReplicas}}
	all.deps = e.deps
	bTrig := Notifications(NotificationsOpts{}).(*notificationTrigger)
	if err := bTrig.bind(all); err != nil {
		t.Fatal(err)
	}
	if !bTrig.broadcast {
		t.Fatalf("OnAllReplicas default broadcast = %v", bTrig.broadcast)
	}
	noMQ := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	noMQ.deps = converge.JobDeps{Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications(NotificationsOpts{}).(*notificationTrigger)).bind(noMQ); err == nil {
		t.Fatal("bind without an MQ must error")
	}
	bare := bareMQ{}
	e2 := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	e2.deps = converge.JobDeps{MQ: bare, Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications(NotificationsOpts{}).(*notificationTrigger)).bind(e2); err == nil {
		t.Fatal("OnOneReplica without GroupConsumer must error")
	}
	e3 := &engine{cfg: config{name: "job", runMode: converge.OnAllReplicas}}
	e3.deps = converge.JobDeps{MQ: bare, Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications(NotificationsOpts{}).(*notificationTrigger)).bind(e3); err == nil {
		t.Fatal("OnAllReplicas without BroadcastConsumer must error")
	}
}

func TestNotificationsFromBindKeepsGivenQueue(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	mq := inmem.NewMQWithClock(clock)
	e := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	e.deps = converge.JobDeps{MQ: mq, Namespace: "acme", Clock: clock, Observer: &convergetest.Recorder{}}
	trig := NotificationsFrom("legacy:queue", NotificationsOpts{ID: RawID()}).(*notificationTrigger)
	if err := trig.bind(e); err != nil {
		t.Fatal(err)
	}
	if trig.queue != "legacy:queue" {
		t.Fatalf("queue = %q, want the foreign name unchanged", trig.queue)
	}
}

func TestMissingMQErrorNamesTheTriggerConstructor(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	bareEngine := func() *engine {
		e := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
		e.deps = converge.JobDeps{Clock: clock, Observer: &convergetest.Recorder{}}
		return e
	}
	err := (Notifications(NotificationsOpts{}).(*notificationTrigger)).bind(bareEngine())
	if err == nil || !strings.Contains(err.Error(), "Notifications needs Options.MQ") {
		t.Fatalf("Notifications bind error = %v, want it to name Notifications", err)
	}
	if strings.Contains(err.Error(), "NotificationsFrom") {
		t.Fatalf("Notifications bind error = %v, must not name NotificationsFrom", err)
	}
	err = (NotificationsFrom("legacy:queue", NotificationsOpts{ID: RawID()}).(*notificationTrigger)).bind(bareEngine())
	if err == nil || !strings.Contains(err.Error(), `NotificationsFrom("legacy:queue")`) {
		t.Fatalf(`NotificationsFrom bind error = %v, want it to name NotificationsFrom("legacy:queue")`, err)
	}
}

func TestNotificationsWakesFromOwnInbox(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := Notifications(NotificationsOpts{}).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	payload, err := notice.Encode("ws_9")
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(context.Background(), trig.queue, converge.Message{Kind: notice.Kind, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "ws_9"
		}) == 1
	})
}

func TestNotificationsForwardCompatibleFieldsStillDecode(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := Notifications(NotificationsOpts{}).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	future := []byte(`{"id":"ws_9","schema":"v3-not-yet-invented"}`)
	if err := mq.Publish(context.Background(), trig.queue, converge.Message{Kind: notice.Kind, Payload: future}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "ws_9"
		}) == 1
	})
}

func TestNotificationsEmptyIDAddressesSingleIDJob(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica, single: true}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := Notifications(NotificationsOpts{}).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	payload, err := notice.Encode("")
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(context.Background(), trig.queue, converge.Message{Kind: notice.Kind, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == ""
		}) == 1
	})
}

func TestNotificationsUndecodablePayloadCountedAndDropped(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := Notifications(NotificationsOpts{}).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), trig.queue, converge.Message{Payload: []byte(`garbage`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			nd, ok := e.(converge.NotificationDropped)
			return ok && errors.Is(nd.Err, converge.ErrNotificationUndecodable)
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("undecodable notification must not run anything, got %d runs", n)
	}
}

func TestNotificationsFromWakesUsingIDFunc(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := NotificationsFrom("app-events", NotificationsOpts{ID: IDFromJSON("workspace_id")}).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), "app-events", converge.Message{Payload: []byte(`{"workspace_id": "ws_9"}`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "ws_9"
		}) == 1
	})
}

func TestNotificationsFromUndecodablePayloadCountedAndDropped(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := NotificationsFrom("app-events", NotificationsOpts{ID: RawID()}).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), "app-events", converge.Message{Payload: nil}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			nd, ok := e.(converge.NotificationDropped)
			return ok && errors.Is(nd.Err, converge.ErrNotificationUndecodable)
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("undecodable notification must not run anything, got %d runs", n)
	}
}

type bareMQ struct{}

func (bareMQ) Publish(context.Context, string, converge.Message) error { return nil }

func (bareMQ) Consume(context.Context, string, func(converge.Delivery)) error {
	return nil
}
