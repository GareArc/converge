package reconcile

import (
	"context"
	"errors"
	"fmt"
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
	run func(ctx context.Context, sink Sink) error
}

func (t funcTrigger) Run(ctx context.Context, sink Sink) error { return t.run(ctx, sink) }

func TestCustomTriggerQueuesIDs(t *testing.T) {
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
	trig := funcTrigger{run: func(ctx context.Context, sink Sink) error {
		sink.Notify("ws_1")
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

func TestCustomTriggerNotifyPullsIDOutOfFailureBackoff(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n == 1 {
			return errors.New("boom")
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	te.e.notify(ctx, "ws_1")
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 1
	})

	fired := make(chan struct{})
	trig := funcTrigger{run: func(ctx context.Context, sink Sink) error {
		sink.Notify("ws_1")
		close(fired)
		<-ctx.Done()
		return ctx.Err()
	}}
	go te.e.runTrigger(ctx, 0, trig)
	<-fired

	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 2
	})
}

func TestCustomTriggerDropReportsNotificationDropped(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	fired := make(chan struct{})
	reason := errors.New("boom: not JSON")
	trig := funcTrigger{run: func(ctx context.Context, sink Sink) error {
		sink.Drop(reason)
		close(fired)
		<-ctx.Done()
		return ctx.Err()
	}}
	go te.e.runTrigger(ctx, 0, trig)
	<-fired

	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.NotificationDropped)
			return ok
		}) == 1
	})
	var ev converge.NotificationDropped
	for _, e := range te.rec.Events() {
		if nd, ok := e.(converge.NotificationDropped); ok {
			ev = nd
		}
	}
	if !errors.Is(ev.Err, converge.ErrNotificationUndecodable) {
		t.Fatalf("Drop error = %v, want it to wrap ErrNotificationUndecodable", ev.Err)
	}
	if !strings.Contains(ev.Err.Error(), "boom: not JSON") {
		t.Fatalf("Drop error = %v, want the reason to appear in the message", ev.Err)
	}
}

func TestCustomTriggerIsRestartedAfterFailure(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var mu sync.Mutex
	starts := 0
	trig := funcTrigger{run: func(ctx context.Context, sink Sink) error {
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
	trig := funcTrigger{run: func(ctx context.Context, sink Sink) error {
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
	trig := funcTrigger{run: func(ctx context.Context, sink Sink) error {
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

func TestCustomTriggerSinkCallsAfterStopDoNotPanic(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())

	sink := engineSink{e: te.e, ctx: ctx}
	cancel()
	te.cancel()
	te.hstop()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Notify/Drop after stop panicked: %v", r)
		}
	}()
	sink.Notify("ws_1")
	sink.Drop(errors.New("boom"))
}

func TestNotificationsBindResolvesOwnChannelAndCapabilities(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	mq := inmem.NewMQWithClock(clock)
	e := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnOneReplica}}
	e.deps = converge.JobDeps{MQ: mq, Namespace: "acme", Clock: clock, Observer: &convergetest.Recorder{}}
	trig := Notifications().(*notificationTrigger)
	if err := trig.bind(e); err != nil {
		t.Fatal(err)
	}
	if trig.broadcast || trig.mq != converge.MQ(mq) {
		t.Fatalf("bind = %v %T", trig.broadcast, trig.mq)
	}
	if want := keys.Notifications("acme", "job"); trig.source != want {
		t.Fatalf("source = %q, want %q", trig.source, want)
	}
	all := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnAllReplicas}}
	all.deps = e.deps
	bTrig := Notifications().(*notificationTrigger)
	if err := bTrig.bind(all); err != nil {
		t.Fatal(err)
	}
	if !bTrig.broadcast {
		t.Fatalf("OnAllReplicas default broadcast = %v", bTrig.broadcast)
	}
	noMQ := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnOneReplica}}
	noMQ.deps = converge.JobDeps{Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications().(*notificationTrigger)).bind(noMQ); err == nil {
		t.Fatal("bind without an MQ must error")
	}
	bare := bareMQ{}
	e2 := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnOneReplica}}
	e2.deps = converge.JobDeps{MQ: bare, Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications().(*notificationTrigger)).bind(e2); err != nil {
		t.Fatalf("OnOneReplica binds through base Consume; no capability is required: %v", err)
	}
	e3 := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnAllReplicas}}
	e3.deps = converge.JobDeps{MQ: bare, Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications().(*notificationTrigger)).bind(e3); err == nil {
		t.Fatal("OnAllReplicas without BroadcastConsumer must error")
	}
	e4 := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.Competing}}
	e4.deps = converge.JobDeps{MQ: bare, Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (Notifications().(*notificationTrigger)).bind(e4); err == nil {
		t.Fatal("Competing without GroupConsumer must error")
	}
}

func TestNotificationsFromBindKeepsTheGivenSource(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	mq := inmem.NewMQWithClock(clock)
	e := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnOneReplica}}
	e.deps = converge.JobDeps{MQ: mq, Namespace: "acme", Clock: clock, Observer: &convergetest.Recorder{}}
	trig := NotificationsFrom("legacy:queue", nil, RawID()).(*notificationTrigger)
	if err := trig.bind(e); err != nil {
		t.Fatal(err)
	}
	if trig.source != "legacy:queue" {
		t.Fatalf("source = %q, want the foreign name unchanged", trig.source)
	}
}

func TestNotificationsBindUsesADeclaredName(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	mq := inmem.NewMQWithClock(clock)
	e := &engine{cfg: config{job: NewJob("job", JobOpts{Notifications: "dify:workspace-credentials"}), runMode: converge.OnOneReplica}}
	e.deps = converge.JobDeps{MQ: mq, Namespace: "acme", Clock: clock, Observer: &convergetest.Recorder{}}
	trig := Notifications().(*notificationTrigger)
	if err := trig.bind(e); err != nil {
		t.Fatal(err)
	}
	if trig.source != "dify:workspace-credentials" {
		t.Fatalf("source = %q, want the declared name verbatim", trig.source)
	}
}

func TestMissingMQErrorNamesTheTriggerConstructor(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	bareEngine := func() *engine {
		e := &engine{cfg: config{job: NewJob("job", JobOpts{}), runMode: converge.OnOneReplica}}
		e.deps = converge.JobDeps{Clock: clock, Observer: &convergetest.Recorder{}}
		return e
	}
	err := (Notifications().(*notificationTrigger)).bind(bareEngine())
	if err == nil || !strings.Contains(err.Error(), "Notifications needs Options.MQ") {
		t.Fatalf("Notifications bind error = %v, want it to name Notifications", err)
	}
	if strings.Contains(err.Error(), "NotificationsFrom") {
		t.Fatalf("Notifications bind error = %v, must not name NotificationsFrom", err)
	}
	err = (NotificationsFrom("legacy:queue", nil, RawID()).(*notificationTrigger)).bind(bareEngine())
	if err == nil || !strings.Contains(err.Error(), `NotificationsFrom("legacy:queue")`) {
		t.Fatalf(`NotificationsFrom bind error = %v, want it to name NotificationsFrom("legacy:queue")`, err)
	}
}

func TestNotificationsReadsTheJobsOwnChannel(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := Notifications().(*notificationTrigger)
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
	if err := mq.Publish(context.Background(), trig.source, converge.Message{Kind: notice.Kind, Payload: payload}); err != nil {
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
	trig := Notifications().(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	future := []byte(`{"id":"ws_9","schema":"v3-not-yet-invented"}`)
	if err := mq.Publish(context.Background(), trig.source, converge.Message{Kind: notice.Kind, Payload: future}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "ws_9"
		}) == 1
	})
}

func TestAllNotificationRunsTheSingleID(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica, single: true}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := Notifications().(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	payload, err := notice.EncodeAll()
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Publish(context.Background(), trig.source, converge.Message{Kind: notice.Kind, Payload: payload}); err != nil {
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
	trig := Notifications().(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), trig.source, converge.Message{Payload: []byte(`garbage`)}); err != nil {
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

func TestNotificationsFromQueuesUsingIDFunc(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := NotificationsFrom("app-events", nil, IDFromJSON("workspace_id")).(*notificationTrigger)
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
	trig := NotificationsFrom("app-events", nil, RawID()).(*notificationTrigger)
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

func TestSkipFromAnIDFunctionAcksAndReportsSkipped(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	mine := func(payload []byte) (ID, error) {
		if string(payload) == "theirs" {
			return "", Skip
		}
		return ID(payload), nil
	}
	trig := NotificationsFrom("shared", nil, mine).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	for _, p := range []string{"theirs", "ws-1"} {
		if err := mq.Publish(context.Background(), "shared", converge.Message{Payload: []byte(p)}); err != nil {
			t.Fatal(err)
		}
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "ws-1"
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		s, ok := e.(converge.NotificationSkipped)
		return ok && s.Job == "job"
	}); n != 1 {
		t.Fatalf("NotificationSkipped count = %d, want 1", n)
	}
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.NotificationDropped)
		return ok
	}); n != 0 {
		t.Fatalf("a Skip is not a drop; NotificationDropped count = %d", n)
	}
	convergetest.AssertStable(t, func() bool { return mq.Idle() })
}

func TestSkipWrappedInFmtErrorfIsStillRecognized(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	wrapped := func(payload []byte) (ID, error) {
		return "", fmt.Errorf("mine: %w", Skip)
	}
	trig := NotificationsFrom("shared", nil, wrapped).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), "shared", converge.Message{Payload: []byte("theirs")}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			s, ok := e.(converge.NotificationSkipped)
			return ok && s.Job == "job"
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.NotificationDropped)
		return ok
	}); n != 0 {
		t.Fatalf("a wrapped Skip must still be recognised via errors.Is, not counted as a drop; got %d drops", n)
	}
}

func TestSkipWithANonEmptyIDIsStillSkipped(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	confused := func(payload []byte) (ID, error) {
		return ID(payload), Skip
	}
	trig := NotificationsFrom("shared", nil, confused).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), "shared", converge.Message{Payload: []byte("ws-1")}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			s, ok := e.(converge.NotificationSkipped)
			return ok && s.Job == "job"
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.ID == "ws-1"
	}); n != 0 {
		t.Fatalf("Skip alongside a non-empty ID must not queue that ID, got %d runs", n)
	}
	convergetest.AssertStable(t, func() bool { return mq.Idle() })
}

func TestUnknownErrorFromAnIDFunctionIsDroppedNotSkipped(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	broken := func(payload []byte) (ID, error) {
		return "", errors.New("some other system's error")
	}
	trig := NotificationsFrom("shared", nil, broken).(*notificationTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runNotifications(ctx, trig)
	if err := mq.Publish(context.Background(), "shared", converge.Message{Payload: []byte("anything")}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			nd, ok := e.(converge.NotificationDropped)
			return ok && errors.Is(nd.Err, converge.ErrNotificationUndecodable)
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.NotificationSkipped)
		return ok
	}); n != 0 {
		t.Fatalf("an unrelated error must not be reported as a Skip, got %d NotificationSkipped", n)
	}
}

type bareMQ struct{}

func (bareMQ) Publish(context.Context, string, converge.Message) error { return nil }

func (bareMQ) Consume(context.Context, string, func(converge.Delivery)) error {
	return nil
}
