package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
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

func TestOnMessageBindDefaultsAndCapabilities(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	mq := inmem.NewMQWithClock(clock)
	e := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	e.deps = converge.JobDeps{MQ: mq, Clock: clock, Observer: &convergetest.Recorder{}}
	trig := OnMessage("app-events", RawID(), OnMessageOpts{}).(*messageTrigger)
	if err := trig.bind(e); err != nil {
		t.Fatal(err)
	}
	if trig.delivery != converge.Group || trig.mq != converge.MQ(mq) {
		t.Fatalf("bind = %v %T", trig.delivery, trig.mq)
	}
	all := &engine{cfg: config{name: "job", runMode: converge.OnAllReplicas}}
	all.deps = e.deps
	bTrig := OnMessage("app-events", RawID(), OnMessageOpts{}).(*messageTrigger)
	if err := bTrig.bind(all); err != nil {
		t.Fatal(err)
	}
	if bTrig.delivery != converge.Broadcast {
		t.Fatalf("OnAllReplicas default delivery = %v", bTrig.delivery)
	}
	noMQ := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	noMQ.deps = converge.JobDeps{Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (OnMessage("q", RawID(), OnMessageOpts{}).(*messageTrigger)).bind(noMQ); err == nil {
		t.Fatal("bind without an MQ must error")
	}
	bare := bareMQ{}
	e2 := &engine{cfg: config{name: "job", runMode: converge.OnOneReplica}}
	e2.deps = converge.JobDeps{MQ: bare, Clock: clock, Observer: &convergetest.Recorder{}}
	if err := (OnMessage("q", RawID(), OnMessageOpts{}).(*messageTrigger)).bind(e2); err == nil {
		t.Fatal("Group delivery without GroupConsumer must error")
	}
	if err := (OnMessage("q", RawID(), OnMessageOpts{Delivery: converge.Broadcast}).(*messageTrigger)).bind(e2); err == nil {
		t.Fatal("Broadcast delivery without BroadcastConsumer must error")
	}
}

type bareMQ struct{}

func (bareMQ) Publish(context.Context, string, converge.Message) error { return nil }

func (bareMQ) Consume(context.Context, string, func(converge.Delivery)) error {
	return nil
}

func TestOnMessageWakesFromHints(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := OnMessage("app-events", IDFromJSONField("workspace_id"), OnMessageOpts{}).(*messageTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runMessages(ctx, trig)
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

func TestOnMessageUndecodableHintCountedAndDropped(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnOneReplica}, func(ctx context.Context, id ID) error { return nil })
	mq := inmem.NewMQWithClock(te.clock)
	te.e.deps.MQ = mq
	trig := OnMessage("app-events", IDFromJSONField("workspace_id"), OnMessageOpts{}).(*messageTrigger)
	if err := trig.bind(te.e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runMessages(ctx, trig)
	if err := mq.Publish(context.Background(), "app-events", converge.Message{Payload: []byte(`garbage`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardUndecodable
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("undecodable hint must not run anything, got %d runs", n)
	}
}
