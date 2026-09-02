package convredis_test

import (
	"context"
	"sync"
	"testing"
	"time"

	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

type fakeSink struct {
	mu       sync.Mutex
	notified []reconcile.ID
	dropped  []error
}

func (s *fakeSink) Notify(id reconcile.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notified = append(s.notified, id)
}

func (s *fakeSink) Drop(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = append(s.dropped, err)
}

func (s *fakeSink) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.notified), len(s.dropped)
}

func (s *fakeSink) snapshot() ([]reconcile.ID, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	notified := make([]reconcile.ID, len(s.notified))
	copy(notified, s.notified)
	dropped := make([]error, len(s.dropped))
	copy(dropped, s.dropped)
	return notified, dropped
}

func waitForSink(t *testing.T, s *fakeSink, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		notified, dropped := s.counts()
		if notified+dropped >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	notified, dropped := s.counts()
	t.Fatalf("timed out waiting for %d sink events, got notified=%d dropped=%d", want, notified, dropped)
}

func TestListTriggerNotifiesInPushOrderAndDropsMalformed(t *testing.T) {
	rdb, _, _ := openMini(t)
	key := "test:workspace:sync"
	rdb.LPush(context.Background(), key, `{"workspace_id":"ws-a"}`, "not-json", `{"workspace_id":"ws-b"}`)

	trig, err := convredis.ListTrigger(rdb, key, convredis.ListTriggerOpts{
		ID: reconcile.IDFromJSON("workspace_id"),
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- trig.Run(ctx, sink) }()

	waitForSink(t, sink, 3)
	cancel()
	<-done

	notified, dropped := sink.snapshot()
	want := []reconcile.ID{"ws-a", "ws-b"}
	if len(notified) != len(want) || notified[0] != want[0] || notified[1] != want[1] {
		t.Fatalf("notified = %v, want %v", notified, want)
	}
	if len(dropped) != 1 {
		t.Fatalf("dropped = %d, want 1", len(dropped))
	}
}

func TestListTriggerDefaultIDIsRawID(t *testing.T) {
	rdb, _, _ := openMini(t)
	key := "test:raw"
	rdb.LPush(context.Background(), key, "ws_1")

	trig, err := convredis.ListTrigger(rdb, key, convredis.ListTriggerOpts{})
	if err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- trig.Run(ctx, sink) }()

	waitForSink(t, sink, 1)
	cancel()
	<-done

	notified, dropped := sink.snapshot()
	if len(dropped) != 0 {
		t.Fatalf("dropped = %d, want 0", len(dropped))
	}
	if len(notified) != 1 || notified[0] != reconcile.ID("ws_1") {
		t.Fatalf("notified = %v, want [ws_1]", notified)
	}
}

func TestListTriggerReturnsContextCanceledOnCancel(t *testing.T) {
	rdb, _, _ := openMini(t)
	trig, err := convredis.ListTrigger(rdb, "test:empty", convredis.ListTriggerOpts{})
	if err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- trig.Run(ctx, sink) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestListTriggerTwoTriggersOnOneKeySplitElements(t *testing.T) {
	rdb, _, _ := openMini(t)
	key := "test:shared"
	for i := 0; i < 10; i++ {
		if err := rdb.LPush(context.Background(), key, "ws_1").Err(); err != nil {
			t.Fatal(err)
		}
	}

	trig1, err := convredis.ListTrigger(rdb, key, convredis.ListTriggerOpts{})
	if err != nil {
		t.Fatal(err)
	}
	trig2, err := convredis.ListTrigger(rdb, key, convredis.ListTriggerOpts{})
	if err != nil {
		t.Fatal(err)
	}
	sink1 := &fakeSink{}
	sink2 := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() { done1 <- trig1.Run(ctx, sink1) }()
	go func() { done2 <- trig2.Run(ctx, sink2) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n1, d1 := sink1.counts()
		n2, d2 := sink2.counts()
		if n1+d1+n2+d2 >= 10 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done1
	<-done2

	n1, d1 := sink1.counts()
	n2, d2 := sink2.counts()
	if n1+d1+n2+d2 != 10 {
		t.Fatalf("total events across both sinks = %d, want 10", n1+d1+n2+d2)
	}
}

func TestListTriggerConstructorErrors(t *testing.T) {
	rdb, _, _ := openMini(t)
	cases := []struct {
		name string
		rdb  *redis.Client
		key  string
		opts convredis.ListTriggerOpts
		want string
	}{
		{"nil client", nil, "k", convredis.ListTriggerOpts{}, "convredis: ListTrigger needs a client"},
		{"empty key", rdb, "", convredis.ListTriggerOpts{}, "convredis: ListTrigger needs a list key"},
		{"negative poll", rdb, "k", convredis.ListTriggerOpts{Poll: -time.Second}, "convredis: ListTrigger Poll must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convredis.ListTrigger(tc.rdb, tc.key, tc.opts)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("ListTrigger(%q) error = %v, want %q", tc.key, err, tc.want)
			}
		})
	}
}
