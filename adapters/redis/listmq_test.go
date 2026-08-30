package convredis_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
)

func TestListMQPublishConsumeAckRoundtrip(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan converge.Delivery, 1)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	d := recvListDelivery(t, got)
	if string(d.Message().Payload) != "a" {
		t.Fatalf("got %q, want a", d.Message().Payload)
	}
	if d.Attempt() != 1 {
		t.Fatalf("Attempt = %d, want 1", d.Attempt())
	}
	if err := d.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := client.LLen(context.Background(), "q").Result(); err != nil || n != 0 {
		t.Fatalf("LLen after ack = %d (err %v), want 0", n, err)
	}
}

func TestListMQNackRepublishesForRedelivery(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan converge.Delivery, 1)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	d := recvListDelivery(t, got)
	if err := d.Nack(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	d2 := recvListDelivery(t, got)
	if string(d2.Message().Payload) != "a" {
		t.Fatalf("redelivered payload = %q, want a", d2.Message().Payload)
	}
	if d2.Attempt() != 1 {
		t.Fatalf("Attempt after nack = %d, want 1: a list has no attempt tracking", d2.Attempt())
	}
}

func TestListMQExtendIsANoOp(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan converge.Delivery, 1)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	d := recvListDelivery(t, got)
	if err := d.Extend(context.Background(), time.Hour); err != nil {
		t.Fatalf("Extend = %v, want nil", err)
	}
}

func TestListMQBacklogReportsLLen(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	br, ok := mq.(converge.BacklogReporter)
	if !ok {
		t.Fatal("NewListMQ must implement converge.BacklogReporter")
	}
	for i := 0; i < 3; i++ {
		if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := br.Backlog(context.Background(), "q")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("Backlog = %d, want 3", n)
	}
	if got, err := client.LLen(context.Background(), "q").Result(); err != nil || got != 3 {
		t.Fatalf("LLen = %d (err %v), want 3", got, err)
	}
}

func TestListMQPreservesPublishOrder(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	for _, p := range []string{"a", "b", "c"} {
		if err := mq.Publish(context.Background(), "q", converge.Message{Payload: []byte(p)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan converge.Delivery, 3)
	go mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d })
	for _, want := range []string{"a", "b", "c"} {
		d := recvListDelivery(t, got)
		if string(d.Message().Payload) != want {
			t.Fatalf("got %q, want %q", d.Message().Payload, want)
		}
	}
}

func TestListMQTwoConsumersOnTheSameQueueSplitTheMessages(t *testing.T) {
	client, _, _ := openMini(t)
	mqA := convredis.NewListMQ(client)
	mqB := convredis.NewListMQ(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	gotA := make(chan converge.Delivery, 32)
	gotB := make(chan converge.Delivery, 32)
	go mqA.Consume(ctx, "q", func(d converge.Delivery) { gotA <- d })
	go mqB.Consume(ctx, "q", func(d converge.Delivery) { gotB <- d })
	const n = 20
	for i := range n {
		if err := mqA.Publish(context.Background(), "q", converge.Message{Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[byte]int{}
	for range n {
		select {
		case d := <-gotA:
			seen[d.Message().Payload[0]]++
		case d := <-gotB:
			seen[d.Message().Payload[0]]++
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for all deliveries")
		}
	}
	if len(seen) != n {
		t.Fatalf("saw %d distinct messages, want %d: two unrelated consumers on one list steal each other's messages instead of each seeing every message", len(seen), n)
	}
	for b, c := range seen {
		if c != 1 {
			t.Fatalf("message %d delivered %d times, want exactly 1", b, c)
		}
	}
}

func TestListMQConsumeStopsOnCancel(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan converge.Delivery, 1)
	stopped := make(chan error, 1)
	go func() { stopped <- mq.Consume(ctx, "q", func(d converge.Delivery) { got <- d }) }()
	cancel()
	select {
	case err := <-stopped:
		if err != context.Canceled {
			t.Fatalf("Consume returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after cancel")
	}
}

func TestListMQImplementsOnlyMQAndBacklogReporter(t *testing.T) {
	client, _, _ := openMini(t)
	mq := convredis.NewListMQ(client)
	if _, ok := mq.(converge.GroupConsumer); ok {
		t.Fatal("a list-backed MQ must not claim GroupConsumer")
	}
	if _, ok := mq.(converge.BroadcastConsumer); ok {
		t.Fatal("a list-backed MQ must not claim BroadcastConsumer")
	}
	if _, ok := mq.(converge.DelayedPublisher); ok {
		t.Fatal("a list-backed MQ must not claim DelayedPublisher")
	}
	if _, ok := mq.(converge.BacklogReporter); !ok {
		t.Fatal("a list-backed MQ must implement BacklogReporter")
	}
}

func TestListMQNotificationsFromDrivesReconcileJobUnderOnOneReplica(t *testing.T) {
	client, _, _ := openMini(t)
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Job:       reconcile.NewJob("member-sync", reconcile.JobOpts{}),
		Reconcile: func(ctx context.Context, id reconcile.ID) error { return nil },
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Hour)),
			reconcile.NotificationsFrom("enterprise:member:sync", convredis.NewListMQ(client), reconcile.IDFromJSON("workspace_id")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.LPush(context.Background(), "enterprise:member:sync", `{"type":"member_added","workspace_id":"ws-e2e"}`).Err(); err != nil {
		t.Fatal(err)
	}
	h.AssertReconciled(t, "member-sync", "ws-e2e")
}

func TestListMQNotificationsFromFailsUnderOnAllReplicasNamingBroadcastConsumer(t *testing.T) {
	client, clock, _ := openMini(t)
	rt, err := converge.New(converge.Options{
		Clock:        clock,
		Observer:     &convergetest.Recorder{},
		DrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Job:       reconcile.NewJob("member-sync", reconcile.JobOpts{}),
		RunMode:   converge.OnAllReplicas,
		Reconcile: func(ctx context.Context, id reconcile.ID) error { return nil },
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Hour)),
			reconcile.NotificationsFrom("enterprise:member:sync", convredis.NewListMQ(client), reconcile.IDFromJSON("workspace_id")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runErr := rt.Run(ctx)
	if runErr == nil || !strings.Contains(runErr.Error(), "BroadcastConsumer") {
		t.Fatalf("Run() = %v, want an error naming BroadcastConsumer", runErr)
	}
}

func TestListMQNotificationsFromRejectedUnderCompetingAtRegister(t *testing.T) {
	client, _, _ := openMini(t)
	rt, err := converge.New(converge.Options{Observer: &convergetest.Recorder{}})
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Job:       reconcile.NewJob("member-sync", reconcile.JobOpts{}),
		RunMode:   converge.Competing,
		Reconcile: func(ctx context.Context, id reconcile.ID) error { return nil },
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Hour)),
			reconcile.NotificationsFrom("enterprise:member:sync", convredis.NewListMQ(client), reconcile.IDFromJSON("workspace_id")),
		},
	})
	if err == nil {
		t.Fatal("Competing is a worker mode; a reconcile job requesting it must be rejected at Register")
	}
}

func recvListDelivery(t *testing.T, ch chan converge.Delivery) converge.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return nil
	}
}

func TestListMQCannotCarryWork(t *testing.T) {
	client, _, _ := openMini(t)
	rt, err := converge.New(converge.Options{
		MQ:       convredis.NewListMQ(client),
		Lease:    inmem.NewLease(),
		KV:       inmem.NewKV(),
		Observer: &convergetest.Recorder{},
	})
	if err != nil {
		t.Fatal(err)
	}
	rotate := worker.NewTask[string]("credential-rotate", worker.TaskOpts{Queue: "dify:credential:rotate"})
	if err := worker.Handle(rt, rotate, func(context.Context, string) error { return nil }, worker.HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runErr := rt.Run(ctx)
	want := `worker: task "credential-rotate": *convredis.listMQ cannot carry work: a worker's MQ needs DelayedPublisher and GroupConsumer`
	if runErr == nil || !strings.Contains(runErr.Error(), want) {
		t.Fatalf("Run() = %v, want %q", runErr, want)
	}
}
