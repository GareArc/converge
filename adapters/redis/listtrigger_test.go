package convredis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/reconcile"
)

func assertWake(t *testing.T, woke chan reconcile.ID, want reconcile.ID) {
	t.Helper()
	select {
	case got := <-woke:
		if got != want {
			t.Fatalf("woke = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for wake(%q)", want)
	}
}

func TestListTriggerWakesExtractedIDs(t *testing.T) {
	client, _, _ := openMini(t)
	tr := convredis.ListTrigger(client, "enterprise:member:sync", reconcile.IDFromJSONField("workspace_id"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	woke := make(chan reconcile.ID, 16)
	done := make(chan error, 1)
	go func() { done <- tr.Run(ctx, func(id reconcile.ID) { woke <- id }) }()
	client.LPush(ctx, "enterprise:member:sync", `{"type":"member_added","workspace_id":"ws-1"}`)
	client.LPush(ctx, "enterprise:member:sync", `not json`)
	client.LPush(ctx, "enterprise:member:sync", `{"type":"member_removed","workspace_id":"ws-2"}`)
	assertWake(t, woke, "ws-1")
	assertWake(t, woke, "ws-2")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}

func TestListTriggerDrivesReconcileJob(t *testing.T) {
	client, _, _ := openMini(t)
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name:      "member-sync",
		Reconcile: func(ctx context.Context, id reconcile.ID) error { return nil },
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) { return nil, nil }), reconcile.Every(time.Hour)),
			convredis.ListTrigger(client, "enterprise:member:sync", reconcile.IDFromJSONField("workspace_id")),
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
