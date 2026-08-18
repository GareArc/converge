package convergetest_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/internal/credcheck"
	"github.com/GareArc/converge/convergetest/internal/jobs"
	"github.com/GareArc/converge/worker"
)

var errAppRunnerDownstream = errors.New("app-runner: downstream rejected")

type guideRepo struct {
	mu sync.Mutex

	workspaceIDs []string
	appIDs       []string

	app13Blocking bool
	app13Started  chan struct{}
	app13Canceled chan struct{}

	startOnce    sync.Once
	canceledOnce sync.Once
}

func newGuideRepo() *guideRepo {
	return &guideRepo{
		workspaceIDs:  []string{"ws_42"},
		appIDs:        []string{"app_13"},
		app13Started:  make(chan struct{}),
		app13Canceled: make(chan struct{}),
	}
}

func (r *guideRepo) WorkspaceIDs(ctx context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.workspaceIDs...), nil
}

func (r *guideRepo) SyncCredentials(ctx context.Context, workspaceID string) error {
	return nil
}

func (r *guideRepo) AppIDs(ctx context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.appIDs...), nil
}

func (r *guideRepo) RunApp(ctx context.Context, appID string) error {
	if appID != "app_13" {
		return nil
	}
	r.mu.Lock()
	blocking := r.app13Blocking
	r.mu.Unlock()
	if !blocking {
		return errAppRunnerDownstream
	}
	r.startOnce.Do(func() { close(r.app13Started) })
	<-ctx.Done()
	r.canceledOnce.Do(func() { close(r.app13Canceled) })
	return ctx.Err()
}

func (r *guideRepo) enableAppBlock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.app13Blocking = true
}

func TestGuideSection5TestingWorkflow(t *testing.T) {
	fakeRepo := newGuideRepo()
	errBoom := errors.New("boom")

	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}

	_, err = credcheck.NewReconciler(rt, fakeRepo)
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Handle(rt, jobs.SendInvite, func(ctx context.Context, p jobs.Invite) error {
		return nil
	}, worker.HandleOpts{}); err != nil {
		t.Fatal(err)
	}

	h.Wake("workspace-credentials", "ws_42")

	h.Drain(t)

	h.Clock.Advance(24 * time.Hour)

	h.RunPass(t, "workspace-credentials")

	h.AssertReconciled(t, "workspace-credentials", "ws_42")

	h.AssertParked(t, "app-runner", "app_13")

	prod, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	wantPayload := jobs.Invite{Email: "new-member@example.com"}
	if err := jobs.SendInvite.Enqueue(context.Background(), prod, wantPayload, worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	h.AssertEnqueued(t, jobs.SendInvite, wantPayload)

	h.MQ.FailNextPublish(errBoom)

	failedPayload := jobs.Invite{Email: "outage@example.com"}
	if err := jobs.SendInvite.Enqueue(context.Background(), prod, failedPayload, worker.EnqueueOpts{}); !errors.Is(err, errBoom) {
		t.Fatalf("Enqueue after FailNextPublish = %v, want %v", err, errBoom)
	}

	fakeRepo.enableAppBlock()
	beforePoke := len(h.Events())
	if err := rt.Poke("app-runner", "app_13"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fakeRepo.app13Started:
	case <-time.After(2 * time.Second):
		t.Fatal("app_13 blocking run never started")
	}

	h.Lease.Expire("app-runner")

	select {
	case <-fakeRepo.app13Canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("app_13 run was not canceled on lease loss")
	}
	convergetest.Await(t, func() bool {
		for _, e := range h.Events() {
			lt, ok := e.(converge.LeaseTransition)
			if ok && lt.Job == "app-runner" && !lt.Acquired {
				return true
			}
		}
		return false
	})
	for _, e := range h.Events()[beforePoke:] {
		rc, ok := e.(converge.RunCompleted)
		if ok && rc.Job == "app-runner" && rc.ID == "app_13" {
			t.Fatalf("app_13 cancellation must be neutral, not observed as a completed run: %+v", rc)
		}
	}
}
