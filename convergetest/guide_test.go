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

	blockingEnabled bool
	runStarted      chan struct{}
	runCanceled     chan struct{}

	startOnce    sync.Once
	canceledOnce sync.Once
}

func newGuideRepo() *guideRepo {
	return &guideRepo{
		workspaceIDs: []string{"ws_42"},
		appIDs:       []string{"app_13"},
		runStarted:   make(chan struct{}),
		runCanceled:  make(chan struct{}),
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
	if appID == "app_13" {
		r.mu.Lock()
		blocking := r.blockingEnabled
		r.mu.Unlock()
		if !blocking {
			return errAppRunnerDownstream
		}
		return nil
	}
	if appID != "app_14" {
		return nil
	}
	r.mu.Lock()
	blocking := r.blockingEnabled
	r.mu.Unlock()
	if !blocking {
		return nil
	}
	r.startOnce.Do(func() { close(r.runStarted) })
	<-ctx.Done()
	r.canceledOnce.Do(func() { close(r.runCanceled) })
	return ctx.Err()
}

func (r *guideRepo) enableAppBlock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockingEnabled = true
}

func reconciledCount(h *convergetest.Harness, job, id string) int {
	n := 0
	for _, e := range h.Events() {
		rc, ok := e.(converge.RunCompleted)
		if ok && rc.Job == job && rc.ID == id && rc.Err == nil {
			n++
		}
	}
	return n
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

	h.Drain(t)
	baselineReconciled := reconciledCount(h, "workspace-credentials", "ws_42")

	h.Wake("workspace-credentials", "ws_42")

	h.Drain(t)

	afterWakeReconciled := reconciledCount(h, "workspace-credentials", "ws_42")
	if afterWakeReconciled <= baselineReconciled {
		t.Fatalf("workspace-credentials ws_42 RunCompleted count after Wake+Drain = %d, want > %d (the wake must drive a real run, not just the startup pass)", afterWakeReconciled, baselineReconciled)
	}

	h.Clock.Advance(24 * time.Hour)

	h.Drain(t)

	afterAdvanceReconciled := reconciledCount(h, "workspace-credentials", "ws_42")
	if afterAdvanceReconciled <= afterWakeReconciled {
		t.Fatalf("workspace-credentials ws_42 RunCompleted count after Advance+Drain = %d, want > %d (the 24h advance must cross the schedule boundary and drive a real run)", afterAdvanceReconciled, afterWakeReconciled)
	}

	h.RunPass(t, "workspace-credentials")

	afterRunPassReconciled := reconciledCount(h, "workspace-credentials", "ws_42")
	if afterRunPassReconciled <= afterAdvanceReconciled {
		t.Fatalf("workspace-credentials ws_42 RunCompleted count after RunPass = %d, want > %d (RunPass must force a real additional pass)", afterRunPassReconciled, afterAdvanceReconciled)
	}

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
	beforeWake := len(h.Events())
	h.Wake("app-runner", "app_14")
	select {
	case <-fakeRepo.runStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("app_14 blocking run never started")
	}

	h.Lease.Expire("app-runner")

	select {
	case <-fakeRepo.runCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("app_14 run was not canceled on lease loss")
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
	for _, e := range h.Events()[beforeWake:] {
		rc, ok := e.(converge.RunCompleted)
		if ok && rc.Job == "app-runner" && rc.ID == "app_14" {
			t.Fatalf("app_14 cancellation must be neutral, not observed as a completed run: %+v", rc)
		}
	}
}
