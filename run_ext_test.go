package converge_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/hook"
)

func TestRunCleanShutdownReturnsNil(t *testing.T) {
	rt := mustRuntime(t)
	j := newStubJob("a")
	hook.RegisterJob(rt, j)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	select {
	case <-rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready never closed")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown must return nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunPassesDepsWithDefaults(t *testing.T) {
	kv := inmem.NewKV()
	rt, err := converge.New(converge.Options{Namespace: "svc", KV: kv})
	if err != nil {
		t.Fatal(err)
	}
	gotDeps := make(chan converge.JobDeps, 1)
	j := newStubJob("a")
	j.run = func(ctx context.Context, d converge.JobDeps) error {
		gotDeps <- d
		<-ctx.Done()
		return nil
	}
	hook.RegisterJob(rt, j)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.Run(ctx)

	select {
	case d := <-gotDeps:
		if d.Namespace != "svc" || d.KV == nil || d.Observer == nil || d.Clock == nil {
			t.Fatalf("deps not plumbed: %+v", d)
		}
		if d.LeaseTTL != 30*time.Second || d.DrainTimeout != 30*time.Second {
			t.Fatalf("defaults not plumbed: %+v", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}
}

func TestRunJobFailureStopsRuntime(t *testing.T) {
	rt := mustRuntime(t)
	boom := errors.New("boom")
	bad := newStubJob("bad")
	bad.run = func(ctx context.Context, d converge.JobDeps) error { return boom }
	good := newStubJob("good")
	hook.RegisterJob(rt, bad)
	hook.RegisterJob(rt, good)

	err := rt.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the job failure", err)
	}
}

var errKVUnreachable = errors.New("kv unreachable")

type unreachableKV struct {
	converge.KV
}

func (unreachableKV) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, errKVUnreachable
}

func TestRunFailsFastOnAnUnreachableKV(t *testing.T) {
	rt, err := converge.New(converge.Options{KV: unreachableKV{inmem.NewKV()}})
	if err != nil {
		t.Fatal(err)
	}
	hook.RegisterJob(rt, newStubJob("a"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, errKVUnreachable) {
			t.Fatalf("Run = %v, want the store's own error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run must fail fast when the KV is unreachable, not report a healthy job over a dead store")
	}
	select {
	case <-rt.Ready():
		t.Fatal("Run must not report ready over an unreachable KV")
	default:
	}
}

func TestRunTreatsAMissingProbeKeyAsHealthy(t *testing.T) {
	rt, err := converge.New(converge.Options{KV: inmem.NewKV()})
	if err != nil {
		t.Fatal(err)
	}
	hook.RegisterJob(rt, newStubJob("a"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	select {
	case <-rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("an empty KV must not fail the probe; absence is not an error")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil on clean shutdown", err)
	}
}

func TestRunTwiceFails(t *testing.T) {
	rt := mustRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rt.Run(ctx)
	if err := rt.Run(context.Background()); err == nil {
		t.Fatal("second Run must fail")
	}
}

func TestRegisterAfterRunFails(t *testing.T) {
	rt := mustRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rt.Run(ctx)
	if err := hook.RegisterJob(rt, newStubJob("late")); err == nil {
		t.Fatal("register after Run must fail")
	}
}

func TestRunNoJobsBlocksUntilCancel(t *testing.T) {
	rt := mustRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	select {
	case <-rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("empty runtime must still become ready")
	}
	select {
	case <-done:
		t.Fatal("Run must block with no jobs")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
