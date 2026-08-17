package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/GareArc/converge/inmem"
)

func TestMarkChangedIncrementsFromZero(t *testing.T) {
	tr := NewTracker(inmem.NewKV(), "job")
	ctx := context.Background()
	for want := Version(1); want <= 3; want++ {
		got, err := tr.MarkChanged(ctx, "a")
		if err != nil || got != want {
			t.Fatalf("MarkChanged = %d, %v; want %d", got, err, want)
		}
	}
	if v, err := tr.Latest(ctx, "a"); err != nil || v != 3 {
		t.Fatalf("Latest = %d, %v", v, err)
	}
}

func TestLatestOfUntrackedIDIsZero(t *testing.T) {
	tr := NewTracker(inmem.NewKV(), "job")
	if v, err := tr.Latest(context.Background(), "ghost"); err != nil || v != 0 {
		t.Fatalf("Latest = %d, %v; want 0, nil", v, err)
	}
}

func TestConcurrentMarkChangedNeverLosesABump(t *testing.T) {
	tr := NewTracker(inmem.NewKV(), "job")
	ctx := context.Background()
	const workers = 8
	var wg sync.WaitGroup
	for range make([]struct{}, workers) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := tr.MarkChanged(ctx, "a"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if v, err := tr.Latest(ctx, "a"); err != nil || v != workers {
		t.Fatalf("Latest = %d, %v; want %d", v, err, workers)
	}
}

func TestMarkAppliedRefusesOnlyMovedPast(t *testing.T) {
	tr := NewTracker(inmem.NewKV(), "job")
	ctx := context.Background()
	v, _ := tr.MarkChanged(ctx, "a")
	if err := tr.MarkApplied(ctx, "a", v); err != nil {
		t.Fatalf("current version refused: %v", err)
	}
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := tr.MarkApplied(ctx, "a", v); !errors.Is(err, ErrOutdated) {
		t.Fatalf("stale MarkApplied = %v; want ErrOutdated", err)
	}
	if err := tr.Forget(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := tr.MarkApplied(ctx, "a", v); err != nil {
		t.Fatalf("MarkApplied after Forget = %v; want nil", err)
	}
}

func TestForgetAbsentSucceedsAndResetsCounter(t *testing.T) {
	tr := NewTracker(inmem.NewKV(), "job")
	ctx := context.Background()
	if err := tr.Forget(ctx, "never-seen"); err != nil {
		t.Fatal(err)
	}
	tr.MarkChanged(ctx, "a")
	tr.MarkChanged(ctx, "a")
	tr.Forget(ctx, "a")
	if v, _ := tr.MarkChanged(ctx, "a"); v != 1 {
		t.Fatalf("MarkChanged after Forget = %d; want 1", v)
	}
}

func TestNamespacesAreIsolated(t *testing.T) {
	kv := inmem.NewKV()
	ctx := context.Background()
	a := NewTracker(kv, "job-a")
	b := NewTracker(kv, "job-b")
	a.MarkChanged(ctx, "x")
	if v, err := b.Latest(ctx, "x"); err != nil || v != 0 {
		t.Fatalf("namespace leak: %d, %v", v, err)
	}
}

func TestCorruptStoredVersionErrors(t *testing.T) {
	kv := inmem.NewKV()
	ctx := context.Background()
	tr := NewTracker(kv, "job")
	if err := kv.Set(ctx, "converge/tracker/job/a", []byte("not-a-number"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Latest(ctx, "a"); err == nil {
		t.Fatal("Latest on corrupt record must error")
	}
	if _, err := tr.MarkChanged(ctx, "a"); err == nil {
		t.Fatal("MarkChanged on corrupt record must error")
	}
	if err := tr.MarkApplied(ctx, "a", 1); err == nil || errors.Is(err, ErrOutdated) {
		t.Fatalf("MarkApplied on corrupt record = %v; want a plain error", err)
	}
}

func TestMisconstructedTrackerErrorsOnUse(t *testing.T) {
	ctx := context.Background()
	for _, tr := range []*Tracker{NewTracker(nil, "job"), NewTracker(inmem.NewKV(), "")} {
		if _, err := tr.MarkChanged(ctx, "a"); err == nil {
			t.Fatal("misconstructed Tracker must error, not panic")
		}
		if _, err := tr.Latest(ctx, "a"); err == nil {
			t.Fatal("misconstructed Tracker must error, not panic")
		}
	}
}
