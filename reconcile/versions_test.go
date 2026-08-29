package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

type bumpingVersions struct {
	mu sync.Mutex
	v  Version
}

func (b *bumpingVersions) Latest(context.Context, ID) (Version, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v, nil
}

func (b *bumpingVersions) bump() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.v++
}

func TestVersionAheadOfSnapshotDefersInsteadOfFailing(t *testing.T) {
	versions := &bumpingVersions{}
	var mu sync.Mutex
	calls := 0
	te := startEngine(t, config{versions: versions}, func(context.Context, ID) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			versions.bump()
		}
		return nil
	})
	te.e.notify(context.Background(), "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	advanceUntil(t, te, 100*time.Millisecond, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
	if s := te.e.Stats(); s.ConsecutiveFails != 0 {
		t.Fatalf("a version bump during the run must defer, not fail: %+v", s)
	}
	if n := te.rec.Count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 0 {
		t.Fatalf("a version bump during the run reported %d failed runs, want 0", n)
	}
}

func TestVersionsDoNotNeedKV(t *testing.T) {
	e, err := newEngine(Spec{
		Job:       NewJob("job", JobOpts{}),
		Reconcile: func(context.Context, ID) error { return nil },
		Triggers:  []Trigger{Schedule(SingleID(), Every(time.Hour))},
		Versions:  fakeVersions{},
	})
	if err != nil {
		t.Fatal(err)
	}
	deps := converge.JobDeps{Lease: inmem.NewLease(), Clock: convergetest.NewClock(wqStart), Observer: &convergetest.Recorder{}}
	if err := e.bind(deps); err != nil {
		t.Fatalf("bind with Versions and no KV = %v, want nil", err)
	}
}
