package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
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
			return ErrOutdated
		}
		return nil
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	advanceUntil(t, te, 100*time.Millisecond, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
	if s := te.e.Stats(); s.ConsecutiveFails != 0 {
		t.Fatalf("ErrOutdated from a version bump must defer, not fail: %+v", s)
	}
}
