package reconcile_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/sig"
	"github.com/GareArc/converge/reconcile"
)

func TestCheckAgainIsAReconcileSignal(t *testing.T) {
	var err error = reconcile.CheckAgain{In: 10 * time.Second}
	s, ok := sig.FromError(err)
	if !ok || s.ControlSurface() != converge.SurfaceReconcile {
		t.Fatal("CheckAgain must be a reconcile control signal")
	}
}

func TestErrOutdatedIsAReconcileSignal(t *testing.T) {
	s, ok := sig.FromError(reconcile.ErrOutdated)
	if !ok || s.ControlSurface() != converge.SurfaceReconcile {
		t.Fatal("ErrOutdated must be a reconcile control signal")
	}
	if !errors.Is(reconcile.ErrOutdated, reconcile.ErrOutdated) {
		t.Fatal("ErrOutdated must match itself with errors.Is")
	}
}

func TestWrappedSignalsAreDetected(t *testing.T) {
	wrapped := fmt.Errorf("saving: %w", reconcile.CheckAgain{In: time.Second})
	s, ok := sig.FromError(wrapped)
	if !ok {
		t.Fatal("wrapped CheckAgain must still be detected")
	}
	ca, isCA := s.(reconcile.CheckAgain)
	if !isCA || ca.In != time.Second {
		t.Fatalf("detected %T, want CheckAgain{In: 1s}", s)
	}
	deep := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", reconcile.ErrOutdated))
	if s, ok := sig.FromError(deep); !ok || !errors.Is(s, reconcile.ErrOutdated) {
		t.Fatal("doubly wrapped ErrOutdated must be detected")
	}
}

func TestPointerReturnMatches(t *testing.T) {
	var err error = &reconcile.CheckAgain{In: 2 * time.Second}
	s, ok := sig.FromError(err)
	if !ok {
		t.Fatal("pointer CheckAgain must be a signal")
	}
	p, isP := s.(*reconcile.CheckAgain)
	if !isP || p.In != 2*time.Second {
		t.Fatalf("detected %T", s)
	}
}
