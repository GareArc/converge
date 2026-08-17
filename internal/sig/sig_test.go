package sig_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/sig"
)

type fakeSignal struct{ surface converge.Surface }

func (s fakeSignal) Error() string                    { return "fake signal" }
func (s fakeSignal) ControlSurface() converge.Surface { return s.surface }

func TestFromErrorFindsSignal(t *testing.T) {
	s, ok := sig.FromError(fakeSignal{converge.SurfaceReconcile})
	if !ok || s.ControlSurface() != converge.SurfaceReconcile {
		t.Fatalf("got %v, %v", s, ok)
	}
}

func TestFromErrorPlainErrorIsNotASignal(t *testing.T) {
	if _, ok := sig.FromError(errors.New("boom")); ok {
		t.Fatal("plain error must not be a signal")
	}
	if _, ok := sig.FromError(nil); ok {
		t.Fatal("nil must not be a signal")
	}
}

func TestFromErrorWrappedSignalStillDetected(t *testing.T) {
	err := fmt.Errorf("context: %w", fakeSignal{converge.SurfaceWorker})
	s, ok := sig.FromError(err)
	if !ok || s.ControlSurface() != converge.SurfaceWorker {
		t.Fatalf("got %v, %v", s, ok)
	}
}

func TestFromErrorOutermostWins(t *testing.T) {
	inner := fakeSignal{converge.SurfaceWorker}
	outer := wrapSignal{fakeSignal{converge.SurfaceReconcile}, inner}
	s, ok := sig.FromError(outer)
	if !ok || s.ControlSurface() != converge.SurfaceReconcile {
		t.Fatalf("outermost must win, got %v, %v", s, ok)
	}
}

type wrapSignal struct {
	fakeSignal
	wrapped error
}

func (w wrapSignal) Unwrap() error { return w.wrapped }
