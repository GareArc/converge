package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/sig"
)

func TestOutcomesAreWorkerSignals(t *testing.T) {
	for _, err := range []error{Snooze{In: time.Second}, Discard{Reason: "gone"}} {
		s, ok := sig.FromError(fmt.Errorf("wrap: %w", err))
		if !ok || s.ControlSurface() != converge.SurfaceWorker {
			t.Fatalf("%T not detected as a worker signal", err)
		}
	}
	if _, ok := sig.FromError(errors.New("plain")); ok {
		t.Fatal("plain error detected as signal")
	}
}

func TestPointerOutcomesAreWorkerSignals(t *testing.T) {
	for _, err := range []error{&Snooze{In: time.Second}, &Discard{Reason: "gone"}} {
		s, ok := sig.FromError(fmt.Errorf("wrap: %w", err))
		if !ok || s.ControlSurface() != converge.SurfaceWorker {
			t.Fatalf("%T not detected as a worker signal", err)
		}
	}
}
