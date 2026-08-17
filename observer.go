package converge

import "time"

type Observer interface {
	// Observe runs synchronously on engine goroutines: implementations must
	// be fast and non-blocking. Always handle a default case — event types
	// are added in minor releases.
	Observe(e Event)
}

type Event interface{ event() }

type RunCompleted struct {
	Job      string
	Surface  Surface
	ID       string // reconcile ID or worker message ID
	Attempt  int
	Duration time.Duration
	Err      error // nil on success
}

func (RunCompleted) event() {}

type LeaseTransition struct {
	Job      string
	Acquired bool // false = lost or released
}

func (LeaseTransition) event() {}

// Remaining events (WakeDiscarded, PassOverrun, IDDeadLettered,
// MessageDiscarded, QueueDepth, WrongSurfaceSignal) ship with the engines
// that emit them.

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
