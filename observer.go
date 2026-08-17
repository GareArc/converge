package converge

import "time"

type Observer interface {
	Observe(e Event)
}

type Event interface{ event() }

type RunCompleted struct {
	Job      string
	Surface  Surface
	ID       string
	Attempt  int
	Duration time.Duration
	Err      error
}

func (RunCompleted) event() {}

type LeaseTransition struct {
	Job      string
	Acquired bool
}

func (LeaseTransition) event() {}

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
