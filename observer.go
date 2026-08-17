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

type wakeDiscardKind int

const (
	wakeDiscardUnset wakeDiscardKind = iota
	wakeDiscardParked
	wakeDiscardPaused
	wakeDiscardUndecodable
	wakeDiscardEmptyID
	wakeDiscardOverflow
)

type WakeDiscardReason struct{ kind wakeDiscardKind }

var (
	DiscardParked      = WakeDiscardReason{wakeDiscardParked}
	DiscardPaused      = WakeDiscardReason{wakeDiscardPaused}
	DiscardUndecodable = WakeDiscardReason{wakeDiscardUndecodable}
	DiscardEmptyID     = WakeDiscardReason{wakeDiscardEmptyID}
	DiscardOverflow    = WakeDiscardReason{wakeDiscardOverflow}
)

func (r WakeDiscardReason) IsZero() bool { return r.kind == wakeDiscardUnset }

func (r WakeDiscardReason) String() string {
	switch r.kind {
	case wakeDiscardParked:
		return "parked"
	case wakeDiscardPaused:
		return "paused"
	case wakeDiscardUndecodable:
		return "undecodable"
	case wakeDiscardEmptyID:
		return "empty-id"
	case wakeDiscardOverflow:
		return "overflow"
	default:
		return "unknown"
	}
}

type WakeDiscarded struct {
	Job    string
	ID     string
	Reason WakeDiscardReason
}

func (WakeDiscarded) event() {}

type PassOverrun struct {
	Job string
	Due time.Time
}

func (PassOverrun) event() {}

type IDParked struct {
	Job      string
	ID       string
	Failures int
	Err      error
}

func (IDParked) event() {}

type WrongSurfaceSignal struct {
	Job     string
	ID      string
	Surface Surface
}

func (WrongSurfaceSignal) event() {}

type BackoffFallback struct {
	Job         string
	ID          string
	Consecutive int
}

func (BackoffFallback) event() {}

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
