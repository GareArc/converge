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
	wakeDiscardUndecodable
	wakeDiscardEmptyID
	wakeDiscardOverflow
)

type WakeDiscardReason struct{ kind wakeDiscardKind }

var (
	DiscardUndecodable = WakeDiscardReason{wakeDiscardUndecodable}
	DiscardEmptyID     = WakeDiscardReason{wakeDiscardEmptyID}
	DiscardOverflow    = WakeDiscardReason{wakeDiscardOverflow}
)

func (r WakeDiscardReason) IsZero() bool { return r.kind == wakeDiscardUnset }

func (r WakeDiscardReason) String() string {
	switch r.kind {
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

type MessageDiscarded struct {
	Job       string
	Queue     string
	MessageID string
	Reason    string
}

func (MessageDiscarded) event() {}

type MessageDeadLettered struct {
	Job       string
	Queue     string
	MessageID string
	Attempt   int
	Reason    string
	Err       error
}

func (MessageDeadLettered) event() {}

type QueueDepth struct {
	Job   string
	Queue string
	Depth int
}

func (QueueDepth) event() {}

type JobDestroyed struct {
	Job   string
	Cause StopCondition
}

func (JobDestroyed) event() {}

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
