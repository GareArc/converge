package converge

import (
	"errors"
	"time"
)

type Observer interface {
	Observe(e Event)
}

type Event interface{ event() }

type outcomeKind int

const (
	outcomeUnknown outcomeKind = iota
	outcomeSucceeded
	outcomeRetrying
	outcomeDeferred
	outcomeDiscarded
	outcomeShelved
)

type Outcome struct{ kind outcomeKind }

var (
	Succeeded = Outcome{outcomeSucceeded}
	Retrying  = Outcome{outcomeRetrying}
	Deferred  = Outcome{outcomeDeferred}
	Discarded = Outcome{outcomeDiscarded}
	Shelved   = Outcome{outcomeShelved}
)

func (o Outcome) String() string {
	switch o.kind {
	case outcomeSucceeded:
		return "succeeded"
	case outcomeRetrying:
		return "retrying"
	case outcomeDeferred:
		return "deferred"
	case outcomeDiscarded:
		return "discarded"
	case outcomeShelved:
		return "shelved"
	default:
		return "unknown"
	}
}

var (
	ErrNotificationUndecodable = errors.New("converge: notification: undecodable")
	ErrNotificationEmptyID     = errors.New("converge: notification: empty id")
	ErrInboxOverflow           = errors.New("converge: notification: inbox overflow")
)

type RunCompleted struct {
	Job      string
	ID       string
	Attempt  int
	Duration time.Duration
	Outcome  Outcome
	Err      error
}

func (RunCompleted) event() {}

type LeaseChanged struct {
	Job  string
	Held bool
}

func (LeaseChanged) event() {}

type ScheduleOverrun struct {
	Job  string
	Due  time.Time
	Late time.Duration
}

func (ScheduleOverrun) event() {}

type NotificationDropped struct {
	Job string
	ID  string
	Err error
}

func (NotificationDropped) event() {}

type JobDestroyed struct {
	Job   string
	Cause StopCondition
}

func (JobDestroyed) event() {}

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
