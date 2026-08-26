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
	wakeDiscardUndecodable
	wakeDiscardEmptyID
	wakeDiscardOverflow
)

type WakeDiscardReason struct{ kind wakeDiscardKind }

var (
	DiscardParked      = WakeDiscardReason{wakeDiscardParked}
	DiscardUndecodable = WakeDiscardReason{wakeDiscardUndecodable}
	DiscardEmptyID     = WakeDiscardReason{wakeDiscardEmptyID}
	DiscardOverflow    = WakeDiscardReason{wakeDiscardOverflow}
)

func (r WakeDiscardReason) IsZero() bool { return r.kind == wakeDiscardUnset }

func (r WakeDiscardReason) String() string {
	switch r.kind {
	case wakeDiscardParked:
		return "parked"
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

type VersionZero struct {
	Job string
	ID  string
}

func (VersionZero) event() {}

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

type deadLetterReasonKind int

const (
	deadLetterReasonUnset deadLetterReasonKind = iota
	deadLetterMaxAttempts
	deadLetterMaxAge
	deadLetterWrongKind
	deadLetterSchemaVersion
	deadLetterUndecodable
	deadLetterWrongSurface
)

type DeadLetterReason struct{ kind deadLetterReasonKind }

var (
	DeadLetterMaxAttempts   = DeadLetterReason{deadLetterMaxAttempts}
	DeadLetterMaxAge        = DeadLetterReason{deadLetterMaxAge}
	DeadLetterWrongKind     = DeadLetterReason{deadLetterWrongKind}
	DeadLetterSchemaVersion = DeadLetterReason{deadLetterSchemaVersion}
	DeadLetterUndecodable   = DeadLetterReason{deadLetterUndecodable}
	DeadLetterWrongSurface  = DeadLetterReason{deadLetterWrongSurface}
)

func (r DeadLetterReason) IsZero() bool { return r.kind == deadLetterReasonUnset }

func (r DeadLetterReason) String() string {
	switch r.kind {
	case deadLetterMaxAttempts:
		return "max-attempts"
	case deadLetterMaxAge:
		return "max-age"
	case deadLetterWrongKind:
		return "wrong-kind"
	case deadLetterSchemaVersion:
		return "schema-version"
	case deadLetterUndecodable:
		return "undecodable"
	case deadLetterWrongSurface:
		return "wrong-surface"
	default:
		return "unknown"
	}
}

type MessageDeadLettered struct {
	Job       string
	Queue     string
	MessageID string
	Attempt   int
	Reason    DeadLetterReason
	Err       error
}

func (MessageDeadLettered) event() {}

type QueueDepth struct {
	Job   string
	Queue string
	Depth int
}

func (QueueDepth) event() {}

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
