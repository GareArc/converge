package converge

import (
	"time"

	"github.com/GareArc/converge/internal/hook"
)

type stopKind int

const (
	stopUnset stopKind = iota
	stopDeadline
	stopKeyKind
)

type StopCondition struct {
	kind stopKind
	at   time.Time
	key  string
}

func Deadline(t time.Time) StopCondition {
	return StopCondition{kind: stopDeadline, at: t}
}

func StopKey(key string) StopCondition {
	return StopCondition{kind: stopKeyKind, key: key}
}

func (c StopCondition) IsZero() bool { return c.kind == stopUnset }

func (c StopCondition) String() string {
	switch c.kind {
	case stopDeadline:
		return "deadline " + c.at.Format(time.RFC3339)
	case stopKeyKind:
		return "stop key " + c.key
	default:
		return "none"
	}
}

func (c StopCondition) deadlineAt() (time.Time, bool) {
	return c.at, c.kind == stopDeadline
}

func (c StopCondition) stopKey() (string, bool) {
	return c.key, c.kind == stopKeyKind
}

type stateKind int

const (
	stateUnset stateKind = iota
	stateNotStarted
	stateActive
	stateDestroyed
)

type State struct{ kind stateKind }

var (
	NotStarted = State{stateNotStarted}
	Active     = State{stateActive}
	Destroyed  = State{stateDestroyed}
)

func (s State) IsZero() bool { return s.kind == stateUnset }

func (s State) String() string {
	switch s.kind {
	case stateNotStarted:
		return "not started"
	case stateActive:
		return "active"
	case stateDestroyed:
		return "destroyed"
	default:
		return "unknown"
	}
}

func init() {
	hook.StopConditionDeadline = func(c any) (time.Time, bool) {
		sc, ok := c.(StopCondition)
		if !ok {
			return time.Time{}, false
		}
		return sc.deadlineAt()
	}
	hook.StopConditionKey = func(c any) (string, bool) {
		sc, ok := c.(StopCondition)
		if !ok {
			return "", false
		}
		return sc.stopKey()
	}
}
