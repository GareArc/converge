package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

type missedTickKind int

const (
	missedUnset missedTickKind = iota
	missedSkip
	missedRunOnce
	missedCatchup
)

type MissedTickPolicy struct{ kind missedTickKind }

var (
	Skip    = MissedTickPolicy{missedSkip}
	RunOnce = MissedTickPolicy{missedRunOnce}
	Catchup = MissedTickPolicy{missedCatchup}
)

func (p MissedTickPolicy) IsZero() bool { return p.kind == missedUnset }

func (p MissedTickPolicy) String() string {
	switch p.kind {
	case missedSkip:
		return "Skip"
	case missedRunOnce:
		return "RunOnce"
	case missedCatchup:
		return "Catchup"
	default:
		return "unset"
	}
}

type CronOpts struct {
	Location   *time.Location
	MissedTick MissedTickPolicy
}

type Cadence struct {
	every  time.Duration
	sched  cron.Schedule
	loc    *time.Location
	missed MissedTickPolicy
	err    error
}

func Every(d time.Duration) Cadence {
	if d <= 0 {
		return Cadence{err: errors.New("reconcile: Every needs a positive duration")}
	}
	return Cadence{every: d}
}

func Cron(expr string, o CronOpts) Cadence {
	if strings.HasPrefix(strings.TrimSpace(expr), "@") {
		return Cadence{err: fmt.Errorf("reconcile: cron %q: descriptors are not supported, use five fields", expr)}
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return Cadence{err: fmt.Errorf("reconcile: cron %q: %w", expr, err)}
	}
	loc := o.Location
	if loc == nil {
		loc = time.UTC
	}
	return Cadence{sched: sched, loc: loc, missed: o.MissedTick}
}

func (c Cadence) missedTick() MissedTickPolicy {
	if c.missed.IsZero() {
		return RunOnce
	}
	return c.missed
}

func (c Cadence) next(anchor, after time.Time) time.Time {
	if c.every > 0 {
		if after.Before(anchor) {
			return anchor
		}
		n := after.Sub(anchor)/c.every + 1
		return anchor.Add(n * c.every)
	}
	return c.sched.Next(after.In(c.loc))
}

type scheduleTrigger struct {
	source IDSource
	cad    Cadence
}

func Schedule(ids IDSource, c Cadence) PeriodicTrigger {
	return &scheduleTrigger{source: ids, cad: c}
}

func (s *scheduleTrigger) Run(ctx context.Context, wake func(ID)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *scheduleTrigger) NextAfter(t time.Time) time.Time {
	if s.cad.err != nil {
		return time.Time{}
	}
	return s.cad.next(t, t)
}
