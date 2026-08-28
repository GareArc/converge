package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GareArc/converge/internal/durfmt"
	"github.com/robfig/cron/v3"
)

type CronOpts struct {
	Location *time.Location
}

type Cadence struct {
	every time.Duration
	sched cron.Schedule
	loc   *time.Location
	err   error
	expr  string
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
	return Cadence{sched: sched, loc: loc, expr: expr}
}

func (c Cadence) render() string {
	if c.every > 0 {
		return "every " + durfmt.Format(c.every)
	}
	s := "cron " + c.expr
	if c.loc != nil && c.loc != time.UTC {
		s += " (loc: " + c.loc.String() + ")"
	}
	return s
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

func (s *scheduleTrigger) Run(ctx context.Context, notify func(ID)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *scheduleTrigger) NextAfter(t time.Time) time.Time {
	if s.cad.err != nil {
		return time.Time{}
	}
	return s.cad.next(t, t)
}
