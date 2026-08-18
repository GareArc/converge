package reconcile

import (
	"context"
	"strconv"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
)

const (
	maxMissedBoundaries = 1000
	pageRetryMax        = time.Minute
)

func boundaries(c Cadence, last, now time.Time) []time.Time {
	var out []time.Time
	t := last
	for len(out) < maxMissedBoundaries {
		t = c.next(last, t)
		if t.After(now) {
			break
		}
		out = append(out, t)
	}
	return out
}

func latestBoundary(c Cadence, from, now time.Time) time.Time {
	if now.Before(from) {
		return from
	}
	if c.every > 0 {
		return from.Add(now.Sub(from) / c.every * c.every)
	}
	for {
		n := c.sched.Next(from.In(c.loc))
		if n.After(now) {
			return from
		}
		from = n
	}
}

func (e *engine) runSchedule(ctx context.Context, idx int, st *scheduleTrigger) {
	if st.cad.err != nil {
		return
	}
	lastKey := e.key("sched", strconv.Itoa(idx), "last")
	cursorKey := e.key("sched", strconv.Itoa(idx), "cursor")
	readLast := func(ctx context.Context) (time.Time, bool) { return e.readTime(ctx, lastKey) }
	writeLast := func(ctx context.Context, t time.Time) { e.writeTime(ctx, lastKey, t) }
	if e.cfg.runMode == converge.OnAllReplicas {
		var local time.Time
		readLast = func(context.Context) (time.Time, bool) { return local, !local.IsZero() }
		writeLast = func(_ context.Context, t time.Time) { local = t }
	}
	for ctx.Err() == nil {
		last, ok := readLast(ctx)
		now := e.deps.Clock.Now()
		if !ok {
			if !e.runPass(ctx, st, cursorKey) {
				return
			}
			writeLast(ctx, now)
			e.checkOverrun(ctx, st, writeLast, now)
			continue
		}
		pending := boundaries(st.cad, last, now)
		if len(pending) == 0 {
			next := st.cad.next(last, now)
			select {
			case <-ctx.Done():
				return
			case <-e.deps.Clock.After(next.Sub(now)):
			}
			continue
		}
		switch {
		case len(pending) == 1 || st.cad.missedTick() == RunOnce:
			if !e.runPass(ctx, st, cursorKey) {
				return
			}
		case st.cad.missedTick() == Catchup:
			for range pending {
				if !e.runPass(ctx, st, cursorKey) {
					return
				}
			}
		}
		latest := pending[len(pending)-1]
		if st.cad.missedTick() != Catchup {
			latest = latestBoundary(st.cad, latest, now)
		}
		writeLast(ctx, latest)
		e.checkOverrun(ctx, st, writeLast, latest)
	}
}

func (e *engine) checkOverrun(ctx context.Context, st *scheduleTrigger, writeLast func(context.Context, time.Time), anchor time.Time) {
	now := e.deps.Clock.Now()
	over := boundaries(st.cad, anchor, now)
	if len(over) == 0 {
		return
	}
	for _, due := range over {
		e.deps.Observer.Observe(converge.PassOverrun{Job: e.cfg.name, Due: due})
	}
	writeLast(ctx, latestBoundary(st.cad, over[len(over)-1], now))
}

func (e *engine) runPass(ctx context.Context, st *scheduleTrigger, cursorKey string) bool {
	cursor := e.readString(ctx, cursorKey)
	retry := triggerRestartMin
	for {
		ids, next, err := st.source.page(ctx, cursor)
		if ctx.Err() != nil {
			return false
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			case <-e.deps.Clock.After(backoff.Jitter(retry)):
			}
			retry = min(retry*2, pageRetryMax)
			continue
		}
		retry = triggerRestartMin
		for _, id := range ids {
			e.hint(ctx, id)
		}
		if next == "" {
			e.deleteKey(ctx, cursorKey)
			return true
		}
		cursor = next
		e.writeString(ctx, cursorKey, cursor)
	}
}

func (e *engine) pauseOnInfraError(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-e.deps.Clock.After(backoff.Jitter(triggerRestartMin)):
		return true
	}
}

func (e *engine) readTime(ctx context.Context, key string) (time.Time, bool) {
	for {
		val, ok, err := e.deps.KV.Get(ctx, key)
		if err == nil {
			if !ok {
				return time.Time{}, false
			}
			t, perr := time.Parse(time.RFC3339Nano, string(val))
			if perr != nil {
				return time.Time{}, false
			}
			return t, true
		}
		if !e.pauseOnInfraError(ctx) {
			return time.Time{}, false
		}
	}
}

func (e *engine) writeTime(ctx context.Context, key string, t time.Time) {
	e.writeString(ctx, key, t.Format(time.RFC3339Nano))
}

func (e *engine) readString(ctx context.Context, key string) string {
	for {
		val, ok, err := e.deps.KV.Get(ctx, key)
		if err == nil {
			if !ok {
				return ""
			}
			return string(val)
		}
		if !e.pauseOnInfraError(ctx) {
			return ""
		}
	}
}

func (e *engine) writeString(ctx context.Context, key, val string) {
	for {
		if err := e.deps.KV.Set(ctx, key, []byte(val), 0); err == nil {
			return
		}
		if !e.pauseOnInfraError(ctx) {
			return
		}
	}
}

func (e *engine) deleteKey(ctx context.Context, key string) {
	for {
		if err := e.deps.KV.Delete(ctx, key); err == nil {
			return
		}
		if !e.pauseOnInfraError(ctx) {
			return
		}
	}
}
