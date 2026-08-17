package reconcile

import (
	"context"
	"strconv"
	"time"

	"github.com/GareArc/converge"
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

func (e *engine) runSchedule(ctx context.Context, idx int, st *scheduleTrigger) {
	lastKey := e.key("sched", strconv.Itoa(idx), "last")
	cursorKey := e.key("sched", strconv.Itoa(idx), "cursor")
	for ctx.Err() == nil {
		last, ok := e.readTime(ctx, lastKey)
		now := e.deps.Clock.Now()
		if !ok {
			if !e.runPass(ctx, st, cursorKey) {
				return
			}
			e.writeTime(ctx, lastKey, now)
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
		e.writeTime(ctx, lastKey, latest)
		if over := boundaries(st.cad, latest, e.deps.Clock.Now()); len(over) > 0 {
			for _, due := range over {
				e.deps.Observer.Observe(converge.PassOverrun{Job: e.cfg.name, Due: due})
			}
			e.writeTime(ctx, lastKey, over[len(over)-1])
		}
	}
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
			case <-e.deps.Clock.After(jitter(retry)):
			}
			retry = min(retry*2, pageRetryMax)
			continue
		}
		retry = triggerRestartMin
		for _, id := range ids {
			e.hint(id)
		}
		for _, id := range ids {
			if !e.queue.awaitSettle(ctx, id) {
				return false
			}
		}
		if next == "" {
			e.deps.KV.Delete(ctx, cursorKey)
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
	case <-e.deps.Clock.After(jitter(triggerRestartMin)):
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
