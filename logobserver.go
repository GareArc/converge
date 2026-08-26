package converge

import (
	"context"
	"log/slog"
)

type logObserver struct {
	log *slog.Logger
}

func LogObserver(l *slog.Logger) Observer {
	if l == nil {
		return noopObserver{}
	}
	return logObserver{log: l}
}

func (o logObserver) Observe(e Event) {
	switch v := e.(type) {
	case RunCompleted:
		o.log.LogAttrs(context.Background(), runCompletedLevel(v.Outcome), "converge: run completed",
			slog.String("job", v.Job),
			slog.String("id", v.ID),
			slog.Int("attempt", v.Attempt),
			slog.Duration("duration", v.Duration),
			slog.String("outcome", v.Outcome.String()),
			errAttr(v.Err),
		)
	case LeaseChanged:
		o.log.LogAttrs(context.Background(), slog.LevelInfo, "converge: lease changed",
			slog.String("job", v.Job),
			slog.Bool("held", v.Held),
		)
	case ScheduleOverrun:
		o.log.LogAttrs(context.Background(), slog.LevelWarn, "converge: schedule overrun",
			slog.String("job", v.Job),
			slog.Time("due", v.Due),
			slog.Duration("late", v.Late),
		)
	case NotificationDropped:
		o.log.LogAttrs(context.Background(), slog.LevelWarn, "converge: notification dropped",
			slog.String("job", v.Job),
			slog.String("id", v.ID),
			errAttr(v.Err),
		)
	case JobDestroyed:
		o.log.LogAttrs(context.Background(), slog.LevelInfo, "converge: job destroyed",
			slog.String("job", v.Job),
			slog.String("cause", v.Cause.String()),
		)
	}
}

func runCompletedLevel(o Outcome) slog.Level {
	switch o {
	case Succeeded, Discarded:
		return slog.LevelInfo
	case Retrying, Deferred:
		return slog.LevelWarn
	case Shelved:
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}

func errAttr(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String("err", err.Error())
}
