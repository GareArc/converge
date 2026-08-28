package clockctx

import (
	"context"
	"errors"
	"time"

	"github.com/GareArc/converge"
)

func WithTimeout(parent context.Context, clock converge.Clock, d time.Duration) (context.Context, context.CancelFunc) {
	inner, cancelCause := context.WithCancelCause(parent)
	expired := clock.After(d)
	go func() {
		select {
		case <-expired:
			cancelCause(context.DeadlineExceeded)
		case <-inner.Done():
		}
	}()
	return &ctx{Context: inner}, func() { cancelCause(context.Canceled) }
}

type ctx struct{ context.Context }

func (c *ctx) Err() error {
	err := c.Context.Err()
	if err == nil {
		return nil
	}
	if errors.Is(context.Cause(c.Context), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return err
}
