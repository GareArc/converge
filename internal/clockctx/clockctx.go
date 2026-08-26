package clockctx

import (
	"context"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

func WithTimeout(parent context.Context, clock converge.Clock, d time.Duration) (context.Context, context.CancelFunc) {
	c := &ctx{Context: parent, done: make(chan struct{}), deadline: clock.Now().Add(d)}
	stop := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		select {
		case <-clock.After(d):
			c.finish(context.DeadlineExceeded)
		case <-parent.Done():
			c.finish(parent.Err())
		case <-stop:
		}
	}()
	return c, func() {
		c.finish(context.Canceled)
		stopOnce.Do(func() { close(stop) })
	}
}

type ctx struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	mu       sync.Mutex
	err      error
}

func (c *ctx) Deadline() (time.Time, bool) { return c.deadline, true }

func (c *ctx) Done() <-chan struct{} { return c.done }

func (c *ctx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *ctx) finish(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
}
