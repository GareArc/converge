package convergetest

import (
	"sync"
	"time"
)

type Clock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []clockWaiter
}

type clockWaiter struct {
	at time.Time
	ch chan time.Time
}

func NewClock(start time.Time) *Clock { return &Clock{now: start} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, clockWaiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var keep, fire []clockWaiter
	for _, w := range c.waiters {
		if w.at.After(c.now) {
			keep = append(keep, w)
		} else {
			fire = append(fire, w)
		}
	}
	c.waiters = keep
	c.mu.Unlock()
	for _, w := range fire {
		w.ch <- w.at
	}
}
