package pausegate

import (
	"context"
	"sync"
)

type Gate struct {
	Paused   bool
	resumeCh chan struct{}
}

func New(paused bool) Gate {
	return Gate{Paused: paused}
}

func (g *Gate) SetPaused(paused bool) (changed bool, closeCh chan struct{}) {
	if g.Paused == paused {
		return false, nil
	}
	g.Paused = paused
	if paused {
		return true, nil
	}
	closeCh = g.resumeCh
	g.resumeCh = nil
	return true, closeCh
}

func (g *Gate) resumeChan() chan struct{} {
	if g.resumeCh == nil {
		g.resumeCh = make(chan struct{})
	}
	return g.resumeCh
}

func AwaitUnpaused(ctx context.Context, mu sync.Locker, g *Gate) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		mu.Lock()
		if !g.Paused {
			mu.Unlock()
			return true
		}
		ch := g.resumeChan()
		mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-ch:
		}
	}
}
