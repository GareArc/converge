package convkratos

import (
	"context"
	"errors"
	"sync"

	"github.com/GareArc/converge"
	"github.com/go-kratos/kratos/v2/transport"
)

var (
	ErrDrainIncomplete = errors.New("convkratos: Stop returned before the runtime finished draining")
	ErrAlreadyStarted  = errors.New("convkratos: Start called on a server that is already running")
)

type server struct {
	rt   *converge.Runtime
	done chan struct{}

	mu      sync.Mutex
	cancel  context.CancelFunc
	started bool
	stopped bool
}

func Server(rt *converge.Runtime) transport.Server {
	return &server{rt: rt, done: make(chan struct{})}
}

func (s *server) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		cancel()
		return nil
	}
	if s.started {
		s.mu.Unlock()
		cancel()
		return ErrAlreadyStarted
	}
	s.cancel = cancel
	s.started = true
	s.mu.Unlock()

	defer cancel()
	defer close(s.done)
	return s.rt.Run(runCtx)
}

func (s *server) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	cancel, started := s.cancel, s.started
	s.mu.Unlock()

	if !started {
		return nil
	}
	cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ErrDrainIncomplete
	}
}
