package converge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/GareArc/converge/internal/hook"
)

// job is the engine contract. It stays unexported: surface packages satisfy
// it structurally and register through internal/hook, which keeps outside
// modules from injecting foreign jobs.
type job interface {
	Name() string
	// Run blocks until ctx is canceled, drains in flight-work within
	// JobDeps.DrainTimeout, and returns nil on a clean stop.
	Run(ctx context.Context, d JobDeps) error
	Ready() <-chan struct{}
	Poke(id string) error
	Stats() JobStats
}

type Runtime struct {
	opts  Options
	ready chan struct{}

	mu     sync.Mutex
	jobs   map[string]job
	order  []string
	frozen bool
}

func init() {
	hook.RegisterJob = func(rt any, j any) error {
		r, ok := rt.(*Runtime)
		if !ok {
			return fmt.Errorf("converge: register: %T is not a *converge.Runtime", rt)
		}
		jj, ok := j.(job)
		if !ok {
			return fmt.Errorf("converge: register: %T does not satisfy the engine job contract", j)
		}
		return r.register(jj)
	}
}

func (rt *Runtime) register(j job) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.frozen {
		return fmt.Errorf("converge: register %q: runtime already running", j.Name())
	}
	if j.Name() == "" {
		return errors.New("converge: job name must not be empty")
	}
	if _, dup := rt.jobs[j.Name()]; dup {
		return fmt.Errorf("converge: duplicate job name %q", j.Name())
	}
	rt.jobs[j.Name()] = j
	rt.order = append(rt.order, j.Name())
	return nil
}

func (rt *Runtime) Stats() []JobStats {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]JobStats, 0, len(rt.order))
	for _, name := range rt.order {
		out = append(out, rt.jobs[name].Stats())
	}
	return out
}
