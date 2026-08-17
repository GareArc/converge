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
	// Run blocks until ctx is canceled, drains in-flight work within
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

// Run freezes the registry, starts every job, and blocks. Cancel ctx to
// stop: intake stops, in-flight work drains within Options.DrainTimeout,
// leases release. Returns nil on a clean shutdown; a non-nil return is
// always a real failure (and one job's failure stops the runtime).
func (rt *Runtime) Run(ctx context.Context) error {
	rt.mu.Lock()
	if rt.frozen {
		rt.mu.Unlock()
		return errors.New("converge: Run called twice")
	}
	rt.frozen = true
	jobs := make([]job, 0, len(rt.order))
	for _, name := range rt.order {
		jobs = append(jobs, rt.jobs[name])
	}
	rt.mu.Unlock()

	deps := JobDeps{
		MQ:           rt.opts.MQ,
		Lease:        rt.opts.Lease,
		KV:           rt.opts.KV,
		Observer:     rt.opts.Observer,
		Clock:        rt.opts.Clock,
		Namespace:    rt.opts.Namespace,
		LeaseTTL:     rt.opts.LeaseTTL,
		DrainTimeout: rt.opts.DrainTimeout,
		Middleware:   rt.opts.Middleware,
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for _, j := range jobs {
			select {
			case <-j.Ready():
			case <-runCtx.Done():
				return
			}
		}
		close(rt.ready)
	}()

	if len(jobs) == 0 {
		<-runCtx.Done()
		return nil
	}

	results := make(chan error, len(jobs))
	for _, j := range jobs {
		go func(j job) { results <- j.Run(runCtx, deps) }(j)
	}

	var failures []error
	for range jobs {
		if err := <-results; err != nil && !errors.Is(err, context.Canceled) {
			failures = append(failures, err)
			cancel()
		}
	}
	return errors.Join(failures...)
}

// Ready closes when every registered job's consumers and triggers are live.
// With no jobs it closes as soon as Run starts. Select against your own context
// as well: if shutdown wins before every job becomes ready, this channel never closes.
func (rt *Runtime) Ready() <-chan struct{} { return rt.ready }

// Poke wakes one ID of one job: bypasses backoff and revives parked IDs
// (guide §3.1). Routing beyond this process arrives with plan 05.
func (rt *Runtime) Poke(jobName, id string) error {
	rt.mu.Lock()
	j, ok := rt.jobs[jobName]
	rt.mu.Unlock()
	if !ok {
		return fmt.Errorf("converge: unknown job %q", jobName)
	}
	return j.Poke(id)
}
