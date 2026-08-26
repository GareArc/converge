package converge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/internal/keys"
)

type job interface {
	Name() string
	Run(ctx context.Context, d JobDeps) error
	Ready() <-chan struct{}
	Stats() JobStats
	Info() JobInfo
	Quiet() bool
	Notify(id string) error
	Sweep(ctx context.Context) error
}

type Runtime struct {
	opts    Options
	ready   chan struct{}
	replica string

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
	hook.Inspect = func(rt any) (any, error) {
		r, ok := rt.(*Runtime)
		if !ok || r == nil {
			return nil, fmt.Errorf("converge: inspect: %T is not a usable *converge.Runtime", rt)
		}
		r.mu.Lock()
		jobs := make([]job, 0, len(r.order))
		for _, name := range r.order {
			jobs = append(jobs, r.jobs[name])
		}
		r.mu.Unlock()
		out := make([]JobInfo, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, j.Info())
		}
		return out, nil
	}
	hook.OpsDeps = func(rt any) (hook.OpsWiring, error) {
		r, ok := rt.(*Runtime)
		if !ok || r == nil {
			return hook.OpsWiring{}, fmt.Errorf("converge: ops: %T is not a usable *converge.Runtime", rt)
		}
		return hook.OpsWiring{
			KV:        r.opts.KV,
			MQ:        r.opts.MQ,
			Clock:     r.opts.Clock,
			Namespace: r.opts.Namespace,
			Replica:   r.replica,
		}, nil
	}
	hook.Notify = func(rt any, jobName, id string) error {
		r, ok := rt.(*Runtime)
		if !ok || r == nil {
			return fmt.Errorf("converge: notify: %T is not a usable *converge.Runtime", rt)
		}
		r.mu.Lock()
		j, ok := r.jobs[jobName]
		r.mu.Unlock()
		if !ok {
			return fmt.Errorf("converge: unknown job %q", jobName)
		}
		return j.Notify(id)
	}
	hook.Sweep = func(rt any, ctx context.Context, jobName string) error {
		r, ok := rt.(*Runtime)
		if !ok || r == nil {
			return fmt.Errorf("converge: run-pass-now: %T is not a usable *converge.Runtime", rt)
		}
		r.mu.Lock()
		j, ok := r.jobs[jobName]
		r.mu.Unlock()
		if !ok {
			return fmt.Errorf("converge: unknown job %q", jobName)
		}
		return j.Sweep(ctx)
	}
	hook.Quiet = func(rt any) bool {
		r, ok := rt.(*Runtime)
		if !ok || r == nil {
			return false
		}
		r.mu.Lock()
		jobs := make([]job, 0, len(r.order))
		for _, name := range r.order {
			jobs = append(jobs, r.jobs[name])
		}
		r.mu.Unlock()
		for _, j := range jobs {
			if !j.Quiet() {
				return false
			}
		}
		return true
	}
	hook.AttachOptions = func(o any, attach func(rt any)) any {
		opts, ok := o.(Options)
		if !ok {
			return o
		}
		opts.attach = func(rt *Runtime) { attach(rt) }
		return opts
	}
	hook.FailingIDs = func(rt any, jobName string) (any, error) {
		r, ok := rt.(*Runtime)
		if !ok || r == nil {
			return nil, fmt.Errorf("converge: failing: %T is not a usable *converge.Runtime", rt)
		}
		r.mu.Lock()
		j, ok := r.jobs[jobName]
		r.mu.Unlock()
		if !ok {
			return nil, fmt.Errorf("converge: unknown job %q", jobName)
		}
		fl, ok := j.(interface{ FailingIDs() []FailingID })
		if !ok {
			return nil, nil
		}
		return fl.FailingIDs(), nil
	}
}

func (rt *Runtime) register(j job) error {
	name := j.Name()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.frozen {
		return fmt.Errorf("converge: register %q: runtime already running", name)
	}
	if name == "" {
		return errors.New("converge: job name must not be empty")
	}
	if _, dup := rt.jobs[name]; dup {
		return fmt.Errorf("converge: duplicate job name %q", name)
	}
	rt.jobs[name] = j
	rt.order = append(rt.order, name)
	return nil
}

func (rt *Runtime) Stats() []JobStats {
	rt.mu.Lock()
	jobs := make([]job, 0, len(rt.order))
	for _, name := range rt.order {
		jobs = append(jobs, rt.jobs[name])
	}
	rt.mu.Unlock()
	out := make([]JobStats, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Stats())
	}
	return out
}

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

	if err := rt.probeKV(ctx); err != nil {
		return err
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

func (rt *Runtime) probeKV(ctx context.Context) error {
	if rt.opts.KV == nil {
		return nil
	}
	_, _, err := rt.opts.KV.Get(ctx, keys.Probe(rt.opts.Namespace))
	if err == nil || ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("converge: KV is unreachable: %w", err)
}

func (rt *Runtime) Ready() <-chan struct{} { return rt.ready }
