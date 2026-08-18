package converge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/GareArc/converge/internal/hook"
)

type job interface {
	Name() string
	Run(ctx context.Context, d JobDeps) error
	Ready() <-chan struct{}
	Poke(id string) error
	Stats() JobStats
}

type queueBound interface {
	QueueBinding() (queue string, mq MQ)
}

type queueBinding struct {
	job string
	mq  MQ
}

type Runtime struct {
	opts  Options
	ready chan struct{}

	mu     sync.Mutex
	jobs   map[string]job
	order  []string
	queues map[string]queueBinding
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
	hook.ProducerDeps = func(rt any) (hook.ProducerWiring, error) {
		r, ok := rt.(*Runtime)
		if !ok {
			return hook.ProducerWiring{}, fmt.Errorf("converge: producer: %T is not a *converge.Runtime", rt)
		}
		return hook.ProducerWiring{
			MQ:    r.opts.MQ,
			Clock: r.opts.Clock,
			QueueMQ: func(queue string) any {
				r.mu.Lock()
				defer r.mu.Unlock()
				b, ok := r.queues[queue]
				if !ok || b.mq == nil {
					return nil
				}
				return b.mq
			},
		}, nil
	}
}

func (rt *Runtime) register(j job) error {
	name := j.Name()
	var queue string
	var binding *queueBinding
	if qb, ok := j.(queueBound); ok {
		q, mq := qb.QueueBinding()
		queue = q
		binding = &queueBinding{job: name, mq: mq}
	}
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
	if binding != nil {
		if existing, ok := rt.queues[queue]; ok {
			return fmt.Errorf("converge: job %q: queue %q is already handled by job %q", name, queue, existing.job)
		}
	}
	rt.jobs[name] = j
	rt.order = append(rt.order, name)
	if binding != nil {
		rt.queues[queue] = *binding
	}
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

func (rt *Runtime) Ready() <-chan struct{} { return rt.ready }

func (rt *Runtime) Poke(jobName, id string) error {
	rt.mu.Lock()
	j, ok := rt.jobs[jobName]
	rt.mu.Unlock()
	if !ok {
		return fmt.Errorf("converge: unknown job %q", jobName)
	}
	return j.Poke(id)
}
