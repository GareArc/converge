package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GareArc/converge"
)

type EnqueueOpts struct {
	Delay   time.Duration
	Headers map[string]string
}

type Producer[T any] struct {
	task  Task[T]
	mq    converge.MQ
	clock converge.Clock
	queue string
}

func (t Task[T]) NewProducer(s converge.Scope) (*Producer[T], error) {
	if err := t.check(); err != nil {
		return nil, fmt.Errorf("worker: NewProducer: %w", err)
	}
	if s.MQ == nil {
		return nil, fmt.Errorf("worker: task %q: NewProducer needs Scope.MQ", t.name)
	}
	clock := s.Clock
	if clock == nil {
		clock = converge.SystemClock()
	}
	return &Producer[T]{task: t, mq: s.MQ, clock: clock, queue: t.QueueName(s.Namespace)}, nil
}

func (p *Producer[T]) Queue() string {
	if p == nil {
		return ""
	}
	return p.queue
}

func (p *Producer[T]) Enqueue(ctx context.Context, payload T, o EnqueueOpts) error {
	if p == nil || p.mq == nil {
		return errors.New("worker: producer has no MQ; build it with Task.NewProducer")
	}
	if o.Delay < 0 {
		return fmt.Errorf("worker: task %q: Delay must not be negative", p.task.name)
	}
	raw, err := p.task.codec.Marshal(payload)
	if err != nil {
		return fmt.Errorf("worker: task %q: encode: %w", p.task.name, err)
	}
	m, err := seedMessage(p.task.name, p.task.version, p.clock.Now(), o.Headers, raw)
	if err != nil {
		return fmt.Errorf("worker: task %q: %w", p.task.name, err)
	}
	if o.Delay == 0 {
		return p.mq.Publish(ctx, p.queue, m)
	}
	dp, ok := p.mq.(converge.DelayedPublisher)
	if !ok {
		return fmt.Errorf("worker: task %q: Delay needs the DelayedPublisher capability", p.task.name)
	}
	return dp.PublishDelayed(ctx, p.queue, m, o.Delay)
}
