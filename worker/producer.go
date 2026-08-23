package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/wiring"
)

type Producer struct {
	mq      converge.MQ
	clock   converge.Clock
	queueMQ func(queue string) converge.MQ
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func NewProducer(mq converge.MQ) (*Producer, error) {
	if mq == nil {
		return nil, errors.New("worker: NewProducer needs an MQ")
	}
	return &Producer{mq: mq, clock: wallClock{}}, nil
}

func ProducerFrom(rt *converge.Runtime) (*Producer, error) {
	w, err := wiring.ProducerFor(rt)
	if err != nil {
		return nil, err
	}
	p := &Producer{mq: w.MQ, clock: wallClock{}, queueMQ: w.QueueMQ}
	if w.Clock != nil {
		p.clock = w.Clock
	}
	return p, nil
}

func resolveQueue(queueMQ func(string) converge.MQ, def converge.MQ, queue string) (converge.MQ, error) {
	if queueMQ != nil {
		if m := queueMQ(queue); m != nil {
			return m, nil
		}
	}
	if def != nil {
		return def, nil
	}
	return nil, fmt.Errorf("worker: queue %q: no handler binding and no default Options.MQ", queue)
}

type EnqueueOpts struct {
	Delay   time.Duration
	Headers map[string]string
}

func (t Task[T]) Enqueue(ctx context.Context, p *Producer, payload T, o EnqueueOpts) error {
	if t.err != nil {
		return fmt.Errorf("worker: Enqueue: %w", t.err)
	}
	if p == nil {
		return fmt.Errorf("worker: task %q: Enqueue needs a Producer", t.name)
	}
	if o.Delay < 0 {
		return fmt.Errorf("worker: task %q: Delay must not be negative", t.name)
	}
	mq, err := resolveQueue(p.queueMQ, p.mq, t.queue)
	if err != nil {
		return err
	}
	raw, err := t.codec.Marshal(payload)
	if err != nil {
		return fmt.Errorf("worker: task %q: encode: %w", t.name, err)
	}
	m, err := seedMessage(t.name, t.version, p.clock.Now(), o.Headers, raw)
	if err != nil {
		return fmt.Errorf("worker: task %q: %w", t.name, err)
	}
	if o.Delay > 0 {
		dp, ok := mq.(converge.DelayedPublisher)
		if !ok {
			return fmt.Errorf("worker: task %q: Delay needs the DelayedPublisher capability", t.name)
		}
		return dp.PublishDelayed(ctx, t.queue, m, o.Delay)
	}
	return mq.Publish(ctx, t.queue, m)
}
