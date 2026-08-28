package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type EnqueueOpts struct {
	Delay   time.Duration
	Headers map[string]string
}

func (t Task[T]) Enqueue(ctx context.Context, p *converge.Producer, payload T, o EnqueueOpts) error {
	if t.err != nil {
		return fmt.Errorf("worker: Enqueue: %w", t.err)
	}
	if p == nil {
		return fmt.Errorf("worker: task %q: Enqueue needs a Producer", t.name)
	}
	if o.Delay < 0 {
		return fmt.Errorf("worker: task %q: Delay must not be negative", t.name)
	}
	now, ok := hook.ProducerNow(p)
	if !ok {
		return fmt.Errorf("worker: task %q: Enqueue needs a Producer built with converge.NewProducer", t.name)
	}
	raw, err := t.codec.Marshal(payload)
	if err != nil {
		return fmt.Errorf("worker: task %q: encode: %w", t.name, err)
	}
	m, err := seedMessage(t.name, t.version, now, o.Headers, raw)
	if err != nil {
		return fmt.Errorf("worker: task %q: %w", t.name, err)
	}
	return hook.ProducerSend(p, ctx, t.name, m, o.Delay)
}
