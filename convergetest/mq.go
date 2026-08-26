package convergetest

import (
	"bytes"
	"context"
	"maps"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
)

var (
	_ converge.MQ                = (*MQ)(nil)
	_ converge.GroupConsumer     = (*MQ)(nil)
	_ converge.BroadcastConsumer = (*MQ)(nil)
	_ converge.DelayedPublisher  = (*MQ)(nil)
	_ converge.BacklogReporter   = (*MQ)(nil)
)

type MQ struct {
	base *inmem.MQ
	mu   sync.Mutex
	rec  map[string][]converge.Message
	fail error
}

func WrapMQ(base *inmem.MQ) *MQ {
	return &MQ{base: base, rec: map[string][]converge.Message{}}
}

func (m *MQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	if err := m.takeFailure(); err != nil {
		return err
	}
	if err := m.base.Publish(ctx, queue, msg); err != nil {
		return err
	}
	m.record(queue, msg)
	return nil
}

func (m *MQ) PublishDelayed(ctx context.Context, queue string, msg converge.Message, delay time.Duration) error {
	if err := m.takeFailure(); err != nil {
		return err
	}
	if err := m.base.PublishDelayed(ctx, queue, msg, delay); err != nil {
		return err
	}
	m.record(queue, msg)
	return nil
}

func (m *MQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return m.base.Consume(ctx, queue, deliver)
}

func (m *MQ) ConsumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	return m.base.ConsumeGroup(ctx, queue, group, deliver)
}

func (m *MQ) ConsumeBroadcast(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return m.base.ConsumeBroadcast(ctx, queue, deliver)
}

func (m *MQ) Backlog(ctx context.Context, queue string) (int, error) {
	return m.base.Backlog(ctx, queue)
}

func (m *MQ) FailNextPublish(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = err
}

func (m *MQ) Published(queue string) []converge.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.rec[queue]
	out := make([]converge.Message, len(msgs))
	for i, msg := range msgs {
		out[i] = cloneMessage(msg)
	}
	return out
}

func (m *MQ) Idle() bool {
	return m.base.Idle()
}

func (m *MQ) takeFailure() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.fail
	m.fail = nil
	return err
}

func (m *MQ) record(queue string, msg converge.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rec[queue] = append(m.rec[queue], cloneMessage(msg))
}

func cloneMessage(m converge.Message) converge.Message {
	m.Headers = maps.Clone(m.Headers)
	m.Payload = bytes.Clone(m.Payload)
	return m
}
