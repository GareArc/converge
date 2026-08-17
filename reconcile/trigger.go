package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/GareArc/converge"
)

type Trigger interface {
	Run(ctx context.Context, wake func(ID)) error
}

type PeriodicTrigger interface {
	Trigger
	NextAfter(t time.Time) time.Time
}

const (
	triggerRestartMin = time.Second
	triggerRestartMax = time.Minute
)

type OnMessageOpts struct {
	MQ       converge.MQ
	Delivery converge.DeliveryMode
}

type messageTrigger struct {
	queue string
	idf   IDFunc
	opts  OnMessageOpts

	mq       converge.MQ
	delivery converge.DeliveryMode
}

func OnMessage(queue string, id IDFunc, o OnMessageOpts) Trigger {
	return &messageTrigger{queue: queue, idf: id, opts: o}
}

func (t *messageTrigger) Run(ctx context.Context, wake func(ID)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (t *messageTrigger) bind(e *engine) error {
	t.mq = t.opts.MQ
	if t.mq == nil {
		t.mq = e.deps.MQ
	}
	if t.mq == nil {
		return fmt.Errorf("reconcile: job %q: OnMessage(%q) needs an MQ", e.cfg.name, t.queue)
	}
	t.delivery = t.opts.Delivery
	if t.delivery.IsZero() {
		if e.cfg.runMode == converge.OnAllReplicas {
			t.delivery = converge.Broadcast
		} else {
			t.delivery = converge.Group
		}
	}
	switch t.delivery {
	case converge.Group:
		if _, ok := t.mq.(converge.GroupConsumer); !ok {
			return fmt.Errorf("reconcile: job %q: OnMessage(%q) with Group delivery needs the GroupConsumer capability", e.cfg.name, t.queue)
		}
	case converge.Broadcast:
		if _, ok := t.mq.(converge.BroadcastConsumer); !ok {
			return fmt.Errorf("reconcile: job %q: OnMessage(%q) with Broadcast delivery needs the BroadcastConsumer capability", e.cfg.name, t.queue)
		}
	}
	return nil
}

func (e *engine) runTrigger(ctx context.Context, idx int, t Trigger) {
	switch tr := t.(type) {
	case *messageTrigger:
		e.runMessages(ctx, tr)
		return
	}
	backoff := triggerRestartMin
	for {
		t.Run(ctx, e.hint)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-e.deps.Clock.After(jitter(backoff)):
		}
		backoff = min(backoff*2, triggerRestartMax)
	}
}

func (e *engine) runMessages(ctx context.Context, t *messageTrigger) {
	backoff := triggerRestartMin
	deliver := func(d converge.Delivery) {
		e.deliverHint(ctx, t, d)
	}
	for {
		if t.delivery == converge.Broadcast {
			t.mq.(converge.BroadcastConsumer).ConsumeBroadcast(ctx, t.queue, deliver)
		} else {
			t.mq.(converge.GroupConsumer).ConsumeGroup(ctx, t.queue, e.key("hints"), deliver)
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-e.deps.Clock.After(jitter(backoff)):
		}
		backoff = min(backoff*2, triggerRestartMax)
	}
}

func (e *engine) deliverHint(ctx context.Context, t *messageTrigger, d converge.Delivery) {
	id, err := t.idf(d.Message().Payload)
	if err != nil {
		e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, Reason: converge.DiscardUndecodable})
	} else {
		e.hint(id)
	}
	d.Ack(ctx)
}
