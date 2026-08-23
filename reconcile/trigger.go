package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
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

var triggerRestartCurve = backoff.Curve{Min: triggerRestartMin, Max: triggerRestartMax}

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
	case *scheduleTrigger:
		e.runSchedule(ctx, idx, tr)
		return
	case *messageTrigger:
		e.runMessages(ctx, tr)
		return
	}
	e.supervise(ctx, func() { t.Run(ctx, func(id ID) { e.hint(ctx, id) }) })
}

func (e *engine) runMessages(ctx context.Context, t *messageTrigger) {
	deliver := func(d converge.Delivery) {
		e.deliverHint(ctx, t, d)
	}
	e.supervise(ctx, func() {
		if t.delivery == converge.Broadcast {
			t.mq.(converge.BroadcastConsumer).ConsumeBroadcast(ctx, t.queue, deliver)
		} else {
			t.mq.(converge.GroupConsumer).ConsumeGroup(ctx, t.queue, e.key("hints"), deliver)
		}
	})
}

func (e *engine) supervise(ctx context.Context, run func()) {
	attempt := 0
	for {
		start := e.deps.Clock.Now()
		run()
		if ctx.Err() != nil {
			return
		}
		if e.deps.Clock.Now().Sub(start) >= triggerRestartMax {
			attempt = 0
		}
		attempt++
		select {
		case <-ctx.Done():
			return
		case <-e.deps.Clock.After(triggerRestartCurve.Delay(attempt)):
		}
	}
}

func (e *engine) deliverHint(ctx context.Context, t *messageTrigger, d converge.Delivery) {
	id, err := t.idf(d.Message().Payload)
	if err != nil {
		e.deps.Observer.Observe(converge.WakeDiscarded{Job: e.cfg.name, Reason: converge.DiscardUndecodable})
	} else {
		e.hint(ctx, id)
	}
	d.Ack(ctx)
}
