package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
	"github.com/GareArc/converge/internal/notice"
)

type Trigger interface {
	Run(ctx context.Context, notify func(ID)) error
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

type notificationTrigger struct {
	source  string
	foreign bool
	id      func(payload []byte) (ID, error)

	mq        converge.MQ
	broadcast bool
}

func Notifications() Trigger {
	return &notificationTrigger{}
}

func NotificationsFrom(source string, mq converge.MQ, id func(payload []byte) (ID, error)) Trigger {
	return &notificationTrigger{source: source, foreign: true, mq: mq, id: id}
}

func (t *notificationTrigger) Run(ctx context.Context, notify func(ID)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (t *notificationTrigger) decode(payload []byte) (notice.Notification, error) {
	if t.foreign {
		id, err := t.id(payload)
		return notice.Notification{ID: string(id)}, err
	}
	return notice.Decode(payload)
}

func (t *notificationTrigger) bind(e *engine) error {
	if !t.foreign {
		e.mu.Lock()
		t.source = e.cfg.job.NotificationsName(e.deps.Namespace)
		e.mu.Unlock()
	}
	if t.mq == nil {
		t.mq = e.deps.MQ
	}
	if t.mq == nil {
		if t.foreign {
			return fmt.Errorf("reconcile: job %q: NotificationsFrom(%q) needs an MQ", e.cfg.job.Name(), t.source)
		}
		return fmt.Errorf("reconcile: job %q: Notifications needs Options.MQ", e.cfg.job.Name())
	}
	switch e.cfg.runMode {
	case converge.OnAllReplicas:
		if _, ok := t.mq.(converge.BroadcastConsumer); !ok {
			return fmt.Errorf("reconcile: job %q: notifications from %q need the BroadcastConsumer capability", e.cfg.job.Name(), t.source)
		}
		t.broadcast = true
	case converge.OnOneReplica:
	default:
		if _, ok := t.mq.(converge.GroupConsumer); !ok {
			return fmt.Errorf("reconcile: job %q: notifications from %q need the GroupConsumer capability", e.cfg.job.Name(), t.source)
		}
	}
	return nil
}

func (e *engine) runTrigger(ctx context.Context, idx int, t Trigger) {
	switch tr := t.(type) {
	case *scheduleTrigger:
		e.runSchedule(ctx, idx, tr)
		return
	case *notificationTrigger:
		e.runNotifications(ctx, tr)
		return
	}
	e.supervise(ctx, func() { t.Run(ctx, func(id ID) { e.notify(ctx, id) }) })
}

func (e *engine) runNotifications(ctx context.Context, t *notificationTrigger) {
	deliver := func(d converge.Delivery) {
		e.deliverNotification(ctx, t, d)
	}
	e.supervise(ctx, func() {
		switch e.cfg.runMode {
		case converge.OnAllReplicas:
			t.mq.(converge.BroadcastConsumer).ConsumeBroadcast(ctx, t.source, deliver)
		case converge.OnOneReplica:
			t.mq.Consume(ctx, t.source, deliver)
		default:
			t.mq.(converge.GroupConsumer).ConsumeGroup(ctx, t.source, e.key("notifications"), deliver)
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

func (e *engine) deliverNotification(ctx context.Context, t *notificationTrigger, d converge.Delivery) {
	n, err := t.decode(d.Message().Payload)
	switch {
	case errors.Is(err, Skip):
		e.deps.Observer.Observe(converge.NotificationSkipped{Job: e.cfg.job.Name()})
	case err != nil:
		e.deps.Observer.Observe(converge.NotificationDropped{Job: e.cfg.job.Name(), Err: converge.ErrNotificationUndecodable})
	case n.All:
		e.notifyAll(ctx)
	default:
		e.notifyVia(ctx, e.idQueueRef(), ID(n.ID), causeNotification)
	}
	d.Ack(ctx)
}
