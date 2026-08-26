package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/notice"
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

type NotificationsOpts struct {
	ID func(payload []byte) (ID, error)
	MQ converge.MQ
}

type notificationTrigger struct {
	queue   string
	foreign bool
	opts    NotificationsOpts

	mq        converge.MQ
	broadcast bool
}

func Notifications(o NotificationsOpts) Trigger {
	return &notificationTrigger{opts: o}
}

func NotificationsFrom(queue string, o NotificationsOpts) Trigger {
	return &notificationTrigger{queue: queue, foreign: true, opts: o}
}

func (t *notificationTrigger) Run(ctx context.Context, wake func(ID)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (t *notificationTrigger) decode(payload []byte) (ID, error) {
	if t.foreign {
		return t.opts.ID(payload)
	}
	id, err := notice.Decode(payload)
	if err != nil {
		return "", err
	}
	return ID(id), nil
}

func (t *notificationTrigger) bind(e *engine) error {
	if !t.foreign {
		e.mu.Lock()
		t.queue = keys.Inbox(e.deps.Namespace, e.cfg.name)
		e.mu.Unlock()
	}
	t.mq = t.opts.MQ
	if t.mq == nil {
		t.mq = e.deps.MQ
	}
	if t.mq == nil {
		if t.foreign {
			return fmt.Errorf("reconcile: job %q: NotificationsFrom(%q) needs an MQ", e.cfg.name, t.queue)
		}
		return fmt.Errorf("reconcile: job %q: Notifications needs Options.MQ", e.cfg.name)
	}
	if e.cfg.runMode == converge.OnAllReplicas {
		if _, ok := t.mq.(converge.BroadcastConsumer); !ok {
			return fmt.Errorf("reconcile: job %q: notifications from %q need the BroadcastConsumer capability", e.cfg.name, t.queue)
		}
		t.broadcast = true
		return nil
	}
	if _, ok := t.mq.(converge.GroupConsumer); !ok {
		return fmt.Errorf("reconcile: job %q: notifications from %q need the GroupConsumer capability", e.cfg.name, t.queue)
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
		if t.broadcast {
			t.mq.(converge.BroadcastConsumer).ConsumeBroadcast(ctx, t.queue, deliver)
		} else {
			t.mq.(converge.GroupConsumer).ConsumeGroup(ctx, t.queue, e.key("notifications"), deliver)
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
	id, err := t.decode(d.Message().Payload)
	if err != nil {
		e.deps.Observer.Observe(converge.NotificationDropped{Job: e.cfg.name, Err: converge.ErrNotificationUndecodable})
	} else {
		e.notifyVia(ctx, e.wakeQueueRef(), id, wakeNotify)
	}
	d.Ack(ctx)
}
