package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/notice"
)

type Notifier struct {
	job           Job
	mq            converge.MQ
	notifications string
}

func (j Job) NewProducer(s converge.Scope) (*Notifier, error) {
	if err := j.check(); err != nil {
		return nil, fmt.Errorf("reconcile: NewProducer: %w", err)
	}
	if s.MQ == nil {
		return nil, fmt.Errorf("reconcile: job %q: NewProducer needs Scope.MQ", j.name)
	}
	return &Notifier{job: j, mq: s.MQ, notifications: j.NotificationsName(s.Namespace)}, nil
}

func (n *Notifier) usable() error {
	if n == nil || n.mq == nil {
		return errors.New("reconcile: notifier has no MQ; build it with Job.NewProducer")
	}
	return nil
}

func (n *Notifier) Notifications() string {
	if n == nil {
		return ""
	}
	return n.notifications
}

func (n *Notifier) Notify(ctx context.Context, id ID) error {
	if err := n.usable(); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("reconcile: job %q: Notify needs an id; NotifyAll addresses the whole job", n.job.name)
	}
	payload, err := notice.Encode(string(id))
	if err != nil {
		return fmt.Errorf("reconcile: job %q: notify: %w", n.job.name, err)
	}
	return n.mq.Publish(ctx, n.notifications, converge.Message{Kind: notice.Kind, Payload: payload})
}

func (n *Notifier) NotifyAll(ctx context.Context) error {
	if err := n.usable(); err != nil {
		return err
	}
	payload, err := notice.EncodeAll()
	if err != nil {
		return fmt.Errorf("reconcile: job %q: notify all: %w", n.job.name, err)
	}
	return n.mq.Publish(ctx, n.notifications, converge.Message{Kind: notice.Kind, Payload: payload})
}
