package converge

import (
	"context"
	"errors"
	"fmt"

	"github.com/GareArc/converge/internal/keys"
	"github.com/GareArc/converge/internal/notice"
)

type ProducerOpts struct {
	Namespace string
	Clock     Clock
}

type Producer struct {
	mq        MQ
	clock     Clock
	namespace string
}

func NewProducer(mq MQ, o ProducerOpts) (*Producer, error) {
	if mq == nil {
		return nil, errors.New("converge: NewProducer needs an MQ")
	}
	p := &Producer{mq: mq, clock: o.Clock, namespace: o.Namespace}
	if p.clock == nil {
		p.clock = systemClock{}
	}
	return p, nil
}

func (p *Producer) usable() error {
	if p == nil || p.mq == nil {
		return errors.New("converge: producer has no MQ; build it with converge.NewProducer")
	}
	return nil
}

func (p *Producer) Notify(ctx context.Context, job, id string) error {
	if err := p.usable(); err != nil {
		return err
	}
	if job == "" {
		return errors.New("converge: Notify needs a job name")
	}
	var payload []byte
	var err error
	if id == "" {
		payload, err = notice.EncodeAll()
	} else {
		payload, err = notice.Encode(id)
	}
	if err != nil {
		return fmt.Errorf("converge: notify %q: %w", job, err)
	}
	return p.mq.Publish(ctx, keys.Inbox(p.namespace, job), Message{Kind: notice.Kind, Payload: payload})
}
