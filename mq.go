package converge

import (
	"context"
	"time"
)

type MQ interface {
	Publish(ctx context.Context, queue string, m Message) error
	Consume(ctx context.Context, queue string, deliver func(Delivery)) error
}

type Delivery interface {
	Message() Message
	Attempt() int
	EnqueuedAt() time.Time
	Ack(ctx context.Context) error
	Nack(ctx context.Context, redeliverAfter time.Duration) error
	Extend(ctx context.Context, visibility time.Duration) error
}

type GroupConsumer interface {
	ConsumeGroup(ctx context.Context, queue, group string, deliver func(Delivery)) error
}

type BroadcastConsumer interface {
	ConsumeBroadcast(ctx context.Context, queue string, deliver func(Delivery)) error
}

type DelayedPublisher interface {
	PublishDelayed(ctx context.Context, queue string, m Message, delay time.Duration) error
}

type BacklogReporter interface {
	Backlog(ctx context.Context, queue string) (int, error)
}
