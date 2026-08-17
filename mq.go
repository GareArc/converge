package converge

import (
	"context"
	"time"
)

type MQ interface {
	Publish(ctx context.Context, queue string, m Message) error
	// Consume joins the queue's implicit competing-consumer group: each
	// message is delivered to exactly one active consumer at a time,
	// including messages published before any consumer existed. deliver is
	// called synchronously; concurrency is the caller's job. Blocks until
	// ctx is canceled, then returns ctx.Err().
	Consume(ctx context.Context, queue string, deliver func(Delivery)) error
}

type Delivery interface {
	Message() Message
	Attempt() int // 1 on first delivery
	EnqueuedAt() time.Time
	Ack(ctx context.Context) error
	// Nack schedules redelivery no sooner than redeliverAfter from now.
	Nack(ctx context.Context, redeliverAfter time.Duration) error
	// Extend moves this delivery's visibility deadline to now+visibility.
	// Errors if the delivery is no longer in flight.
	Extend(ctx context.Context, visibility time.Duration) error
}

// Capability interfaces, checked at registration (guide §6). An MQ that
// lacks a required capability fails the registration that needs it.

type GroupConsumer interface {
	// ConsumeGroup is Consume with named groups: each group competes
	// internally and receives every message on the queue, including
	// messages published before the group was first created.
	ConsumeGroup(ctx context.Context, queue, group string, deliver func(Delivery)) error
}

type BroadcastConsumer interface {
	// ConsumeBroadcast delivers every message published after subscribing to
	// every subscriber. Deliveries are fire-and-forget: Attempt is always 1;
	// Ack and Nack are no-ops.
	ConsumeBroadcast(ctx context.Context, queue string, deliver func(Delivery)) error
}

type DelayedPublisher interface {
	// PublishDelayed affects group consumption only; broadcast is for hints,
	// which are never delayed.
	PublishDelayed(ctx context.Context, queue string, m Message, delay time.Duration) error
}
