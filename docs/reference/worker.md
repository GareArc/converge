# `converge/worker` — the edge-triggered model

```go
func NewTask[T any](name string, o TaskOpts) Task[T]

type TaskOpts struct {
    Queue   string // default: task name
    Codec   Codec  // default: JSON
    Version int    // payload schema version, carried in headers; default 1
}

func (t Task[T]) Enqueue(ctx context.Context, p *Producer, payload T, o EnqueueOpts) error

type EnqueueOpts struct {
    Delay   time.Duration     // needs the DelayedPublisher capability
    Headers map[string]string // user headers; "converge." prefix reserved
}

func NewProducer(mq converge.MQ) (*Producer, error)
func ProducerFrom(rt *converge.Runtime) (*Producer, error) // handler bindings, then default MQ

func Handle[T any](rt *converge.Runtime, t Task[T],
    fn func(ctx context.Context, payload T) error, o HandleOpts) error

type HandleOpts struct {
    Concurrency int              // default 4
    RunMode     converge.RunMode // default SplitAcrossReplicas
    Retry       RetryPolicy
    Visibility  time.Duration    // default 5m; auto-extended while the handler runs
    MQ          converge.MQ      // this queue's transport; default: runtime default
    RateLimit   converge.Rate    // process-local
    Middleware  []converge.Middleware
    Paused      bool
}

type RetryPolicy struct {
    MaxAttempts int           // default 25; handler runs at most this many times
    MinBackoff  time.Duration // default 1s
    MaxBackoff  time.Duration // default 15m; jitter always applied
    MaxAge      time.Duration // default 24h; total time-in-system cap (the snooze ceiling)
}

// per-execution metadata — via context, never via signature (evolvable)
func MetaFromContext(ctx context.Context) (Meta, bool)
type Meta struct {
    Task, Queue, MessageID string
    Attempt, MaxAttempts   int
    EnqueuedAt             time.Time
    Headers                map[string]string
}

// outcome values — implement error; keyed fields required
type Snooze struct {
    In time.Duration
}
type Discard struct {
    Reason string
}
```

`Meta.EnqueuedAt` reflects the message's `converge.enqueued-at` header, not
necessarily the moment your code first called `Enqueue` — see
[Adapters → EnqueuedAt divergence](adapters.md#enqueuedat-divergence) for
how the shipped Redis adapter stamps it on a delayed message.

The dead-letter queue (list/get/requeue/purge) is exposed through
`debughttp.OpsHandler`, not through this package directly — see
[Operations](../guide/operations.md). Its listing is backed by `KV.Scan`,
which is at-least-once under concurrent mutation on the Redis adapter (see
[Adapters → KV](adapters.md#kv--newkv)); the DLQ list dedups by key so a
duplicate-returning `Scan` can never surface a duplicate dead-letter record.

See [Worker](../guide/worker.md) for the outcome table, logical-attempt vs.
transport-delivery distinction, and `OnAllReplicas` semantics.
