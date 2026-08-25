# `converge` — the kernel

**Zero-value rule, stated once:** any unset option field takes its
documented default. You cannot configure an actual zero (e.g. `Concurrency:
0` means the default, not zero workers); the rare "unlimited/disabled" cases
use explicit sentinels noted per field.

```go
func New(o Options) (*Runtime, error)

type Options struct {
    Namespace    string        // prefixes all leases, KV keys, engine queues; default ""
    MQ           MQ            // default transport (optional until a job needs it)
    Lease        Lease         // required by OnOneReplica
    KV           KV            // engine state: last-fire, dead-letter marks, cursors, Tracker
    Observer     Observer      // typed domain events; nil = no-op
    Middleware   []Middleware  // wraps every run, both surfaces, outermost first
    Clock        Clock         // nil = wall clock
    LeaseTTL     time.Duration // default 30s; heartbeat at TTL/3
    DrainTimeout time.Duration // default 30s
}

func (rt *Runtime) Run(ctx context.Context) error    // blocking; nil on clean shutdown
func (rt *Runtime) Ready() <-chan struct{}           // consumers + triggers live
func (rt *Runtime) Poke(job, id string) error        // wake one ID (bypasses backoff, revives parked)
func (rt *Runtime) Stats() []JobStats                // pull API: staleness, queue depth, per job

// run modes — plain values (no parameters in v1)
var OnOneReplica RunMode        // lease per job
var SplitAcrossReplicas RunMode // worker surface only in v1; reconcile → registration error
var OnAllReplicas RunMode

// message delivery mode for hint queues (see the reconcile guide's
// "Triggers and the schedule" section); zero value follows the run mode
var Group DeliveryMode     // competing consumers — each message wakes one replica
var Broadcast DeliveryMode // every replica sees every message

// process-local token bucket (Spec.RateLimit, HandleOpts.RateLimit);
// zero value = unlimited
type Rate struct {
    Events int           // allowed events per window (also the burst size)
    Per    time.Duration
}

// middleware — the platform seam: tracing, logging, timeouts, panic enrichment
type Middleware func(next Handler) Handler
type Handler func(ctx context.Context, run Run) error
type Run struct {           // normalized view of one execution
    Job     string
    Surface Surface         // SurfaceReconcile | SurfaceWorker
    ID      string          // reconcile ID or worker message ID
}
```

Control-flow signals (`reconcile.CheckAgain`, `reconcile.ErrOutdated`,
`worker.Snooze`, `worker.Discard`) implement an unexported kernel interface —
only converge's own types can be signals; wrong-surface returns park/DLQ with
a `WrongSurfaceSignal` event.

## Ports

Implement these to add a backend:

```go
type Message struct {
    Kind    string            // task name for worker messages; "" for reconcile hints
    Headers map[string]string // engine reserves the "converge." prefix
                              // (schema version, enqueue time, trace context)
    Payload []byte
}

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

// MQ capability interfaces, checked at registration:
//   GroupConsumer     — required by SplitAcrossReplicas
//   BroadcastConsumer — required by Delivery: Broadcast
//   DelayedPublisher  — required by EnqueueOpts.Delay and Snooze
// The MQ port targets per-message-ack brokers (Redis Streams, NATS
// JetStream, SQS, RabbitMQ). Kafka's offset model does not fit this port
// and is NOT a planned MQ adapter; Kafka integration would arrive as a
// stream-source trigger seam instead.

type Lease interface {
    TryAcquire(ctx context.Context, name string, ttl time.Duration) (LeaseHandle, bool, error)
}
type LeaseHandle interface {
    Extend(ctx context.Context, ttl time.Duration) error
    Release(ctx context.Context) error
    Done() <-chan struct{} // closed on loss → engine cancels in-flight ctx
                           // (a neutral outcome — see the reconcile/worker guides)
}

type KV interface {
    // Get: ok=false when the key is absent — absence is not an error.
    Get(ctx context.Context, key string) (val []byte, ok bool, err error)
    // SetCAS: old == nil means "create only if absent".
    SetCAS(ctx context.Context, key string, old, new []byte) (bool, error)
    Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
    Delete(ctx context.Context, key string) error
    Scan(ctx context.Context, prefix, cursor string) (keys []string, next string, err error)
}

type Observer interface {
    Observe(e Event) // RunCompleted, IDParked, LeaseTransition, QueueDepth,
}                    // WakeDiscarded, PassOverrun, WrongSurfaceSignal, MessageDiscarded, ...
                     // Always handle a default case: event types are added in minor releases.

type Clock interface { Now() time.Time /* ... */ }
```

See [Adapters](adapters.md) for the shipped implementations of these ports,
and [Operations reference](operations.md#introspection-and-ops-handlers) for
the introspection handlers (`debughttp.ReadOnlyHandler`,
`debughttp.OpsHandler`) built on top of them.
