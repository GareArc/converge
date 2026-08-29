# Kernel reference

Package `github.com/GareArc/converge`. The kernel owns the runtime, the four
ports, the value types both surfaces share, and the observer protocol. It
knows nothing about reconcile or worker jobs — those register themselves
through a sealed seam.

This page answers *what does this do and where are its edges*. For why you
would want any of it, start with [the guide](../guide/index.md); for the
vocabulary, the [glossary](../glossary.md).

- [Options and New](#options-and-new)
- [Runtime](#runtime)
- [Scope](#scope)
- [Message and the envelope headers](#message-and-the-envelope-headers)
- [Ports](#ports)
- [Capability interfaces](#capability-interfaces)
- [RunMode and Surface](#runmode-and-surface)
- [Stop conditions and state](#stop-conditions-and-state)
- [Rate](#rate)
- [Middleware](#middleware)
- [Observer, events and outcomes](#observer-events-and-outcomes)
- [LogObserver](#logobserver)
- [Stats types](#stats-types)

## Options and New

```go
type Options struct {
    Namespace    string
    MQ           MQ
    Lease        Lease
    KV           KV
    Observer     Observer
    Middleware   []Middleware
    Clock        Clock
    LeaseTTL     time.Duration
    DrainTimeout time.Duration
}

func New(o Options) (*Runtime, error)
```

| Field | Zero value means | Notes |
| --- | --- | --- |
| `Namespace` | no namespace segment at all | Every key and queue name the library builds is prefixed with it. Producers must use the same string to address the same job. |
| `MQ` | nil — no transport | Not checked by `New`. A job that needs one fails when `Run` starts it. |
| `Lease` | nil — no leases | Same: only `OnOneReplica` jobs need it, and they fail at `Run`. |
| `KV` | nil — no durable state | Needed for schedule bookkeeping, tombstones and the worker shelf. |
| `Observer` | a no-op observer | Never nil inside the runtime. |
| `Middleware` | no middleware | Copied at `New`; later mutation of your slice has no effect. Runs outermost, before any per-job middleware. |
| `Clock` | the wall clock | Every semantic wait in the library goes through it. |
| `LeaseTTL` | 30s | Also sets the derived heartbeat and polling interval, `LeaseTTL/3`. |
| `DrainTimeout` | 30s | How long in-flight runs get after the context is cancelled, before they are cancelled too. |

`New` returns an error only for `LeaseTTL < 0` or `DrainTimeout < 0`
(`converge: durations in Options must not be negative`). It does not
validate the ports: a missing port is diagnosed by the job that needs it,
when `Run` starts it, with the job's name in the message.

Every `Runtime` mints a random replica ID at construction. It is used
wherever one running copy of a service has to be told apart from another;
nothing you configure feeds it.

converge has no configuration machinery of its own. Every tunable is a plain
struct field, and no package in the library reads a file or an environment
variable. Plumbing them is your composition root's job: wire *quantities* —
periods, budgets, concurrency, timeouts — from whatever configuration system
the service already has, and keep *semantics* — queue names, run modes,
trigger wiring — in code, where the compiler can see them.

## Runtime

```go
type Runtime struct { /* ... */ }

func (rt *Runtime) Run(ctx context.Context) error
func (rt *Runtime) Ready() <-chan struct{}
func (rt *Runtime) Stats() []JobStats
```

Jobs are not registered through the `Runtime` directly. `reconcile.Register`,
`reconcile.Periodic` and `worker.Handle` register through an unexported
seam, and they surface the runtime's three registration errors:

- `converge: job name must not be empty`
- `converge: duplicate job name %q`
- `converge: register %q: runtime already running`

**`Run`** freezes the job set, probes `KV`, then runs every job in its own
goroutine until `ctx` is cancelled.

- The probe is a single `KV.Get` of a namespaced probe key. If it fails and
  `ctx` is still live, `Run` returns `converge: KV is unreachable: ...`
  before any job starts. A nil `KV` skips the probe.
- With no jobs registered, `Run` blocks until `ctx` is done and returns nil.
- It returns nil on clean shutdown. `context.Canceled` from a job is
  tolerated; any other job error is collected, cancels the others, and comes
  back joined via `errors.Join`. **A non-nil return is always a real
  failure.**
- Calling it twice returns `converge: Run called twice`. The second call does
  not start anything.

**`Ready`** returns a channel closed once every registered job has signalled
that it started. It stays open until `Run` is called, and closes immediately
after that if no jobs are registered. Readiness means *this replica has
started the job*, not *this replica is doing the work*: an `OnOneReplica` job
signals it before the lease race resolves, so a replica that is only waiting
for the lease still reports ready. That is the behaviour a readiness probe
wants — the replica is live and will pick the job up if the holder dies.

**`Stats`** returns one `JobStats` per registered job, in registration order,
and works before `Run` (every job reports `NotStarted`). It takes no context
and never touches the network: the fields that need a round trip are
polled in the background — see [the operations
page](operations.md#how-stale-a-number-can-be).

### JobDeps

```go
type JobDeps struct {
    MQ           MQ
    Lease        Lease
    KV           KV
    Observer     Observer
    Clock        Clock
    Namespace    string
    LeaseTTL     time.Duration
    DrainTimeout time.Duration
    Middleware   []Middleware
}
```

What the runtime hands each job when `Run` starts it. It is exported because
the seam between the kernel and the surface engines needs one exported type;
you neither build one nor receive one.

## Scope

```go
type Scope struct {
    MQ        MQ
    Namespace string
    Clock     Clock
}

func (rt *Runtime) Scope() Scope
```

The three things every producer needs, held once per process. It is a
struct with **no methods**: there is nothing you can do with a `Scope` except
hand it to `worker.Task.NewProducer` or `reconcile.Job.NewProducer`, so a
namespace-wide "send anything anywhere" object cannot exist by accident.

`rt.Scope()` is the in-process convenience; a binary that sends but runs no
jobs builds one by hand with the same `MQ` backend and the same `Namespace`
as the consuming runtime. `Clock` may be nil, in which case producers stamp
the wall clock; the runtime's own scope always carries the clock it was
built with.

## Message and the envelope headers

```go
type Message struct {
    Kind    string
    Headers map[string]string
    Payload []byte
}

const HeaderPrefix = "converge."

const (
    HeaderSchemaVersion = HeaderPrefix + "schema-version"
    HeaderEnqueuedAt    = HeaderPrefix + "enqueued-at"
    HeaderMessageID     = HeaderPrefix + "message-id"
    HeaderAttempt       = HeaderPrefix + "attempt"
    HeaderSnoozes       = HeaderPrefix + "snoozes"
)
```

`Message` is the only thing that crosses the `MQ` port. Its zero value is a
legal message with no kind, no headers and no payload.

The five header names are the worker envelope. The library owns every name
beginning `converge.`: `worker.Producer.Enqueue` refuses a caller header that
starts with the prefix rather than overwriting it, and folds these five
forward itself on every republish. You should not need to read them —
`worker.MetaFromContext` gives you the decoded values — but they are exported
because they appear on the wire and in whatever your backend shows you.

`Kind` on a worker message is the task name. On a notification it is the
constant `converge.notification`, not the job name; the job is identified by
the channel the message was published to.

## Ports

Four interfaces. Three of them — `MQ`, `Lease` and `KV` — are satisfied by
[`inmem`](adapters.md#inmem) and by [`convredis`](adapters.md#convredis);
`Clock` has no adapter, because the default is the wall clock and the fake is
[`convergetest.Clock`](adapters.md#convergetest). What an implementation must
do is pinned by the [`portcheck` suites](adapters.md#portcheck), not by these
signatures.

```go
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
```

`Consume` blocks until `ctx` is done and calls `deliver` once per delivery.
`Delivery.Attempt` is the **transport delivery** count, starting at 1 and
climbing with every `Nack` — not the logical attempt the handler is shown.
`Nack(ctx, 0)` means redeliver as soon as possible. `Extend` postpones
redelivery of a delivery still in flight; calling it after the delivery has
been settled is allowed to fail, and on `convredis` it returns
`convredis.ErrSettled`.

```go
type Lease interface {
    TryAcquire(ctx context.Context, name string, ttl time.Duration) (LeaseHandle, bool, error)
}

type LeaseHandle interface {
    Extend(ctx context.Context, ttl time.Duration) error
    Release(ctx context.Context) error
    Done() <-chan struct{}
}
```

`TryAcquire` returns `(nil, false, nil)` when the lease is held elsewhere —
that is not an error. `Done` closes when the handle is known to be lost or
released; an engine watching it stops the job's work when it fires. A lease
is an efficiency device, never a correctness device.

```go
type KV interface {
    Get(ctx context.Context, key string) (val []byte, ok bool, err error)
    SetCAS(ctx context.Context, key string, old, new []byte) (bool, error)
    Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
    Delete(ctx context.Context, key string) error
    Scan(ctx context.Context, prefix, cursor string) (keys []string, next string, err error)
}
```

Absence is not an error: `Get` reports it through `ok`, and deleting a key
that is not there succeeds. `SetCAS` with `old == nil` means *create only if
absent*; a successful `SetCAS` clears any TTL the key had. `Scan` starts with
an empty cursor and is finished when it returns an empty `next`. It visits
every matching key **at least** once and never strays outside the prefix; it
may repeat a key, because Redis `SCAN` guarantees no better and an adapter
cannot promise what its store will not. Deduplicate if repeats would matter
to you — converge's own readers do. `Set` with `ttl == 0` stores the
key with no expiry.

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}
```

Every semantic wait in the library goes through `Clock`, which is what makes
tests able to move time by hand. `After` with a non-positive duration fires
immediately.

## Capability interfaces

Optional interfaces an `MQ` may also satisfy. Engines check for them with a
type assertion **when the job starts**, not when the first message arrives,
so a missing capability is a startup failure naming the job and the
capability — never a 3am surprise.

```go
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

type GroupBacklogReporter interface {
    BacklogForGroup(ctx context.Context, queue, group string) (int, error)
}

var ErrBacklogUnknown = errors.New("converge: backlog: unknown")
```

| Capability | Who demands it |
| --- | --- |
| `GroupConsumer` | a `Competing` worker job |
| `BroadcastConsumer` | any `OnAllReplicas` job that reads a queue — a broadcast worker job, or a reconcile job with a notifications trigger |
| `DelayedPublisher` | every durable worker job (it is how `Snooze` republishes), and `EnqueueOpts.Delay` on the sending side |
| `BacklogReporter` | reporting `JobStats.Backlog` for an `OnOneReplica` job on either surface |
| `GroupBacklogReporter` | reporting `JobStats.Backlog` for a `Competing` worker job |

`DelayedPublisher` is **not** what ordinary retries use. A failed worker run
goes back through `Delivery.Nack(ctx, delay)`; only `Snooze` and a
producer-side `EnqueueOpts.Delay` publish a delayed message.

A backlog reporter that cannot answer for a particular queue and group
returns `ErrBacklogUnknown` rather than a number. The engine then leaves
`BacklogKnown` false — it does not fall back to zero.

## RunMode and Surface

```go
type RunMode struct { /* sealed */ }

var (
    OnOneReplica  = RunMode{...}
    OnAllReplicas = RunMode{...}
    Competing     = RunMode{...}
)

func (m RunMode) IsZero() bool
func (m RunMode) String() string
```

A sealed value type: you can only use the three named values, and no fourth
can be constructed from outside the package. The zero value is *unset*, which
each surface resolves to its own default — `OnOneReplica` for reconcile,
`Competing` for worker. `String` on the zero value is `"unset"`.

Two registration-time refusals turn on this value:

- `Competing` on a reconcile job — `reconcile: job %q: Competing is a worker
  mode`.
- `OnAllReplicas` together with any non-zero `worker.RetryPolicy` — `worker:
  task %q: OnAllReplicas cannot use Retry`. A broadcast worker job has
  nowhere to retry to; a failed run on one is
  [`Discarded`](worker.md#run-modes-and-durability).

```go
type Surface int

const (
    SurfaceReconcile Surface = iota + 1
    SurfaceWorker
)

func (s Surface) String() string
```

Which of the two models a job runs on. It starts at 1 so that the zero value
is not a valid surface: `Surface(0).String()` is `"unknown"`, not a guess.
You see it on `converge.Run`, `JobStats` and `JobInfo`.

## Stop conditions and state

```go
type StopCondition struct { /* sealed */ }

func Deadline(t time.Time) StopCondition
func StopKey(key string) StopCondition

func (c StopCondition) IsZero() bool
func (c StopCondition) String() string
```

A job's declared reason to be destroyed, set as `Spec.Until` or
`HandleOpts.Until`. The zero value is *no stop condition* and `String()` is
`"none"`; the two constructors render as the word `deadline` followed by an
RFC 3339 time, and the words `stop key` followed by the key.

- `Deadline(t)` — the job is destroyed the first time the engine checks at or
  after `t`, and the engine writes the library's own tombstone key so the
  decision survives restarts.
- `StopKey(key)` — the job is destroyed once `key` exists in `KV`. The string
  is used **exactly as given**: it is not namespaced, not prefixed, and its
  value is irrelevant — presence is the signal. That key *is* the tombstone.

Either form needs `Options.KV`; without one the job fails at `Run` with
`... Until needs Options.KV`. When and how often each engine checks is [on the
operations page](operations.md#destroying-a-job).

```go
type State struct { /* sealed */ }

var (
    NotStarted = State{...}
    Active     = State{...}
    Destroyed  = State{...}
)

func (s State) IsZero() bool
func (s State) String() string
```

The three states a job can be in, reported by `JobStats.State`. `String` on
the zero value is `"unknown"`. `NotStarted` and `Active` are what one replica
says about itself; only `Destroyed` is cluster-wide, and it is terminal.

## Rate

```go
type Rate struct {
    Events int
    Per    time.Duration
}

func (r Rate) IsZero() bool
func (r Rate) String() string
```

A token-bucket ceiling, used as `worker.HandleOpts.RateLimit`. The zero value
means unlimited. A `Rate` with one field set and the other zero is rejected at
registration (`RateLimit needs both Events and Per`), and a negative field is
rejected too — you cannot configure an actual rate of zero. The bucket starts
full, so a job that has been idle may burst up to `Events` before it is
throttled. `String` renders as `50/1s`.

## Middleware

```go
type Run struct {
    Job     string
    Surface Surface
    ID      string
}

type Handler func(ctx context.Context, run Run) error

type Middleware func(next Handler) Handler
```

`Run.ID` is the reconcile ID on a reconcile job — empty for a `SingleID` job
— and the **message ID** on a worker job.

Middleware from `Options.Middleware` runs outermost, in slice order, followed
by the job's own `Middleware`, then your function. A middleware sees whatever
the function returned, including the control-flow values
(`reconcile.CheckAgain`, `worker.Snooze`, `worker.Discard`, `worker.Shelve`);
returning them unchanged is what keeps them working.

One constraint: pass the `ctx` you were given, or a context derived from it.
Both surfaces carry per-run values in it — the worker's payload among them —
and a middleware that substitutes a fresh `context.Background()` breaks the
handler underneath it.

A panic inside the chain, or inside your function, is recovered by the engine
and converted into an ordinary error, so it fails one run rather than the
process.

## Observer, events and outcomes

```go
type Observer interface {
    Observe(e Event)
}

type Event interface{ event() }
```

`Event` is a sealed interface: the five concrete events below are the only
implementations, and a `switch` over them is exhaustive today. Handle the
default case anyway — a later version may add a sixth, and an observer that
panics or blocks does so on the engine's goroutine. `Observe` is called
concurrently from multiple jobs; implementations must be safe for that.

```go
type RunCompleted struct {
    Job      string
    ID       string
    Attempt  int
    Duration time.Duration
    Outcome  Outcome
    Err      error
}

type LeaseChanged struct {
    Job  string
    Held bool
}

type ScheduleOverrun struct {
    Job  string
    Due  time.Time
    Late time.Duration
}

type NotificationDropped struct {
    Job string
    ID  string
    Err error
}

type JobDestroyed struct {
    Job   string
    Cause StopCondition
}
```

| Event | Emitted when |
| --- | --- |
| `RunCompleted` | every run of your function that the engine settles. `ID` is the reconcile ID or the message ID; `Attempt` is the logical attempt; `Err` is nil for `Succeeded` and `Deferred`. |
| `LeaseChanged` | an `OnOneReplica` job takes or gives up its lease. Emitted only by jobs that take one. |
| `ScheduleOverrun` | a scheduled sweep boundary passed while the previous sweep was still running. One event per missed boundary, with how late it is. |
| `NotificationDropped` | a notification never reached the queue of pending IDs. |
| `JobDestroyed` | a job's stop condition fired. Emitted once per replica, with the cause. |

A run that ends because the engine is shutting down is **not** reported: the
work is neutrally returned to where it came from rather than counted as an
ending. That is why a clean shutdown produces no final flurry of failures.

```go
var (
    ErrNotificationUndecodable = errors.New("converge: notification: undecodable")
    ErrNotificationEmptyID     = errors.New("converge: notification: empty id")
    ErrInboxOverflow           = errors.New("converge: notification: inbox overflow")
)
```

`NotificationDropped.Err` is one of these three. On `ErrNotificationUndecodable`
the `ID` field is empty — the engine could not read one. `ErrInboxOverflow`
means the job's in-memory queue of pending IDs was at its bound (65536
distinct IDs) when a new one arrived; the schedule still covers that ID on the
next sweep.

```go
type Outcome struct { /* sealed */ }

var (
    Succeeded = Outcome{...}
    Retrying  = Outcome{...}
    Deferred  = Outcome{...}
    Discarded = Outcome{...}
    Shelved   = Outcome{...}
)

func (o Outcome) String() string
```

How one run ended. `String` on the zero value is `"unknown"`.

| Outcome | Reconcile | Worker |
| --- | --- | --- |
| `Succeeded` | the function returned nil | the handler returned nil, **or** returned `Discard` |
| `Retrying` | the function returned an error; the ID is in backoff | the handler failed and the message will be redelivered |
| `Deferred` | `CheckAgain` or `ErrOutdated`; `Err` is nil | `Snooze`; `Err` is nil |
| `Discarded` | never | `Discard`, or any failure on a broadcast job |
| `Shelved` | never | the message went to the shelf |

A reconcile job never reports `Discarded` or `Shelved`: it has no shelf, and a
worker control value returned from a reconcile function is an ordinary failure.

## LogObserver

```go
func LogObserver(l *slog.Logger) Observer
```

Maps the five events onto `slog` records. `LogObserver(nil)` returns the
no-op observer rather than panicking.

| Event | Level |
| --- | --- |
| `RunCompleted` | by outcome: `Succeeded` and `Discarded` info, `Retrying` and `Deferred` warn, `Shelved` error, anything else warn |
| `LeaseChanged` | info |
| `ScheduleOverrun` | warn |
| `NotificationDropped` | warn |
| `JobDestroyed` | info |

Attributes carry the event's fields under the names `job`, `id`, `attempt`,
`duration`, `outcome`, `err`, `held`, `due`, `late` and `cause`. A nil error
contributes no `err` attribute at all. Records are emitted with
`context.Background()`, so a `slog.Handler` that reads values off the context
will not see the run's context here.

## Stats types

```go
type JobStats struct {
    Job              string
    Surface          Surface
    RunMode          RunMode
    State            State
    LeaseHeld        bool
    InFlight         int
    Backlog          int
    BacklogKnown     bool
    BacklogAt        time.Time
    Failing          int
    Shelved          int
    ShelvedKnown     bool
    ShelvedAt        time.Time
    LastSuccess      time.Time
    LastError        error
    LastErrorAt      time.Time
    ConsecutiveFails int
}

type FailingID struct {
    ID       string
    Failures int
    Err      error
    NextTry  time.Time
}

type JobInfo struct {
    Job      string
    Surface  Surface
    RunMode  RunMode
    Queue    string
    Settings map[string]string
}
```

`Backlog` and `Shelved` are the two numbers converge will refuse to invent.
Each travels with a `*Known` flag and a `*At` timestamp: **false means
unknown, not zero**, and the timestamp dates the reading. What each field
means in practice, and how stale it can be, is [on the operations
page](operations.md#reading-jobstats).

`FailingID` describes one reconcile ID currently serving out failure backoff:
how many consecutive failures, the last error, and when it will next run. It
is reconcile-only — the worker engine counts the messages it is retrying but
does not keep them.

`JobInfo` is the static half: what a job was registered as. `Queue` is the
job's inbox on a worker job and empty on a reconcile job. `Settings` is a
rendered, human-readable map — `concurrency`, `retry`, `schema-version`,
`timeout` and `rate-limit` for a worker job; `concurrency`, `schedule`,
`triggers` and `versions` for a reconcile one. It is for reading, not for
parsing. `JobInfo` reaches you through
[`debughttp.ReadOnlyHandler`](operations.md#readonlyhandler).
