# Worker reference

Package `github.com/GareArc/converge/worker`. The edge-triggered surface: the
message *is* the work. It is delivered at least once, retried under a policy
you declare, and set aside on a durable shelf when it can no longer be tried.

[Chapter 4 of the guide](../guide/04-worker.md) teaches the model. This page
is the surface, its defaults, and its refusals.

- [Task](#task)
- [Enqueue](#enqueue)
- [Handle](#handle)
- [RetryPolicy](#retrypolicy)
- [Outcomes: Snooze, Discard, Shelve](#outcomes-snooze-discard-shelve)
- [Meta](#meta)
- [The shelf](#the-shelf)
- [Run modes and durability](#run-modes-and-durability)
- [What one delivery costs](#what-one-delivery-costs)

## Task

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}

type TaskOpts struct {
    Codec   Codec
    Version int
}

type Task[T any] struct { /* sealed */ }

func NewTask[T any](name string, o TaskOpts) Task[T]

func (t Task[T]) Name() string
func (t Task[T]) Encode(v any) ([]byte, error)
```

A task is the typed contract both sides import: the name, the payload type,
and the schema version. Declare it once, in a package the producer and the
consumer share, and the compiler catches drift.

| Field | Zero value means |
| --- | --- |
| `Codec` | JSON (`encoding/json`) |
| `Version` | 1 — you cannot declare version zero |

`NewTask` **returns a value, not an error.** An invalid task carries its
error and reports it the first time you use it, from `Enqueue` (`worker:
Enqueue: ...`), `Handle` (`worker: Handle: ...`) or `Encode`. Three things
are invalid:

- an empty name — `worker: task name is required`
- a name containing `/` — `worker: task %q: name must not contain "/"`
- a negative `Version` — `worker: task %q: Version must not be negative`

That shape exists so a task can be a package-level `var` without an `init`
that panics. Nothing silently succeeds: every path that uses the task
surfaces the error.

`Encode` is the task's own codec applied to a value, and is what
`convergetest.Harness.AssertEnqueued` uses to compare payloads. `Task[T]`
satisfies `convergetest.TaskRef` through `Name` and `Encode`.

## Enqueue

```go
type EnqueueOpts struct {
    Delay   time.Duration
    Headers map[string]string
}

func (t Task[T]) Enqueue(ctx context.Context, p *converge.Producer,
    payload T, o EnqueueOpts) error
```

The whole producer-side surface for worker work. It needs a `*converge.Producer`
built with `converge.NewProducer` and the same `Namespace` the consuming
runtime uses — that plus the task name is the entire coupling between the two
binaries.

- **`Delay`** holds the message back before anyone can pick it up. It needs an
  `MQ` with the `DelayedPublisher` capability; without one the error is
  `converge: job %q: Delay needs the DelayedPublisher capability`. A negative
  `Delay` is `worker: task %q: Delay must not be negative`. Zero publishes
  immediately. Reach for minutes, not days: a due date belongs in a column of
  yours, swept by a reconcile job.
- **`Headers`** are yours to attach and reach the handler through
  [`MetaFromContext`](#meta). Any name beginning `converge.` is refused —
  `worker: task %q: header %q uses the reserved %q prefix` — rather than
  quietly overwritten. Your map is copied; converge does not mutate it.

`Enqueue` seeds the envelope: a fresh message ID, the task's schema version,
the enqueue time from the producer's clock, and an attempt base of zero.

A `Producer.Notify` aimed at a worker job is not an error and not ignored: it
publishes a message with no `converge.schema-version` header, which the worker
shelves with the reason `schema version`. Visible, not silent.

## Handle

```go
type HandleOpts struct {
    Concurrency int
    RunMode     converge.RunMode
    Retry       RetryPolicy
    Timeout     time.Duration
    RateLimit   converge.Rate
    Middleware  []converge.Middleware
    Until       converge.StopCondition
}

func Handle[T any](rt *converge.Runtime, t Task[T],
    fn func(ctx context.Context, payload T) error, o HandleOpts) error
```

| Field | Zero value means | Notes |
| --- | --- | --- |
| `Concurrency` | 4 (`DefaultConcurrency`) | In-flight messages on **this replica**. Never negative. |
| `RunMode` | `converge.Competing` | See [run modes](#run-modes-and-durability). |
| `Retry` | the four defaults below | Per-field: a zero field takes its default, so you can set only `MaxAttempts`. |
| `Timeout` | no limit, and a 5-minute redelivery window | Never negative. |
| `RateLimit` | unlimited | Both `Events` and `Per` must be set together. |
| `Middleware` | none | Runs inside `Options.Middleware`. Copied at registration. |
| `Until` | the job never ends | Needs `Options.KV`. |

Registration-time errors, beyond the task's own and the runtime's three:

| Error | Cause |
| --- | --- |
| `worker: task %q: handler fn is required` | nil `fn` |
| `worker: task %q: Concurrency must not be negative` | negative `Concurrency` |
| `worker: task %q: Timeout must not be negative` | negative `Timeout` |
| `worker: task %q: Retry values must not be negative` | any negative `Retry` field |
| `worker: task %q: RateLimit must not be negative` | a negative `Events` or `Per` |
| `worker: task %q: RateLimit needs both Events and Per` | one of the two set, the other zero |
| `worker: task %q: Retry.MinBackoff must not exceed Retry.MaxBackoff` | checked *after* defaults are filled in |
| `worker: task %q: OnAllReplicas cannot use Retry` | `OnAllReplicas` with any non-zero `Retry` field |

And at `Run`, once the runtime's wiring is visible:

- `worker: job %q: needs Options.MQ`
- `worker: job %q: Competing needs the GroupConsumer capability`
- `worker: job %q: OnAllReplicas needs the BroadcastConsumer capability`
- `worker: job %q: OnOneReplica needs Options.Lease`
- `worker: job %q: shelving needs Options.KV`
- `worker: job %q: Snooze needs the DelayedPublisher capability`
- `worker: job %q: Until needs Options.KV`

The fifth and sixth are asked of every job that is **not** `OnAllReplicas`,
whether or not it ever snoozes or shelves, because both are part of what
makes a durable worker job durable. The seventh is asked of any job that sets
`Until`, broadcast included — a self-destruct needs somewhere to record that
it fired.

**`Timeout` does two jobs here.** It is the time limit for one run, as on
every surface — and it is also what the engine derives the transport's
redelivery window from, so a cancelled run is not handed to somebody else
before the cancellation has been noticed. The window is `Timeout + 1 minute`,
or five minutes flat if `Timeout` is unset, and while a run is in flight the
engine extends it every third of that window. The run context reports no
`Deadline()` — the limit is measured on `Options.Clock` — but `ctx.Err()` is
`context.DeadlineExceeded` when it fires.

**`RateLimit`** is a token bucket over the whole job on this replica, applied
just before the handler is entered. It is not per-payload, per-customer or
cluster-wide. The bucket starts full, so an idle job may burst up to `Events`.

## RetryPolicy

```go
type RetryPolicy struct {
    MaxAttempts int
    MinBackoff  time.Duration
    MaxBackoff  time.Duration
    MaxAge      time.Duration
}

const (
    DefaultConcurrency = 4
    DefaultMaxAttempts = 25
    DefaultMinBackoff  = time.Second
    DefaultMaxBackoff  = 15 * time.Minute
    DefaultMaxAge      = 24 * time.Hour
)
```

| Field | Default | Meaning |
| --- | --- | --- |
| `MaxAttempts` | `DefaultMaxAttempts`, 25 | Logical attempts before the message is shelved with `max attempts`. |
| `MinBackoff` | `DefaultMinBackoff`, 1s | The first wait between attempts. |
| `MaxBackoff` | `DefaultMaxBackoff`, 15m | The ceiling that wait grows to. |
| `MaxAge` | `DefaultMaxAge`, 24h | How old a message may get, measured from its enqueue time, before it is shelved with `max age`. |

Each field defaults independently, so `RetryPolicy{MaxAttempts: 5}` keeps the
other three defaults. That is also why `OnAllReplicas` is refused with *any*
non-zero field: the check is on what you wrote, before defaults are applied.

The backoff between attempts is exponential from `MinBackoff` to `MaxBackoff`,
with every delay jittered into `[base/2, base]` so a thousand messages that
failed together do not come back in lockstep. Retries go back through the
transport as `Delivery.Nack(ctx, delay)` — they do **not** use
`DelayedPublisher`.

`MaxAttempts` is measured against the **logical attempt**, the number your
handler sees. `MaxAge` is measured against the envelope's enqueue time, which
survives every retry and every snooze but is restamped by a requeue off the
shelf.

Both are checked **before** the handler runs, so a message that arrives
already over budget is shelved without being tried. `MaxAttempts` is checked
again after a failure: with `MaxAttempts: 5` the fifth attempt runs, and if it
fails the message is shelved rather than redelivered. `MaxAge` is checked
again only when a handler snoozes — an ordinary failure past `MaxAge` is
redelivered, and it is the next delivery's pre-run check that shelves it.

## Outcomes: Snooze, Discard, Shelve

```go
type Snooze struct {
    In time.Duration
}

type Discard struct {
    Reason string
}

type Shelve struct {
    Reason string
}
```

Three values a handler returns *instead of* an error. Each implements
`error`, so they travel through middleware and `errors.Is`/`errors.As`
unchanged, and each is recognised by identity rather than by message. All
three have an unexported zero-size field, so they must be written with field
names — `worker.Snooze{In: d}`, never `worker.Snooze{d}`.

| Return | Outcome | What happens |
| --- | --- | --- |
| `Snooze{In: d}` | `Deferred` | The delivery is acknowledged and the message republished after `d`, with the logical attempt folded back so it does not move. Costs no retries. |
| `Discard{Reason: s}` | `Discarded` | Acknowledged and forgotten, deliberately. Nothing is kept. |
| `Shelve{Reason: s}` | `Shelved` | Stopped now and written to the shelf under your own reason string. |

`Discard` is a *success* as far as the job's counters are concerned: it clears
`ConsecutiveFails` and stamps `LastSuccess`. That is deliberate — a message
that never needed doing is not a fault — but it is worth knowing before you
alert on `ConsecutiveFails`.

**Snooze has bounds, and they are not `MaxAttempts`.**

- A snooze is bounded by `MaxAge`. If the message is already past it, the
  snooze shelves the message with reason `max age` instead. If the remaining
  age is shorter than the delay you asked for, the delay is shortened to it.
- Your delay is floored at 250ms, jittered up to about 375ms, so
  `Snooze{In: 0}` is not a spin loop.
- After the tenth snooze of a message, converge stops honouring your delay
  and substitutes its own, from the `MinBackoff`–`MaxBackoff` curve, stepping
  further along it with each further snooze. The substitution starts at
  `MinBackoff` — 1s by default — whatever you asked for, so this caps a
  handler that snoozes forever but does **not** always slow one down: a
  message asking for 30s comes back sooner on its eleventh snooze, and takes
  about six more before the curve overtakes it.
- If the delayed republish fails, the engine falls back to
  `Delivery.Nack(ctx, delay)` and still reports `Deferred`.

Returning a **reconcile** control value (`reconcile.CheckAgain`) from a worker
handler is not reinterpreted: the message is shelved with reason
`wrong surface`.

## Meta

```go
type Meta struct {
    Task        string
    Queue       string
    MessageID   string
    Attempt     int
    MaxAttempts int
    EnqueuedAt  time.Time
    Headers     map[string]string
}

func MetaFromContext(ctx context.Context) (Meta, bool)
```

The decoded envelope, available to a handler through the context it is given.
`ok` is false in any context that did not come from a worker run — including a
reconcile function's, which is the honest answer rather than a zero `Meta`.

| Field | Value |
| --- | --- |
| `Task` | the job name, which is the task name |
| `Queue` | the job's inbox, as converge named it |
| `MessageID` | the identity minted at enqueue; unchanged across every retry, snooze and requeue |
| `Attempt` | the **logical attempt**, starting at 1 |
| `MaxAttempts` | the effective policy value, defaults already applied |
| `EnqueuedAt` | when the message was first enqueued |
| `Headers` | a copy of every header on the message, `converge.*` included |

`MessageID` is the one value that follows a piece of work end to end, and it
is what appears as `RunCompleted.ID` and as `id=` in the log line. A message
that arrives with no `converge.message-id` header — one some other system
published straight onto the inbox — is given a stable synthetic id derived
from its kind and payload, prefixed `anon-`.

`Attempt` and `Delivery.Attempt()` are different numbers on purpose. The
transport's count restarts at 1 whenever a message is republished as a fresh
one; the logical attempt survives that because it lives in the envelope. When
they disagree, the logical attempt is the one that means anything.

Mutating `Meta.Headers` affects nothing: it is a copy, and it is not what gets
written back to the message.

## The shelf

```go
type ShelvedMessage struct {
    Task       string            `json:"task"`
    Queue      string            `json:"queue"`
    MessageID  string            `json:"message_id"`
    Attempt    int               `json:"attempt"`
    Reason     string            `json:"reason"`
    Error      string            `json:"error,omitempty"`
    EnqueuedAt time.Time         `json:"enqueued_at"`
    ShelvedAt  time.Time         `json:"shelved_at"`
    Headers    map[string]string `json:"headers,omitempty"`
    Payload    []byte            `json:"payload,omitempty"`
}

type Shelf struct { /* ... */ }

var ErrNotShelved = errors.New("worker: not shelved")

func ShelfFrom(rt *converge.Runtime, job string) (*Shelf, error)

func (s *Shelf) List(ctx context.Context) ([]ShelvedMessage, error)
func (s *Shelf) Get(ctx context.Context, messageID string) (ShelvedMessage, error)
func (s *Shelf) Requeue(ctx context.Context, messageID string) error
func (s *Shelf) Purge(ctx context.Context, messageID string) error
func (s *Shelf) PurgeAll(ctx context.Context) error
```

The durable store a message is set aside in when converge will not try it
again. It lives in `Options.KV`, one record per message ID, under a key
namespaced by the job. Nothing leaves it on its own.

There are exactly six ways to arrive:

| `Reason` | Cause |
| --- | --- |
| `max attempts` | the logical attempt reached `Retry.MaxAttempts` |
| `max age` | the message outlived `Retry.MaxAge` |
| `schema version` | the message's `converge.schema-version` did not match the handler's |
| `undecodable` | the payload would not decode, or the envelope's attempt header was unreadable |
| `wrong surface` | the handler returned `reconcile.CheckAgain` |
| *your own string* | the handler returned `Shelve{Reason: ...}` |

`Error` carries the underlying error where there was one and is omitted where
there was not — a handler-requested `Shelve` records the reason and no error.

**`ShelfFrom` does not need a running runtime.** It reads the `KV`, `MQ`,
namespace and clock the `Runtime` was built with, so a small operator binary
can `converge.New` with the same `Namespace`, `KV` and `MQ`, register nothing,
never call `Run`, and still list and requeue. It fails with `worker: ShelfFrom
needs a job name` on an empty name, and every verb fails with `worker: job %q:
Shelf needs Options.KV` if the runtime had no `KV`.

| Verb | Behaviour |
| --- | --- |
| `List` | every record for the job, deduplicated across scan pages. Order is the `KV`'s scan order, not chronological. A record that will not unmarshal is still returned, with `Reason` set to `undecodable-record` and the parse error in `Error`. |
| `Get` | one record. `ErrNotShelved` when there is no such message ID. |
| `Requeue` | republish and delete. See below. |
| `Purge` | delete one record. Deleting an absent record **succeeds** — absence is not an error. |
| `PurgeAll` | delete every record for the job, page by page. |

**`Requeue`** republishes the message to the queue named in the record, then
deletes the record. What survives and what resets:

- The **message ID survives**, so the same value still ties the whole story
  together in your logs.
- The **logical attempt resets to zero** and the **snooze count is cleared**.
- The **enqueue time is restamped to now**, so `MaxAge` starts over.
- Your own headers survive; the schema version survives with them, so a
  message shelved for `schema version` will be shelved again unless the
  handler's version has caught up.

It returns `ErrNotShelved` for an unknown message ID, and `worker: job %q:
requeue %q: needs Options.MQ` if the runtime had no `MQ`. One failure mode is
worth planning for: if the republish succeeds but the delete does not, the
error says `republished but record not purged` — the message is live *and*
still on the shelf, so purge the record rather than requeueing again.

Fix the cause first, then requeue. A requeue starts the retry budget over, so
requeueing into a still-broken dependency just spends it again.

## Run modes and durability

`OnAllReplicas` makes a worker job **non-durable**, and that single fact
explains everything that is different about it:

| | Durable (`Competing`, `OnOneReplica`) | `OnAllReplicas` |
| --- | --- | --- |
| Retry policy | yours, or the defaults | **refused at registration** |
| A failed run | `Retrying`, redelivered after backoff | `Discarded` |
| `Snooze` | republished after the delay | `Discarded` |
| Shelf | required (`Options.KV`) | none; the guards are not even checked |
| `Backlog` | reported when the backend can | never known |
| Schema version mismatch | shelved | **not checked** — the payload is handed to your codec as-is |

Every replica gets its own copy of every message and acknowledges it for
itself, so there is no redelivery to wait for and nothing durable to set
aside. Use it for cache warming and per-process state, not for work that must
not be lost.

## What one delivery costs

The order in which a durable delivery is judged, before your handler is
entered: schema version, then a readable attempt header, then `MaxAge`, then
`MaxAttempts`. Any of them failing shelves the message without running it.

Then the rate limit, then your handler under `Timeout`. Afterwards:

| Handler returned | Outcome |
| --- | --- |
| `nil` | `Succeeded`; acknowledged |
| a decode failure (before your handler is reached) | `Shelved`, reason `undecodable` |
| `Snooze` / `Discard` / `Shelve` | as [above](#outcomes-snooze-discard-shelve) |
| `reconcile.CheckAgain` | `Shelved`, reason `wrong surface` |
| an ordinary error, budget left | `Retrying`; nacked with the backoff delay |
| an ordinary error, budget spent | `Shelved`, reason `max attempts` |
| a panic | recovered into `worker: job %q: handler panic: %v`, then treated as an ordinary error |

Two paths produce no `RunCompleted` at all. A run that returns an error after
the engine cancelled its context — shutdown, lost lease, stop condition — and
a delivery that arrived while the engine was already stopping are both
returned **neutrally**. On a durable job that means the message is republished
with its logical attempt folded back so it has not spent an attempt, and the
delivery is acknowledged; if that republish fails, or the attempt header was
unreadable, the message is nacked for immediate redelivery instead. On an
`OnAllReplicas` job it means nothing at all happens, which is the same thing:
the copy was this replica's and there is nothing to hand back.

If writing the shelf record fails, the message is **not** dropped: the
delivery is nacked for another attempt and the outcome is reported as
`Retrying` with the write error attached.
