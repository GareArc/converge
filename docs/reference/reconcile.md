# Reconcile reference

Package `github.com/GareArc/converge/reconcile`. The level-triggered surface:
you name a source of IDs and a cadence, converge visits every ID on that
cadence, and your function re-reads the truth and makes the world match it.

The [guide's chapters 2 and 3](../guide/02-ids.md) teach the model. This page
is the surface, its defaults, and its refusals.

- [Job](#job)
- [Notifier](#notifier)
- [Spec and Register](#spec-and-register)
- [Periodic](#periodic)
- [Triggers](#triggers)
- [Cadences](#cadences)
- [ID sources](#id-sources)
- [ID and ToIDs](#id-and-toids)
- [ID functions](#id-functions)
- [Versions](#versions)
- [CheckAgain and ErrOutdated](#checkagain-and-erroutdated)
- [What a run costs when it fails](#what-a-run-costs-when-it-fails)

## Job

```go
type JobOpts struct {
    Notifications string
}

type Job struct { /* sealed */ }

func NewJob(name string, o JobOpts) Job

func (j Job) Name() string
func (j Job) NotificationsName(namespace string) string
```

A job is a value: the name every other piece of code refers to it by, and
the name of the channel its notifications arrive on. Declare it once, in a
package the registering binary and any notifying binary both import, and
hand the same value to `Spec.Job` and to `NewProducer`.

| Field | Zero value means |
| --- | --- |
| `Notifications` | derived: `<namespace>/converge/notifications/<name>` |

`Notifications` is used **verbatim** when set — not namespaced, not
prefixed — so a producer in another language can `XADD` to that exact key.
`NotificationsName` returns whichever resolves; print it at startup for the
team that cannot import your package. Like a task's queue, a declared name
is refused only for what is invisible — leading or trailing whitespace, or a
control character, with the offending byte named — and any other string is
the backend's business.

`NewJob` **returns a value, not an error.** An invalid job carries its error
and reports it from `Register` and from `NewProducer`. Two things are
invalid: an empty name (`reconcile: job name is required`) and a name
containing `/` (`reconcile: job %q: name must not contain "/"`). A zero
`Job` handed to `Spec` is `reconcile: Spec.Job is required; build one with
NewJob`.

## Notifier

```go
type Notifier struct { /* sealed */ }

func (j Job) NewProducer(s converge.Scope) (*Notifier, error)
func (n *Notifier) Notify(ctx context.Context, id ID) error
func (n *Notifier) NotifyAll(ctx context.Context) error
func (n *Notifier) Notifications() string
```

The sending side of a reconcile job, built from the job value so it can
address nothing else. It needs a [`converge.Scope`](kernel.md#scope) —
`rt.Scope()` in the process that runs the job, a struct literal with the
same `MQ` and `Namespace` anywhere else.

- `NewProducer` fails on a misconstructed job (`reconcile: NewProducer:
  ...`) and on a nil `Scope.MQ` (`reconcile: job %q: NewProducer needs
  Scope.MQ`).
- `Notify` publishes `{"id":"<id>"}` to the job's channel. An empty `id` is
  refused: `reconcile: job %q: Notify needs an id; NotifyAll addresses the
  whole job`.
- `NotifyAll` publishes `{"all":true}`. On a `SingleID` job that runs the
  job's one ID as a notification would; on any other job it starts a sweep
  of every `Schedule` trigger now, without moving the cadence. It exists
  for the producer that just changed many IDs at once — a bulk import, a
  migration — and would otherwise have to notify each.
- `Notifications` is the resolved channel name, declared or derived.

A nil or zero-value `Notifier` returns `reconcile: notifier has no MQ; build
it with Job.NewProducer` from every verb rather than panicking. Both payload
forms, and what a producer in another language writes, are in the
[wire reference](wire.md).

## Spec and Register

```go
type Spec struct {
    Job         Job
    Reconcile   func(ctx context.Context, id ID) error
    Triggers    []Trigger
    Concurrency int
    RunMode     converge.RunMode
    Timeout     time.Duration
    Versions    VersionSource
    Middleware  []converge.Middleware
    Until       converge.StopCondition
}

func Register(rt *converge.Runtime, s Spec) error
```

| Field | Zero value means | Notes |
| --- | --- | --- |
| `Job` | invalid | Required. Build it with [`NewJob`](#job). Its name is the name in every log line, metric and key. |
| `Reconcile` | invalid | Required. Called once per pending ID. Must be safe to run twice. |
| `Triggers` | invalid | Must contain at least one `Schedule`. |
| `Concurrency` | 1 | How many IDs this **one replica** may run at once. Never negative. |
| `RunMode` | `converge.OnOneReplica` | `Competing` is refused. |
| `Timeout` | no limit | The time limit for one run; when it elapses the run's context is cancelled. Never negative. |
| `Versions` | no version checking | See [Versions](#versions). |
| `Middleware` | none | Runs inside `Options.Middleware`. Copied at registration. |
| `Until` | the job never ends | Needs `Options.KV`. |

`Register` validates and returns before `Run`; the errors it can produce are
the whole list of things a reconcile job can get wrong at declaration time:

| Error | Cause |
| --- | --- |
| `reconcile: Spec.Job is required; build one with NewJob` | zero `Job` |
| `reconcile: job name is required` / `reconcile: job %q: name must not contain "/"` | carried from `NewJob` |
| `reconcile: job %q: Spec.Reconcile is required` | nil function |
| `reconcile: job %q: Concurrency must not be negative` | negative `Concurrency` |
| `reconcile: job %q: Timeout must not be negative` | negative `Timeout` |
| `reconcile: job %q: Competing is a worker mode` | `RunMode: converge.Competing` |
| `reconcile: job %q: Triggers must not contain nil` | a nil element |
| `reconcile: job %q: no Schedule trigger; every reconcile job needs one` | no `Schedule` in `Triggers` |
| `reconcile: job %q: Schedule needs an IDSource` | `Schedule` built from a zero `IDSource` — including one built from a nil function |
| `reconcile: job %q: Schedule needs a Cadence; use Every or Cron` | zero `Cadence` |
| `reconcile: cron %q: ...` | a `Cadence` that failed to parse; the error is carried from `Cron` and surfaces here |

Plus the runtime's own three: empty name, duplicate name, and registering
after `Run` has started.

Three more failures wait until `Run`, because they are about what the runtime
was wired with rather than what the spec says:

- `reconcile: job %q: OnOneReplica needs Options.Lease`
- `reconcile: job %q: Until needs Options.KV`
- `reconcile: job %q: Notifications needs Options.MQ`, and — under
  `OnAllReplicas` — `notifications from %q need the BroadcastConsumer
  capability`

## Periodic

```go
type PeriodicOpts struct {
    Timeout    time.Duration
    RunMode    converge.RunMode
    Middleware []converge.Middleware
    Until      converge.StopCondition
}

func Periodic(rt *converge.Runtime, name string, c Cadence,
    fn func(ctx context.Context) error, o PeriodicOpts) error
```

Shorthand for the job that has nothing to enumerate: it is exactly
`Register` with `Triggers: []Trigger{Schedule(SingleID(), c)}` and a
`Reconcile` that ignores the ID. A nil `fn` is `reconcile: job %q: Periodic
needs a function`; everything else comes back from `Register`.

`PeriodicOpts` has no `Concurrency` and no `Versions`, because a job with one
ID has nothing to run in parallel and nothing to version.

The ID a `Periodic` job runs under is the empty string. That is what you will
see in `RunCompleted.ID` and in the `id=""` of a log line, and it is what
`Notifier.NotifyAll` addresses.

## Triggers

```go
type Sink interface {
    Notify(id ID)
    Drop(err error)
}

type Trigger interface {
    Run(ctx context.Context, sink Sink) error
}

type PeriodicTrigger interface {
    Trigger
    NextAfter(t time.Time) time.Time
}
```

A trigger is an independent source of pending IDs. All of a job's triggers
feed **one deduplicated queue**, so a sweep and a burst of notifications for
the same ID collapse into one run rather than racing. A trigger only ever
names an ID; it never runs the function and never carries instructions.

`Trigger` is implemented for you twice over — `Schedule` and
`Notifications`. `convredis.ListTrigger` is a third, shipped implementation
rather than a built-in. `PeriodicTrigger` marks the
schedule; a job is refused at registration unless one of its triggers is a
`Schedule`.

You can implement `Trigger` yourself: return when `ctx` is done, and call
`sink.Notify(id)` for every ID you learn about, or `sink.Drop(err)` for
something you received but could not turn into one. A custom trigger that
returns before its context is done is restarted under a bounded backoff (1s
to 1m), so a transient failure does not need handling inside it — but the
error `Run` returns is discarded: nothing logs it, counts it, or reports
the restart. If you want to know your trigger is failing, call `Drop`
yourself before returning, or observe it another way.

An ID passed to `sink.Notify` is treated exactly like a notification from
`Notifications()`: it resets a failing ID's backoff, the same bypass
[chapter 3](../guide/03-notifications.md) describes. A call to
`sink.Drop(err)` is reported as `NotificationDropped` with `err` wrapped
alongside `converge.ErrNotificationUndecodable` — unless `errors.Is(err,
Skip)`, in which case it is a `NotificationSkipped` instead. `Skip` is a
signal for a custom `Trigger`'s own ID function to say an entry is not for
this job; it never arrives through a built-in trigger. A custom trigger is
never swept on a cadence,
whatever it implements. The engine dispatches by concrete type, so `Schedule`
is swept and anything else is simply run. Implementing `PeriodicTrigger` does
not change that, so registration **rejects** a trigger that implements it and
is not a `Schedule`:

```
reconcile: job "x": only Schedule is swept; a custom PeriodicTrigger runs but never sweeps
```

Without that rule such a job would pass the "every reconcile job needs a
`Schedule`" check and then never sweep at all, removing the floor the rest of
this library leans on. Do your own timing inside `Run`, and keep a real
`Schedule` for the guarantee.

```go
func Notifications() Trigger
```

**`Notifications`** reads the job's own channel — `JobOpts.Notifications`
if declared, otherwise `<namespace>/converge/notifications/<name>` — over
`Options.MQ`. It takes nothing, because converge owns the payload format of
its own notifications (`{"id":"..."}` or `{"all":true}`). It is still
explicit: a job with only a schedule is a valid job that should not pay for
a consumer, and the trigger list is the one place a reader learns what runs
a job.

A source some other system writes is not read through `reconcile.Trigger`
built-ins at all — it is read through a custom `Trigger`, and
`convredis.ListTrigger` is the shipped one for a Redis list. See
[the adapters reference](adapters.md#list-trigger) and
[the foreign-queue cookbook](../cookbook/foreign-queue.md).

Every delivery on a notifications trigger is acknowledged, decodable or not.
A payload that will not decode raises `NotificationDropped` with
`converge.ErrNotificationUndecodable` and is gone; there is no retry and no
guessing. A decoded empty ID on a job whose source is not `SingleID` raises
`converge.ErrNotificationEmptyID`. In both cases the schedule still covers
the work.

Under `OnOneReplica` a notifications trigger consumes the queue directly;
under `OnAllReplicas` it consumes it as a broadcast, so every replica sees
every notification.

## Cadences

```go
type CronOpts struct {
    Location *time.Location
}

type Cadence struct { /* sealed */ }

func Every(d time.Duration) Cadence
func Cron(expr string, o CronOpts) Cadence
```

When the schedule fires. A `Cadence` is a value: it carries its own parse
error, which surfaces when you register the job rather than at the call site.

- **`Every(d)`** — every `d`, anchored on the last recorded sweep. `d <= 0`
  is `reconcile: Every needs a positive duration`.
- **`Cron(expr, o)`** — standard **five-field** cron. `CronOpts.Location`
  defaults to `time.UTC`. Descriptors (`@daily`, `@hourly`, ...) are
  deliberately **not supported**: `reconcile: cron %q: descriptors are not
  supported, use five fields`. Any other parse failure comes back as
  `reconcile: cron %q: <parser error>`.

A missed firing has one behaviour and no option. If one or more scheduled
times passed while the job was not running — a restart, a lease move, a first
deploy — the job sweeps **once** on return and then resumes the cadence. It
does not replay the gap.

The last-swept time is kept in `KV`, so it survives restarts and lease moves.
Under `OnAllReplicas`, or with no `KV`, it is kept in memory instead, which is
why a broadcast job sweeps once on every replica's start.

## ID sources

```go
type IDSource struct { /* sealed */ }

func (s IDSource) IsZero() bool

func SingleID() IDSource
func IDs(fn func(ctx context.Context) ([]ID, error)) IDSource
func StringIDs(fn func(ctx context.Context) ([]string, error)) IDSource
func IDsByPage(fn func(ctx context.Context, cursor string) ([]ID, string, error)) IDSource

func Schedule(ids IDSource, c Cadence) PeriodicTrigger
```

| Constructor | Your function returns | Notes |
| --- | --- | --- |
| `SingleID` | nothing | One ID, the empty string. What `Periodic` uses. |
| `IDs` | `[]ID` | The whole list, every sweep. |
| `StringIDs` | `[]string` | `IDs` with the conversion done for you. |
| `IDsByPage` | one page plus the next cursor | For anything unbounded. |

Passing a nil function to `IDs`, `StringIDs` or `IDsByPage` returns the
**zero `IDSource`**, not a panic — and the zero source is refused when you
register the job (`Schedule needs an IDSource`). `IsZero` reports it.

`IDsByPage` is called first with an empty cursor and then with whatever you
returned, until you return an empty one. Its contract:

- **IDs are queued page by page**, as each page arrives, not after the walk
  finishes.
- **The cursor is stored in `KV`** between pages, so a sweep interrupted by a
  restart resumes from where it stopped. It is deleted when the walk
  completes.
- **A page that returns an error is retried with the same cursor**, under a
  bounded backoff (1s to 1m), for as long as the job is active. It is never
  skipped, and the error does not end the sweep.
- Your keyset must be stable enough that paging forward terminates. A cursor
  that never advances is an infinite sweep, and converge cannot tell that
  from a very long one.

`Schedule(ids, c)` is the trigger that walks `ids` once per period of `c`.
Every reconcile job needs one; every other trigger is a latency accelerator.
A job may declare more than one `Schedule` — each keeps its own last-swept
time and its own cursor — and `JobInfo.Settings["schedule"]` renders them
joined with `+`.

## ID and ToIDs

```go
type ID string

func ToIDs(raw ...string) []ID
```

The name of one unit of reconcile work. Any string is a legal ID, including
the empty one — which is reserved for `SingleID` jobs. `ToIDs` is the bulk
conversion `StringIDs` uses internally.

## ID functions

```go
func RawID() func(payload []byte) (ID, error)
func IDFromJSON(field string) func(payload []byte) (ID, error)
```

For the `ID` field of `convredis.ListTriggerOpts`, or any custom `Trigger`
that decodes a foreign payload into a `reconcile.ID`.

- **`RawID()`** takes the whole payload as the ID. An empty payload is
  `reconcile: empty payload`. The whole payload becomes the ID, and an ID
  is not private: it appears in log lines, in `Stats()` and the debug
  handler. Never use `RawID` on a payload that contains a secret.
- **`IDFromJSON(field)`** unmarshals the payload as a JSON object and reads
  one **string** field. It errors — and the notification is dropped and
  reported — if the payload is not a JSON object, if the field is missing, if
  it is not a string, or if it is the empty string. Unknown extra fields are
  ignored.
- **`Skip`** is a value your own ID function returns — `return "",
  reconcile.Skip` — to say *this entry is not for this job*. The element is
  already gone (a `convredis.ListTrigger` pop is destructive) and a
  `NotificationSkipped` event is recorded; nothing is dropped and nothing is
  logged as a fault. Two jobs cannot share one **list** at all — a list pop
  is destructive — and must be fed two lists.

These are conveniences, not the menu. The parameter is an open function,
`func(payload []byte) (reconcile.ID, error)`: a composite ID is

```go
id := func(payload []byte) (reconcile.ID, error) {
    var m struct{ Tenant, Workspace string }
    if err := json.Unmarshal(payload, &m); err != nil {
        return "", err
    }
    return reconcile.ID(m.Tenant + "/" + m.Workspace), nil
}
```

Both return a function, so both are called at spec-construction time:
`convredis.ListTrigger(rdb, "their-list", convredis.ListTriggerOpts{ID:
reconcile.IDFromJSON("workspace_id")})`.

## Versions

```go
type Version uint64

type VersionSource interface {
    Latest(ctx context.Context, id ID) (Version, error)
}
```

A counter on intent that you already keep — typically a column that moves
whenever somebody edits that ID. converge stores no versions of its own and
never writes to your source.

When `Spec.Versions` is set, the engine reads `Latest` **before** the run and
again **after** it. If the value moved, the run does not count as finished:
the ID is queued again, because whatever your function decided was decided
from state that has since changed. The `RunCompleted` outcome is still
`Succeeded` — the function did succeed — but the ID comes back.

Two edges worth knowing:

- **An error from `Latest` disables the check for that run, silently.** The
  pre-run read is treated as "no version known", the post-run read is not
  attempted, and the run settles normally. A `VersionSource` that fails does
  not fail your job; it stops protecting it.
- The re-run is a deferral, not a failure. It does not touch backoff, and it
  is subject to the same anti-hot-loop bound as `CheckAgain` below.

`convergetest/versions` supplies a
[fixed in-memory source](adapters.md#versions) for tests.

## CheckAgain and ErrOutdated

```go
type CheckAgain struct {
    _  struct{}
    In time.Duration
}

var ErrOutdated error
```

Two values a reconcile function returns *instead of* an error, to say the run
did not fail — it just is not done.

- **`CheckAgain{In: d}`** — come back to this ID in `d`. It does not count as
  a failure — it *clears* the ID's consecutive-failure count — and reports
  `Deferred` with a nil `Err`.
- **`ErrOutdated`** — the write was refused because the intent moved. Return
  it after your own conditional write loses the race. Same treatment as
  `CheckAgain{In: 0}`.

`CheckAgain` implements `error` (`reconcile: check again in <d>`), so it
travels through middleware and `errors.Is`/`errors.As` unchanged. Both values
are recognised by identity, not by string.

Two things surprise people:

- **`In: 0` is not "immediately".** Every deferral delay is floored at
  250ms, jittered up to about 375ms, because a zero-delay deferral is a spin
  loop. `ErrOutdated` gets the same floor.
- **Your delay is honoured on ten consecutive deferrals out of every
  eleven.** On the eleventh, converge substitutes a delay from its own
  1s-to-15m curve and starts the ten over; each substitution steps one place
  further along that curve. This is a bound on spinning, not a slow-down: the
  substituted delay begins at 1s whatever you asked for, and the curve is
  fixed — 1s to 15m, with no option to widen it — so it can only ever restore
  a delay up to that ceiling. An ID deferring by a minute is briefly sped up
  and overtaken by the seventh substitution; an ID deferring by an hour is
  shortened to at most 15m and stays there. A success or a failure resets both
  counters.
- **A deferred ID is not protected from being pulled forward.** Any trigger
  — a sweep as much as a notification — moves an ID that is waiting out a
  `CheckAgain` to the front of the queue. Failure backoff is the one that
  only a notification bypasses.

Returning a **worker** control value (`worker.Snooze`, `worker.Discard`,
`worker.Shelve`) from a reconcile function is a plain failure: the ID goes
into failure backoff and `RunCompleted` reports `Retrying` with your value as
the error. It is not silently reinterpreted.

## What a run costs when it fails

Return an ordinary error and that ID becomes a **failing ID**: it waits, and
each consecutive failure lengthens the wait.

- The curve is exponential from **1 second** to a ceiling of **15 minutes**,
  and every delay is jittered into `[base/2, base]`, so a thousand IDs that
  failed together do not come back in lockstep.
- It is a ceiling, not a bench. An ID that has been broken for a week is
  still retried at least four times an hour, forever.
- Other IDs are unaffected. One bad merchant does not stop the other nine
  thousand.
- A **notification resets it**: an ID in failure backoff that is notified runs
  at the next opportunity instead of serving out a penalty computed before the
  notification arrived. That reset is the only bypass in the library, and a
  sweep does not get it — listing an ID again is not new information about it.
- The count and the individual IDs are readable: `JobStats.Failing`, and
  `converge.FailingID` values through
  [`debughttp`](operations.md#one-job).

A run that returns an error *after* the engine cancelled its context — a
shutdown, a lost lease, a stop condition firing — is neither a success nor a
failure: the ID is queued again and no `RunCompleted` is reported at all. That
is why a clean shutdown produces no closing flurry of failures. A panic *is* a
failure: the engine recovers it into `reconcile: <job>: panic: <value>` and
the ID backs off like any other.

`Timeout` cancels the run's context through `Options.Clock`, which has no
wall-clock instant to report, so the context you are handed reports **no**
`Deadline()`. When the limit fires, `ctx.Err()` is `context.DeadlineExceeded`
and `context.Cause(ctx)` agrees.
