# Reconcile reference

Package `github.com/GareArc/converge/reconcile`. The level-triggered surface:
you name a source of IDs and a cadence, converge visits every ID on that
cadence, and your function re-reads the truth and makes the world match it.

The [guide's chapters 2 and 3](../guide/02-ids.md) teach the model. This page
is the surface, its defaults, and its refusals.

- [Spec and Register](#spec-and-register)
- [Periodic](#periodic)
- [Triggers](#triggers)
- [Cadences](#cadences)
- [ID sources](#id-sources)
- [ID and ToIDs](#id-and-toids)
- [ID functions for foreign queues](#id-functions-for-foreign-queues)
- [Versions](#versions)
- [CheckAgain and ErrOutdated](#checkagain-and-erroutdated)
- [What a run costs when it fails](#what-a-run-costs-when-it-fails)

## Spec and Register

```go
type Spec struct {
    Name        string
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
| `Name` | invalid | Required. The name other code addresses the job by, and the name in every log line, metric and key. It must not contain `/`. |
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
| `reconcile: Spec.Name is required` | empty `Name` |
| `reconcile: job %q: Name must not contain "/"` | `/` in `Name` |
| `reconcile: job %q: Spec.Reconcile is required` | nil function |
| `reconcile: job %q: Concurrency must not be negative` | negative `Concurrency` |
| `reconcile: job %q: Timeout must not be negative` | negative `Timeout` |
| `reconcile: job %q: Competing is a worker mode` | `RunMode: converge.Competing` |
| `reconcile: job %q: Triggers must not contain nil` | a nil element |
| `reconcile: job %q: no Schedule trigger; every reconcile job needs one` | no `Schedule` in `Triggers` |
| `reconcile: job %q: Schedule needs an IDSource` | `Schedule` built from a zero `IDSource` — including one built from a nil function |
| `reconcile: job %q: Schedule needs a Cadence; use Every or Cron` | zero `Cadence` |
| `reconcile: cron %q: ...` | a `Cadence` that failed to parse; the error is carried from `Cron` and surfaces here |
| `reconcile: job %q: NotificationsFrom needs a queue name` | empty queue |
| `reconcile: job %q: NotificationsFrom needs an ID function` | `NotificationsOpts.ID` nil on a foreign queue |
| `reconcile: job %q: Notifications always reads Options.MQ; MQ is NotificationsFrom only` | `NotificationsOpts.MQ` set on a plain `Notifications` trigger |

Plus the runtime's own three: empty name, duplicate name, and registering
after `Run` has started.

Three more failures wait until `Run`, because they are about what the runtime
was wired with rather than what the spec says:

- `reconcile: job %q: OnOneReplica needs Options.Lease`
- `reconcile: job %q: Until needs Options.KV`
- `reconcile: job %q: Notifications needs Options.MQ`, or for a foreign queue
  `NotificationsFrom(%q) needs an MQ`, and — under `OnAllReplicas` —
  `notifications from %q need the BroadcastConsumer capability`

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
`Producer.Notify(ctx, name, "")` addresses.

## Triggers

```go
type Trigger interface {
    Run(ctx context.Context, notify func(ID)) error
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

`Trigger` is implemented for you three times over — `Schedule`,
`Notifications` and `NotificationsFrom`. `PeriodicTrigger` marks the
schedule; a job is refused at registration unless one of its triggers
implements it.

You can implement `Trigger` yourself: return when `ctx` is done, and call
`notify(id)` for every ID you learn about. A custom trigger that returns before
its context is done is restarted under a bounded backoff (1s to 1m), so a
transient failure does not need handling inside it.

```go
type NotificationsOpts struct {
    ID func(payload []byte) (ID, error)
    MQ converge.MQ
}

func Notifications(o NotificationsOpts) Trigger
func NotificationsFrom(queue string, o NotificationsOpts) Trigger
```

**`Notifications`** reads the job's own inbox over `Options.MQ`. Both fields
are meant to be left zero: setting `MQ` is refused, and `ID` is ignored
because converge owns the payload format of its own notifications.

**`NotificationsFrom`** reads a queue some other system already writes. It is
the only place in the surface where a raw queue name appears, and the string
is used **exactly as given** — not namespaced, not prefixed. Both fields are
then required: `ID` because converge has no idea what shape that system's
messages are, and `MQ` because a foreign queue is usually not the transport
your own jobs use.

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
the empty one — which is reserved for `SingleID` jobs and is what
`Producer.Notify` sends when you pass an empty id. `ToIDs` is the bulk
conversion `StringIDs` uses internally.

## ID functions for foreign queues

```go
func RawID() func(payload []byte) (ID, error)
func IDFromJSON(field string) func(payload []byte) (ID, error)
```

For `NotificationsOpts.ID` on a foreign queue.

- **`RawID()`** takes the whole payload as the ID. An empty payload is
  `reconcile: empty payload`.
- **`IDFromJSON(field)`** unmarshals the payload as a JSON object and reads
  one **string** field. It errors — and the notification is dropped and
  reported — if the payload is not a JSON object, if the field is missing, if
  it is not a string, or if it is the empty string. Unknown extra fields are
  ignored.

Both return a function, so both are called at spec-construction time:
`ID: reconcile.IDFromJSON("workspace_id")`.

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
[fixed in-memory source](adapters.md#convergetest-versions) for tests.

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
  further along that curve, so a thing that will never be ready costs less
  and less rather than the same forever. A success or a failure resets both
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
