# Adapters and test support reference

Everything that plugs into the kernel's [four ports](kernel.md#ports) from
outside it: the in-process implementations the core ships with, the Redis and
OpenTelemetry adapters, the kratos bridge, and the packages that exist so you
can test all of it.

Each backend lives in its own Go module, so the core stays stdlib-only.

```sh
go get github.com/GareArc/converge/adapters/redis   # convredis
go get github.com/GareArc/converge/adapters/otel    # convotel
go get github.com/GareArc/converge/bridges/kratos   # convkratos
```

- [The port contract](#the-port-contract)
- [inmem](#inmem)
- [convredis](#convredis)
- [convotel](#convotel)
- [convkratos](#convkratos)
- [portcheck](#portcheck)
- [convergetest](#convergetest)
- [convergetest/versions](#versions)

## The port contract

The four port interfaces say almost nothing on their own. What an
implementation must actually *do* is pinned by the `portcheck` suites, which
every adapter in this repository runs identically. If you write your own
adapter, run them too; if you change what a port means, the suite changes in
the same commit.

The obligations worth stating in prose, because they are the ones people get
wrong:

- **`MQ.Consume` keeps a backlog.** A message published before any consumer
  attached is still delivered to the first one that does.
- **A `Nack` redelivers with the transport delivery count one higher**, and a
  `Nack` with a delay holds redelivery for at least that long.
- **An unacknowledged delivery comes back** after the adapter's redelivery
  window, and `Delivery.Extend` postpones that. A *stale* handle — one whose
  delivery has already been redelivered to somebody else — must not postpone
  or settle the successor's delivery.
- **Named groups each see every message**, and a group created after messages
  were published still receives them. Broadcast is the opposite: a subscriber
  sees only what is published after it attaches.
- **`Lease.TryAcquire` returns `(nil, false, nil)`** when the lease is held
  elsewhere, hands the lease to a successor once the holder's TTL expires, and
  a late `Release` by the previous holder must not disturb that successor.
- **`KV.SetCAS` clears any TTL** on the key it writes, `Get` reports absence
  through its `ok` result, and `Scan` visits every matching key at least once
  across its pages, without straying outside the prefix.

## inmem

Package `github.com/GareArc/converge/inmem`. Stdlib-only implementations of
all four ports, in one process. It exists so that everything in this library
runs and tests without a service to install — not so that it can be deployed.
**Nothing in it is shared between processes**, so two replicas backed by
`inmem` are two independent worlds.

```go
const DefaultVisibility = 30 * time.Second

type Options struct {
    Clock     converge.Clock
    Retention time.Duration
}

func NewMQ() *MQ
func NewMQWithClock(c converge.Clock) *MQ
func NewMQWithOpts(o Options) *MQ

func NewKV() *KV
func NewKVWithClock(c converge.Clock) *KV

func NewLease() *Lease
func NewLeaseWithClock(c converge.Clock) *Lease
```

A nil `Clock` in any of them means the wall clock. `NewMQ()` and
`NewMQWithClock(c)` are `NewMQWithOpts` with the obvious options.

**`inmem.MQ`** satisfies `converge.MQ` and every capability interface:
`GroupConsumer`, `BroadcastConsumer`, `DelayedPublisher`, `BacklogReporter`
and `GroupBacklogReporter`. Everything a job can ask for is available, which
is why the guide's programs need no services.

- `DefaultVisibility` (30s) is how long a delivery may go unacknowledged
  before it is redelivered. It is fixed; only `convredis` lets you set one.
- `Options.Retention` has **no default**: zero means entries are never
  dropped. When it is set, the backlog is pruned by enqueue age on publish and
  when a new group attaches.
- `Backlog` is the depth of the group `Consume` uses; `BacklogForGroup` is the
  depth of a named one. Both count pending plus in-flight messages.
- Consumers poll every 2ms rather than blocking, so an idle `inmem` job is
  busy in a small, bounded way.
- One extra method exists for tests: `Idle` reports that nothing is pending or
  in flight, which is what `convergetest.Harness.Drain` waits on.

**`inmem.KV`** stores byte slices with optional TTLs, expiring lazily on
access. `Scan` returns keys in sorted order, 100 per page, and uses the last
key of a page as the next cursor.

**`inmem.Lease`** is a map of names to expiries. Two extra methods exist for
tests: `Expire(name)` forces a lease to be lost so you can watch the failover,
and `Names()` lists what is currently held.

## convredis

Package `github.com/GareArc/converge/adapters/redis`, imported as
`convredis`. All three backing ports over one `*redis.Client`.

```go
const DefaultVisibility = 5 * time.Minute

type StreamsOpts struct {
    Clock      converge.Clock
    Retention  time.Duration
    Visibility time.Duration
}

func NewStreamsMQ(rdb *redis.Client, o StreamsOpts) converge.MQ
func NewListMQ(rdb *redis.Client) converge.MQ
func NewLease(rdb *redis.Client) converge.Lease
func NewKV(rdb *redis.Client) converge.KV

var ErrLeaseLost = errors.New("convredis: lease lost")
var ErrSettled = errors.New("convredis: delivery already settled")
```

### Streams MQ

`NewStreamsMQ` is the job transport. It satisfies every capability interface,
so any run mode and any worker feature works against it.

| Option | Zero value means |
| --- | --- |
| `Clock` | the wall clock |
| `Visibility` | `DefaultVisibility`, 5 minutes. A non-positive value takes the default too. |
| `Retention` | **never trim.** There is no default and no fallback. |

**Retention is the field to be deliberate about**, because every other queue
product you have used has a default here and this one does not.

- Trimming is **by age**, applied when the adapter publishes and when it
  creates a consumer group: entries older than `Retention` are dropped from
  the stream.
- Trimming **does not care whether anyone read the entry**. Set it too short
  and a message can vanish before a replica that was down comes back for it.
- Acknowledgement is `XACK` and nothing else — never `XDEL`. An acknowledged
  entry stays in the stream, so with `Retention` at zero the stream grows for
  as long as the job runs and is never trimmed at all.

Both ends of that are the operator's choice, and neither has a safe default
converge could pick for you. Pick a number that answers "how long an outage do
I want to survive".

**Backlog needs Redis 7.0 or newer.** `Backlog` and `BacklogForGroup` read the
`lag` field of `XINFO GROUPS`, which 7.0 added, and return `lag + pending`.
Against Redis 6.2 the field is absent and the adapter returns
`converge.ErrBacklogUnknown` — the engine then leaves `BacklogKnown` false
rather than reporting a made-up zero. Two special cases answer without
consulting the group at all: an empty stream is a backlog of zero, and a
stream whose group does not exist yet reports the whole stream length.

Other behaviour worth knowing:

- `Consume` reads through a reserved group named `converge`; `ConsumeGroup`
  uses the name the engine gives it.
- `ConsumeBroadcast` reads the stream from `$`, so a subscriber sees only what
  is published after it attaches. Its deliveries report `Attempt() == 1`
  always, and `Ack`, `Nack` and `Extend` are no-ops — there is nothing
  per-subscriber to settle.
- `PublishDelayed` holds the message in a sorted set and moves it into the
  stream when it comes due — which happens on the next poll by a **group**
  consumer of that queue (`Consume` or `ConsumeGroup`). `ConsumeBroadcast`
  does not release delayed messages, so a delayed enqueue aimed at an
  `OnAllReplicas` job never arrives.
- `Extend` after the delivery has been acknowledged returns `ErrSettled`.
- **The stream key is the queue name, exactly as the job declared or derived
  it** — `dify:credential:rotate` if a task declared that, or
  `<ns>/converge/queue/<task>` if it did not — so a producer in any language
  can find it. The adapter's own bookkeeping keeps a prefix, because those
  keys are the adapter's and not yours: `convredis:p:<queue>:<group>` the
  pending index, `convredis:a:<queue>:<group>` the attempt counters,
  `convredis:d:<queue>` the delayed set. None of them is subject to
  `Options.Namespace`; the namespace is already part of a derived queue name.

### List MQ

`NewListMQ` reads and writes a plain Redis list. It exists for exactly one
job: [`reconcile.NotificationsFrom`](reconcile.md#triggers), pointed at a
source some other system already writes. It **cannot carry worker work**: a
`BRPOP` deletes the element the instant it is read, so a durable task
registered against it fails at `Run` with `cannot carry work`, naming the
adapter type.

It is deliberately minimal, and the limits matter if you point it anywhere
else:

- **`Publish` writes the payload only.** `Message.Kind` and `Message.Headers`
  are dropped, so it cannot carry a worker envelope.
- `Ack` and `Extend` are no-ops; `Nack` returns the payload to the queue
  regardless of the delay you asked for, and because this list is written at
  one end and read from the other, it lands behind everything already
  waiting rather than at the front. `Attempt()` is always 1.
- `EnqueuedAt()` is the wall-clock time the payload was popped, not when it
  was written.
- It satisfies `BacklogReporter` via `LLEN`, and nothing else — no groups, no
  broadcast, no delayed publish.

### Lease and KV

`NewLease` takes a lease with `SETNX` plus a random token, and extends and
releases through token-checked Lua so one holder can never disturb another.
`LeaseHandle.Extend` returns `ErrLeaseLost` when the token no longer matches,
and closes `Done()` at that point; `Release` closes it too, and is safe to
call twice.

`NewKV` is `GET`/`SET`/`DEL`/`SCAN` plus a Lua compare-and-set. Two details:

- `SetCAS` writes with a bare `SET`, so a successful swap **clears any TTL**
  the key had. That is the port contract, not an accident.
- `Scan` matches `prefix*` 200 keys at a time and passes Redis's own cursor
  back as a string. This is the one place the adapter cannot fully guarantee
  the port contract: Redis `SCAN` promises that a key present for the whole
  iteration is returned *at least* once, and may return it more than once
  while the keyspace is being rehashed. The contract suite's stable keyspace
  does not provoke that, and converge's own shelf readers deduplicate, but a
  caller reading `KV.Scan` directly against Redis should too.

The adapter's integration tests run only when `CONVREDIS_TEST_ADDR` is set,
and they **flush database 9** of that instance on every open. Point it at a
disposable Redis and nothing else.

## convotel

Package `github.com/GareArc/converge/adapters/otel`, imported as `convotel`.

```go
func NewObserver(meter metric.Meter) (converge.Observer, error)
func RegisterGauges(meter metric.Meter, rt *converge.Runtime) error
```

**`NewObserver`** maps the six events onto instruments. It returns an error
if the meter refuses to create one.

| Instrument | Kind | From |
| --- | --- | --- |
| `converge.run.duration` | histogram, seconds | every `RunCompleted`; attributes `converge.job`, `converge.status` (`ok`/`error`, from whether `Err` was nil), `converge.outcome` |
| `converge.shelved` | counter | `RunCompleted` with outcome `Shelved` |
| `converge.discarded` | counter | `RunCompleted` with outcome `Discarded` |
| `converge.lease.transitions` | counter | `LeaseChanged`; attribute `converge.held` |
| `converge.notifications.dropped` | counter | `NotificationDropped` |
| `converge.notifications.skipped` | counter | `NotificationSkipped` |
| `converge.schedule.overruns` | counter | `ScheduleOverrun` |
| `converge.destroyed` | counter | `JobDestroyed` |

**`RegisterGauges`** adds five observable gauges read from `rt.Stats()` at
collection time. It is separate from `NewObserver` because it needs the
`Runtime`, which the observer is built before.

| Gauge | Observed |
| --- | --- |
| `converge.backlog` | only when `BacklogKnown` — otherwise **nothing is reported for that job**, and the gap is the honest answer |
| `converge.failing` | always |
| `converge.shelved.current` | only when `ShelvedKnown` |
| `converge.lease_held` | 1 or 0, and only for `OnOneReplica` jobs |
| `converge.in_flight` | always |

The two `*Known`-gated gauges are the reason a dashboard may show a gap. That
is deliberate: converge does not publish a zero it cannot stand behind. See
[how stale these readings are](operations.md#how-stale-a-number-can-be).

## convkratos

Package `github.com/GareArc/converge/bridges/kratos`, imported as
`convkratos`.

```go
func Server(rt *converge.Runtime) transport.Server

var ErrAlreadyStarted = errors.New("convkratos: Start called on a server that is already running")
var ErrDrainIncomplete = errors.New("convkratos: Stop returned before the runtime finished draining")
```

Wraps a `Runtime` as a kratos `transport.Server` so converge joins a kratos
app's lifecycle instead of needing a goroutine of its own.

- `Start(ctx)` runs `rt.Run` and blocks until it returns. Its error is
  `rt.Run`'s error, so nil means a clean shutdown.
- `Stop(ctx)` cancels the run and waits for `Run` to return. If `ctx` expires
  first it returns `ErrDrainIncomplete` — the runtime is still draining, and
  kratos's stop timeout was shorter than converge's `DrainTimeout` plus the
  work in flight. Make the kratos timeout the longer of the two.
- `Stop` before `Start` is a no-op returning nil, and `Start` after `Stop`
  returns nil without starting anything.
- A second concurrent `Start` returns `ErrAlreadyStarted`.

## portcheck

Package `github.com/GareArc/converge/convergetest/portcheck`. The exported
contract suites. Run them from your adapter's own tests; they are the
definition of what the ports mean.

```go
type MQOptions struct {
    Advance           func(d time.Duration)
    Visibility        time.Duration
    Retention         time.Duration
    GroupLagIsStubbed bool
}

type LeaseOptions struct {
    Advance func(d time.Duration)
}

type KVOptions struct {
    Advance func(d time.Duration)
}

func MQ(t *testing.T, open func(t *testing.T) converge.MQ, o MQOptions)
func Lease(t *testing.T, open func(t *testing.T) converge.Lease, o LeaseOptions)
func KV(t *testing.T, open func(t *testing.T) converge.KV, o KVOptions)
```

`open` is called once per subtest and must return a fresh, empty instance.

Subtests skip themselves rather than failing when they do not apply, which is
what lets one suite cover very different backends:

| Option | Zero value means |
| --- | --- |
| `Advance` | nil — every subtest that needs to move time skips |
| `Visibility` | zero — the four reclaim and stale-handle subtests skip. Set it to the adapter's redelivery window. |
| `Retention` | zero — the retention subtest skips, because the adapter has no retention to exercise |
| `GroupLagIsStubbed` | false. Set it when the backend does not compute consumer-group lag itself, and the two backlog-arithmetic subtests skip |

Capabilities skip by type assertion: an `MQ` that is not a `GroupConsumer`
simply skips the group subtests. A backlog reporter that answers
`converge.ErrBacklogUnknown` skips the subtest asking for a number.

## convergetest

Package `github.com/GareArc/converge/convergetest`. A whole `Runtime` backed
by in-process ports, a clock you move by hand, and verbs that wait for the
system to settle rather than for the wall clock. [Chapter 7 of the
guide](../guide/07-testing.md) teaches it.

```go
type Options struct {
    Namespace    string
    LeaseTTL     time.Duration
    DrainTimeout time.Duration
    Clock        *Clock
    MQ           func(*Clock) converge.MQ
    KV           func(*Clock) converge.KV
    Lease        func(*Clock) converge.Lease
}

type Harness struct {
    MQ    *MQ
    KV    *inmem.KV
    Lease *inmem.Lease
    // ...
}

func New(t testing.TB) *Harness
func NewWith(t testing.TB, o Options) *Harness

func (h *Harness) Options() converge.Options
func (h *Harness) Build(t testing.TB) *converge.Runtime
func (h *Harness) Runtime(t testing.TB) *converge.Runtime
func (h *Harness) Clock() *Clock
func (h *Harness) Drain(t testing.TB)
func (h *Harness) Sweep(t testing.TB, job string)
func (h *Harness) Notify(job, id string)
func (h *Harness) Stop(t testing.TB) error
func (h *Harness) Events() []converge.Event
func (h *Harness) AssertReconciled(t testing.TB, job, id string)
func (h *Harness) AssertEnqueued(t testing.TB, task TaskRef, want any)
```

| `Options` field | Zero value means |
| --- | --- |
| `Namespace` | `"test"` |
| `LeaseTTL` | one year — long enough that a lease never expires by accident while you are moving the clock |
| `DrainTimeout` | zero, which `converge.New` then turns into its own 30s default |
| `Clock` | a fresh fake clock starting at 2026-08-17T00:00:00Z |
| `MQ` / `KV` / `Lease` | the `inmem` implementation, wired to the harness clock |

The harness fails the test itself rather than returning errors, which is why
most verbs take a `testing.TB`.

- **One runtime per harness.** `Build` calls `converge.New` with
  `h.Options()`; calling it twice fails the test.
- **The runtime starts lazily**, on the first verb that needs it running. That
  is what lets a test `Build`, register jobs and seed its store before
  anything runs.
- **Supplying your own `MQ`, `KV` or `Lease` constructor leaves the matching
  exported field nil.** `h.MQ` is the recording wrapper, so a custom `MQ`
  means `AssertEnqueued` cannot work and says so.

| Verb | Behaviour |
| --- | --- |
| `Drain` | poll until nothing is queued, in flight, or pending in the MQ, twice in a row. It is a settle, not a sleep: it returns as soon as the system is quiet and fails the test after 10s if it never is. |
| `Sweep` | force one sweep of a named job now, then `Drain`. Retries for up to 5s while the job is not yet active. Fails on a worker job — sweeping is a reconcile verb. |
| `Notify` | queue an ID the way a producer's `Notify` would, with no producer and no second binary. Fails on a worker job — workers react to deliveries instead. |
| `Stop` | cancel the runtime and return what `Run` returned. After it, only `Events` still works. |
| `Events` | every event recorded so far, as a copy. |
| `AssertReconciled` | poll for a `RunCompleted` with that job, that ID and outcome `Succeeded`. Fails after 2s, printing every event it saw. |
| `AssertEnqueued` | poll for a message on the task's queue (`TaskRef.QueueName(namespace)`) whose payload equals the task's encoding of `want`. |

```go
type TaskRef interface {
    Name() string
    QueueName(namespace string) string
    Encode(v any) ([]byte, error)
}
```

What `AssertEnqueued` needs of a task. `worker.Task[T]` satisfies it.

### Clock, MQ, Recorder and the free helpers

```go
type Clock struct { /* ... */ }

func NewClock(start time.Time) *Clock
func (c *Clock) Now() time.Time
func (c *Clock) After(d time.Duration) <-chan time.Time
func (c *Clock) Advance(d time.Duration)
func (c *Clock) Waiting(d time.Duration) int
```

Time moves only when you move it. `Advance` fires every waiter whose deadline
the new time has reached, in one step — it does not walk intermediate
instants, so advancing an hour past a one-minute cadence fires that waiter
once, not sixty times. `After` with a non-positive duration fires immediately.
`Waiting(d)` counts the waiters registered with exactly that duration, which
is how a test can tell *which* timer something is sitting on.

```go
type MQ struct { /* ... */ }

func WrapMQ(base *inmem.MQ) *MQ
func (m *MQ) Published(queue string) []converge.Message
func (m *MQ) FailNextPublish(err error)
func (m *MQ) Idle() bool
```

A recording wrapper around `inmem.MQ` that forwards every port and capability
method. `Published` returns copies of what was published to a queue, in order,
including delayed publishes. `FailNextPublish` arms a single failure — the
next `Publish` or `PublishDelayed` returns it and the arm clears, so you can
test one transport failure without breaking the rest of the test.

```go
type Recorder struct { /* ... */ }

func (r *Recorder) Observe(e converge.Event)
func (r *Recorder) Events() []converge.Event
func (r *Recorder) Count(match func(converge.Event) bool) int
```

The harness's own `Observer`. `h.Events()` is the recorder's; `Count` is there
for assertions like "exactly one lease change".

```go
func Await(t testing.TB, cond func() bool)
func AdvanceUntil(t testing.TB, c *Clock, step time.Duration, cond func() bool)
func AssertStable(t testing.TB, cond func() bool)
```

- `Await` polls `cond` until it is true, failing after 2s.
- `AdvanceUntil` does the same while advancing the clock by `step` between
  polls — for conditions that only become true once time passes.
- `AssertStable` waits a short window and then requires `cond` to still hold.
  It is how you assert that something did **not** happen.

Await on the state you are about to assert on, not on a proxy for it. A
counter your handler increments before it returns can be true a moment before
the engine has finished recording the run, and a test that waits on the
counter and then reads `Stats` can sample the gap.

## versions

Package `github.com/GareArc/converge/convergetest/versions`. A
`reconcile.VersionSource` you can drive from a test.

```go
type Source struct { /* ... */ }

func Fixed(latest map[string]reconcile.Version) *Source
func (s *Source) Latest(ctx context.Context, id reconcile.ID) (reconcile.Version, error)
func (s *Source) Bump(id string) reconcile.Version
```

`Fixed` seeds the versions; `Bump` raises one and returns the new value —
call it from inside a reconcile function to simulate the intent moving
mid-run. `Latest` returns an **error** for an ID it has never heard of, which
is on purpose: it makes an unseeded ID visible instead of quietly reading as
version zero. Remember what the engine does with that error — it treats the
version as unknown for that run and settles normally, so an unseeded ID gets
no version checking rather than a failure.
