# Taking it to production

Everything so far ran on `inmem`, which lives in one process and forgets
everything when that process exits. This chapter swaps it for Redis, adds
logs and an endpoint you can curl, and tells you which numbers converge will
report and which it will honestly refuse to.

## Wiring Redis

One `converge.Options` per binary, and no job changes:

```go
rt, err := converge.New(converge.Options{
    Namespace: namespace,
    MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{Retention: 7 * 24 * time.Hour}),
    Lease:     convredis.NewLease(rdb),
    KV:        convredis.NewKV(rdb),
    Observer:  converge.LogObserver(slog.Default()),
})
```

`convredis` lives in its own module, so the core stays dependency-free:

```sh
go get github.com/GareArc/converge/adapters/redis
```

It gives you all three backends over one `*redis.Client`: messages over
Redis Streams, leases and `KV` over plain keys. Nothing about your jobs
changes — the [reconcile](../glossary.md#reconcile) and
[worker](../glossary.md#worker) declarations from chapters 1 to 5 run
against this unmodified.

**`StreamsOpts.Retention` has no default, and zero does not mean "an hour"
or "a day" — it means never trim.** Every other queue product you have used
has a default here, so this is the field to be deliberate about.

converge trims by age, when it publishes: entries older than `Retention` are
dropped from the stream whether or not anybody read them. Leave it at zero
and nothing is ever dropped — a Redis stream keeps an entry after it has
been acknowledged, so the stream grows for as long as the job runs. Set it
too short and a message can vanish before a replica that was down comes back
for it. Seven days, above, reads as "survive a week-long outage, then give
up"; choose yours the same way.

`convredis.NewListMQ(rdb)` is the other one, for reading a Redis list some
other system pushes onto. That is a [chapter 3](03-notifications.md)
concern, and it is the only place a raw queue name appears.

## Logs, for free

`converge.LogObserver(slog.Default())` turns the five things converge
observes into `slog` records:

| Event | Logged when | Level |
| --- | --- | --- |
| `RunCompleted` | every run of your function ends | by outcome |
| `LeaseChanged` | a replica takes or loses a job's [lease](../glossary.md#lease) | info |
| `ScheduleOverrun` | a [sweep](../glossary.md#sweep) was still running when the next fell due | warn |
| `NotificationDropped` | a [notification](../glossary.md#notification) could not be decoded, or the queue of pending IDs was full | warn |
| `JobDestroyed` | a job reached the end of its life | info |

`RunCompleted` is levelled by how the run ended: succeeded and discarded are
info, retrying and deferred are warn, shelved is error. That mapping is the
one you want in an alert rule — a
[shelved message](../glossary.md#shelved-message) is the only one of the
five that means a person has to do something.

`converge.Observer` is a one-method interface, so `LogObserver` is a
starting point rather than a ceiling.
`convotel.NewObserver(meter)` maps the same five events onto OpenTelemetry
metrics, and `convotel.RegisterGauges(meter, rt)` adds five gauges read
from `rt.Stats()` at collection time.

## Readiness and a page to look at

`rt.Ready()` closes when every registered job has started. It is a channel,
so a readiness probe is a `select` with a `default`:

```go
jobs := debughttp.ReadOnlyHandler(rt)
mux := http.NewServeMux()
mux.Handle(readyPath, readyWhen(rt.Ready()))
mux.Handle(statsPath, jobs)
mux.Handle(statsPath+"/", jobs)
```

`debughttp.ReadOnlyHandler(rt)` serves `/debug/jobs` and
`/debug/jobs/{job}`. Those routes are **absolute**: the handler is its own
mux and it matches on the full path, so mounting it under a prefix of your
own will not work. `statsPath` in the program above is exactly
`"/debug/jobs"`, and both patterns are registered so that the trailing-slash
form reaches the single-job route.

`/debug/jobs` answers with a `jobs` array, one object each: everything
`rt.Stats()` carries, plus the queue the job reads and the settings it was
registered with. `/debug/jobs/{job}` answers with that one object plus one
thing the list leaves out — and which one depends on the kind of job. A
reconcile job gets `failing_ids`: the IDs currently in backoff, each with
its last error. A worker job gets `shelved_messages` instead. Neither route
returns both, because neither kind of job has both: a reconcile job has no
[shelf](../glossary.md#shelf), and the worker engine keeps a count of the
messages it is retrying but not a list of them.

## The numbers, and when converge refuses to give you one

`rt.Stats()` returns a `converge.JobStats` per job. Two of its fields do not
come with a number attached, and understanding why will save you filing a
bug:

```go
func describe(s converge.JobStats) string {
    return fmt.Sprintf("%s surface=%s state=%s in_flight=%d backlog=%s shelved=%s",
        s.Job, s.Surface, s.State, s.InFlight,
        countOrUnknown(s.Backlog, s.BacklogKnown),
        countOrUnknown(s.Shelved, s.ShelvedKnown))
}
```

**`Backlog` travels with `BacklogKnown`.** The **backlog** is the real depth
of a job's [inbox](../glossary.md#inbox), read from the message queue rather
than counted inside your process — and not every backend can answer that
question. When it cannot, `BacklogKnown` is false and `Backlog` is
meaningless. Not zero: *unknown*. converge does not invent the number, the
metrics do not publish it, and a dashboard that shows a gap there is telling
you the truth.

One case is unknown on every backend, no matter how capable: **a job running
on all replicas has no backlog.** Both engines decline to read one for that
[run mode](../glossary.md#run-mode), and they are right to — every replica reads every message, so
there is no shared depth that would mean anything. Expect a blank there on
the newest Redis as surely as on the in-memory backend.

On Redis Streams specifically: **backlog needs Redis 7.0 or newer.** converge
reads it from the `lag` field of `XINFO GROUPS`, which 7.0 added. Against
Redis 6.2 the field is absent, converge reports the backlog as not known
rather than guessing, and that is what you will see. If you expected a
number and got a blank, check the server version before you check anything
else.

**`Shelved` travels with `ShelvedKnown`, and it is a depth.** It is how many
messages are on that job's shelf *right now*, not how many runs have ever
been shelved. It falls when somebody requeues or
purges, which is exactly what you want from something you are going to
alert on. It is known only for a worker job with a `KV` and a run mode that
can shelve at all — a reconcile job never reports it, and neither does a
broadcast worker job, because neither has a shelf.

`Failing` is the third number and it always has a value: how many
[IDs](../glossary.md#id) or messages on *this replica* are currently waiting
out a backoff. A **failing ID** is not a lost one — it keeps being retried
at a floor rate — but a `Failing` count that grows and never falls is worth
a look.

Where you look depends on the kind of job, for the reason above. On a
reconcile job, `/debug/jobs/{job}` names the failing IDs and gives you the
last error for each. On a worker job it will not: converge counts the
messages it is retrying and does not keep them, so what you have is
`last_error`, `last_error_at` and `consecutive_fails` in the same response,
and the warn-level `RunCompleted` log lines — each one carries the message
ID of the message being retried, which is the thread to pull.

Run
[`examples/scenarios/a15-operations/main.go`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a15-operations/main.go)
to see all of it end to end — a readiness probe, a message that fails its
way onto the shelf, the shelf depth appearing in the stats, and a requeue:

```sh
cd examples
go run ./scenarios/a15-operations
```

```text
http://127.0.0.1:62657/healthz/ready -> 200 OK, 6 bytes
shelved message: 26782515c4adbd3c71abdf9c355c45f9
http://127.0.0.1:62657/debug/jobs -> 200 OK, 548 bytes
deliver-webhook surface=worker state=active in_flight=0 backlog=0 shelved=1
delivered after requeue: 1
```

That requeue is the operator loop in full:

```go
shelf, err := worker.ShelfFrom(rt, deliverWebhook.Name())
if err != nil {
    return err
}

hooks.repair()
if err := shelf.Requeue(ctx, messageID); err != nil {
    return err
}
```

Fix the cause, then put the message back. Never the other way round: a
requeue starts the retry budget over, so requeueing into a still-broken
endpoint just spends it again.

## Time limits, and the two that are not per-run

`Timeout` — the **time limit** — is per run and belongs to the job. Two
others belong to the runtime, in `converge.Options`:

- **`LeaseTTL`** (default 30s) is how long a lease survives without being
  renewed, so it is also how long a job can be stalled after a replica dies
  before another picks it up. converge renews at a third of it.
- **`DrainTimeout`** (default 30s) is how long a job waits for its in-flight
  runs to finish after the context is cancelled; when it elapses converge
  cancels them and waits for them to return. Make it longer than your
  longest `Timeout` if you care about clean shutdowns, and make your
  orchestrator's grace period longer than it.

## Jobs that are supposed to end

Most jobs run forever. A migration does not. Declare that where you register
it, with `Until`:

```go title=examples/scenarios/a12-legacy-migration/main.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow = 10 * time.Second
	leaseTTL   = 3 * time.Second
	cutoverIn  = 2 * time.Second

	targetAlgorithm = "argon2id"
)

type credentialTable struct {
	mu       sync.Mutex
	legacy   map[reconcile.ID]string
	migrated []string
}

func newCredentialTable(legacy map[reconcile.ID]string) *credentialTable {
	return &credentialTable{legacy: legacy}
}

func (t *credentialTable) unmigrated(_ context.Context, _ string) ([]reconcile.ID, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pending := make([]reconcile.ID, 0, len(t.legacy))
	for id := range t.legacy {
		pending = append(pending, id)
	}
	slices.Sort(pending)
	return pending, "", nil
}

func (t *credentialTable) migrateOne(_ context.Context, id reconcile.ID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	algorithm, ok := t.legacy[id]
	if !ok {
		return nil
	}
	delete(t.legacy, id)
	t.migrated = append(t.migrated, fmt.Sprintf("%s %s->%s", id, algorithm, targetAlgorithm))
	return nil
}

func (t *credentialTable) report() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	migrated := slices.Clone(t.migrated)
	slices.Sort(migrated)
	return []string{
		fmt.Sprintf("migrated: %v", migrated),
		fmt.Sprintf("still legacy: %d", len(t.legacy)),
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	rt, err := converge.New(converge.Options{
		Namespace: "identity",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
		LeaseTTL:  leaseTTL,
	})
	if err != nil {
		return err
	}

	creds := newCredentialTable(map[reconcile.ID]string{
		"cred-3001": "sha1",
		"cred-3002": "sha1",
		"cred-3003": "md5",
	})
	cutover := time.Now().Add(cutoverIn)

	err = reconcile.Register(rt, reconcile.Spec{
		Name:      "legacy-credential-migration",
		Reconcile: creds.migrateOne,
		Triggers:  []reconcile.Trigger{reconcile.Schedule(reconcile.IDsByPage(creds.unmigrated), reconcile.Every(time.Minute))},
		Until:     converge.Deadline(cutover),
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range creds.report() {
		fmt.Println(line)
	}
	for _, s := range rt.Stats() {
		fmt.Printf("%s state=%s\n", s.Job, s.State)
	}
	fmt.Printf("cutover: %s\n", cutover.Format(time.RFC3339))
	return nil
}
```

```sh
cd examples
go run ./scenarios/a12-legacy-migration
```

Timestamps and run durations are trimmed from the log lines below; nothing
else is.

```text
INFO converge: lease changed job=legacy-credential-migration held=true
INFO converge: run completed job=legacy-credential-migration id=cred-3001 attempt=1 outcome=succeeded
INFO converge: run completed job=legacy-credential-migration id=cred-3002 attempt=1 outcome=succeeded
INFO converge: run completed job=legacy-credential-migration id=cred-3003 attempt=1 outcome=succeeded
INFO converge: job destroyed job=legacy-credential-migration cause="deadline 2026-08-27T05:47:31-07:00"
INFO converge: lease changed job=legacy-credential-migration held=false
migrated: [cred-3001 sha1->argon2id cred-3002 sha1->argon2id cred-3003 md5->argon2id]
still legacy: 0
legacy-credential-migration state=destroyed
cutover: 2026-08-27T05:47:31-07:00
```

The job kept the lease while it worked, was destroyed the moment the cutover
passed, and gave the lease back — after which nothing runs it again, on this
replica or any other.

A **stop condition** comes in two forms:

- `converge.Deadline(t)` — the job ends at a moment you already know, like a
  cutover.
- `converge.StopKey("migration/finished")` — the job ends when somebody sets
  that key in `KV`. That somebody is a person with the `KV`, not an API
  converge exposes.

A job has three states and no others: not started, active, destroyed. Only
the last of them is cluster-wide, and the
[tombstone](../glossary.md#tombstone) below is what makes it
so; the first two are what one replica reports about itself in `Stats()`.
Destruction is terminal and needs no cooperation from your function — it
just stops being called. There is no pausing a job and no
resuming one; if a job is ever going to finish, you say so where you
register it.

Destruction survives restarts because converge writes a **tombstone** into
`KV` when it happens, and every replica checks for it before taking a lease,
at each renewal, and at the start of each sweep. Redeploy the same binary
and the job stays destroyed. It goes away for good when you delete its code.

## Next

[Chapter 7](07-testing.md) tests all of this without Redis, without sleeping,
and without flakes.
