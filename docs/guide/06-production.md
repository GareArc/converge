# 6. Going to production

Every job in this guide so far has kept converge's own bookkeeping — the
[lease](../glossary.md#lease) that says which copy is in charge, and the
record of when each [reconcile](../glossary.md#reconcile) job last ran — in
memory. That is why chapter 1 needed nothing installed, and it is also why
none of it survived a restart or reached a second copy of your service: the
bookkeeping lived inside the same process as the job, and died with it. This
chapter moves that bookkeeping to Redis. The job itself does not change —
same function, same schedule, same `reconcile.Periodic` call. What changes
is where converge writes down what it knows.

## The code

The whole program:

```go title=examples/guide/06-production/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	rt, err := converge.New(converge.Options{
		Namespace: "billing",
		MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
		Lease:     convredis.NewLease(rdb),
		KV:        convredis.NewKV(rdb),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Periodic(rt, "refresh-licenses", reconcile.Every(10*time.Second), func(ctx context.Context) error {
		fmt.Println("refreshing licenses")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Two of chapter 1's lines are swapped: `Lease: inmem.NewLease()` and
`KV: inmem.NewKV()` become `convredis.NewLease(rdb)` and
`convredis.NewKV(rdb)`. The third line, `MQ:
convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{})`, replaces the
in-memory `MQ` [chapter 4](04-worker.md) introduced; chapter 1 itself never
had one, and neither did chapters 2 and 3. With everything in memory, a
[poke](../glossary.md#poke) could only ever reach the process that raised
it; there was no second copy for it to reach. `MQ` is what carries a
trigger from the copy that received it to the copy that will act on it, so
from here on it is part of the wiring, the same as the other two.
`Namespace: "billing"` is new too, and gets its own note at the end of this
chapter. The job registration and the `signal.NotifyContext` shutdown below
it are exactly chapter 1's, unchanged, other than the ten-second interval —
long enough to Ctrl-C the process partway through one and restart it before
the next was due, which is what the next section does.

## Run it

This is the only chapter in the guide whose example needs something
installed: a running Redis.

```sh
docker run --rm -d -p 6379:6379 --name converge-docs redis:7-alpine
cd examples && go run ./guide/06-production
```

```
refreshing licenses
refreshing licenses
refreshing licenses
^C
```

## What happened

Nothing about the output is new: the same `refreshing licenses` line, on
the same schedule, with the same immediate first run chapter 1 already
showed you. The difference chapter 1 promised is invisible from the
terminal — it is sitting in Redis. Leave the program running, and in
another terminal:

```sh
docker exec converge-docs redis-cli --scan --pattern 'billing*' | head
```

```
billing/converge/reconcile/refresh-licenses/sched/0/last
billing/converge/reconcile/refresh-licenses/lease
```

Two keys, under the `billing` namespace. The first is the record of when
`refresh-licenses`'s schedule last fired. In chapter 1 the equivalent
record lived on the Go heap, inside the `inmem.KV` value, and vanished the
moment the process did. This one lives in Redis and outlives the process.

The second is [chapter 5](05-run-modes.md)'s lease, made visible for the
first time: the claim that says this copy is the one running
`refresh-licenses`. Chapter 5 described its lifetime; Redis will show it
to you:

```sh
docker exec converge-docs redis-cli ttl billing/converge/reconcile/refresh-licenses/lease
```

```
27
```

Twenty-seven seconds left on a thirty-second claim, pushed back up on a
timer for as long as this copy keeps running. The two keys have opposite
lifetimes, on purpose: Ctrl-C the program, scan again, and the lease is
gone — released on the way out, so the next copy to start does not have to
wait it out — while the schedule's record is still sitting there. That
record is what the next section puts to the test.

## Surviving a restart

[Chapter 3](03-triggers.md) already told you what this swap would do:
restart the service in the middle of an interval, and converge would find
the record and work out the interval had not passed, instead of firing
again the moment the process came back. Try it.

Start the program and note when the first line prints:

```sh
cd examples && go run ./guide/06-production
```

```
refreshing licenses
^C
```

Three seconds later — comfortably inside the ten-second interval — Ctrl-C
landed and the process exited. Start it again immediately:

```sh
cd examples && go run ./guide/06-production
```

Nothing prints right away. It stays that way for seven more seconds, and
then:

```
refreshing licenses
```

Ten seconds after the very first line — not three seconds after the
restart. converge read `billing/converge/reconcile/refresh-licenses/sched/0/last`
on the way up, worked out that only three seconds had passed since the
schedule last fired, and waited out the rest instead of treating the
restart as a reason to run again.

Do the same thing with [chapter 1](01-first-job.md)'s program — stop it a
second into its two-second interval, start it again right away:

```sh
cd examples && go run ./guide/01-first-job
```

```
refreshing licenses
^C
```

```sh
cd examples && go run ./guide/01-first-job
```

```
refreshing licenses
```

No wait, regardless of how far into the interval you stopped it —
`refreshing licenses` prints the instant the new process starts, every
time. There is no record anywhere for it to find: `inmem.NewKV()` forgot
everything the moment the old process exited, so every restart looks like
the very first run again. That is the whole difference this chapter makes:
not a different job, not different output under normal operation, but a
service that remembers what it was doing across the one event — a restart
— in-memory bookkeeping can never survive.

## The principle

Every piece of converge's own bookkeeping goes through exactly four
things. This chapter's program wires three of them; the fourth waits until
chapter 10:

- `MQ` — carries messages between your copies.
- `Lease` — decides which copy is in charge ([chapter 5](05-run-modes.md)).
- `KV` — remembers when each job last ran and what has been set aside.
- `Observer` — reports what happened, for metrics
  ([chapter 10](10-observability.md)).

converge never talks to Redis directly; it only ever calls these four, and
what is wired up behind them — `convredis`, in this chapter — is the whole
of what changes going to production. The job, the schedule, and everything
else about `refresh-licenses` are exactly what they were in chapter 1.

## Try breaking it

Start the program again, and once you have seen the first line, stop
Redis without stopping the program:

```sh
docker stop converge-docs
```

Wait through what should have been the next two ticks — twenty seconds, at
this job's interval. The output stays exactly what it was:

```
refreshing licenses
```

Nothing else. No error on stdout, none on stderr, no crash — the process
is still running, and it is printing nothing. `MQ`, `Lease`, and `KV`
carry no health check of their own: when the backend behind them becomes
unreachable, the loops that use them retry silently and indefinitely, and
nothing in converge surfaces "the backend is down" as an event or a metric
— see the [operations reference](../reference/operations.md#operational-visibility)
for the full picture. A job that has quietly stopped running looks, from
inside the process, identical to a job with nothing to do.

Bring Redis back:

```sh
docker run --rm -d -p 6379:6379 --name converge-docs redis:7-alpine
```

Without restarting the program, it picks up on its own — in the run
behind this transcript, six seconds after Redis came back:

```
refreshing licenses
```

Nobody watching only this process's output would know anything had
happened, either time. Pair converge with monitoring on Redis itself —
connection health, replication lag, disk — because converge will not tell
you it is down. [Chapter 10](10-observability.md) covers the one alert
that catches this from the outside.

## A caveat

`Namespace: "billing"` is what keeps this Redis safe to share. Every key
`refresh-licenses` writes — its lease, its schedule record — is prefixed
with it, so a second service pointed at the same Redis, with its own
namespace, never collides with this one, even if it happens to register a
job with the same name.

Two services that pick the *same* namespace do not get that protection.
If both register a job called `refresh-licenses`, they share one lease and
one schedule record for it: only one service's replicas can hold the
lease at a time, so the other service's copies quietly never run their own
`refresh-licenses`, and every run — by whichever service actually holds
the lease — overwrites the one shared "last ran" timestamp, so each
service's job looks to converge like a run of the other's. Give every
service its own namespace; nothing checks that you picked a different one
than your neighbour.

Next: [7. Stale writes](07-versions.md). The core path ends here —
chapters 1 through 6 are what every job in this guide has needed. What
follows is opt-in: reach for a chapter when your job's situation calls
for it, not because chapter 6 left it unfinished.
