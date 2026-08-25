# 3. Triggers

Chapters 1 and 2 only ever ran on the schedule: converge called your
function on its own timeline, and the only way to see a check happen was to
wait for the next round. This chapter adds two ways to step outside that:
asking converge to check one thing right now, and having your own function
ask to be called back again shortly, without waiting for the schedule to
come around. By the end you will have used both.

## The code

The whole program:

```go title=examples/guide/03-triggers/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func main() {
	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	attempts := 0
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "wait-for-cluster",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			attempts++
			fmt.Printf("checking %s (attempt %d)\n", id, attempts)
			if attempts < 3 {
				return reconcile.CheckAgain{In: 500 * time.Millisecond}
			}
			fmt.Println("cluster is ready")
			return nil
		}),
		AllowUnscheduled: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		<-rt.Ready()
		if err := rt.Poke("wait-for-cluster", "cluster-1"); err != nil {
			log.Println("poke:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`wait-for-cluster` has no `Triggers` at all — nothing walks a list on an
interval. A [reconcile](../glossary.md#reconcile) job needs a periodic
trigger unless you say otherwise, so `AllowUnscheduled: true` is the
explicit opt-out: nothing checks `cluster-1` unless something asks for it.
`rt.Ready()` blocks until registration has finished, so the goroutine waits
for that before asking.

## Run it

```sh
cd examples && go run ./guide/03-triggers
```

```
checking cluster-1 (attempt 1)
checking cluster-1 (attempt 2)
checking cluster-1 (attempt 3)
cluster is ready
```

## What happened

1. The first check happened the moment the [poke](../glossary.md#poke)
   landed — `checking cluster-1 (attempt 1)` — not on any timer.
   `wait-for-cluster` has no periodic trigger, so nothing else would have
   called it at all.
2. Returning `reconcile.CheckAgain{In: 500 * time.Millisecond}` produced
   exactly one more call, about half a second later: `checking cluster-1
   (attempt 2)`.
3. The same thing happened again, another half second on: `checking
   cluster-1 (attempt 3)`.
4. On that third attempt the function returned `nil` instead of
   `CheckAgain`. `cluster is ready` printed, and nothing checked
   `cluster-1` again.

## The principle

`reconcile.CheckAgain{In: d}` is not a retry counter and it is not an
error: returning it does not touch the backoff a real failure would
trigger. It says exactly one thing to converge — nothing is wrong, I am
just not done — and asks to be looked at again in `d`. See
[`reconcile.CheckAgain`](../reference/reconcile.md) for its exact shape.

A [poke](../glossary.md#poke) is the opposite direction: something outside
the function asks converge to look now. `CheckAgain` is the function asking
converge to look again, later, on its own terms — the two exist for
different callers, not different problems.

## Other shapes

There is a third way to [wake](../glossary.md#wake) `cluster-1`:
`reconcile.OnMessage` turns a stream of messages from elsewhere in your
system into wakes, the same way the schedule turns a list into wakes. A
message only ever says which ID to look at — never what to do — and
converge reacts by re-reading the truth about that ID itself, the same as
if the schedule or a poke had asked. The full signature is in
[the reconcile reference](../reference/reconcile.md).

## Try breaking it

Delete the `AllowUnscheduled: true,` line and run it again:

```
2026/08/24 18:45:22 reconcile: job "wait-for-cluster": no periodic trigger; set AllowUnscheduled to opt out of the schedule guarantee
exit status 1
```

No output at all — not even the first `checking cluster-1` line. A
reconcile job needs a periodic trigger unless `AllowUnscheduled: true` says
otherwise; without either, `rt.Run` refuses to start the job, the same way
it refused to start chapter 1's job when `Options.KV` was missing.
`wait-for-cluster` has no schedule and never will — it exists to be poked —
so `AllowUnscheduled` is the honest way to say that, rather than adding a
schedule the job doesn't need just to satisfy the check.

## A caveat

Every scheduled job keeps a record of when its schedule last fired. The
very first time — before that record exists — converge calls your function
immediately, which is what you saw in chapter 1. Once the record exists,
converge works out which scheduled moments have already gone by and waits
for the next one instead of firing right away.

Here that record lives in memory, so it never survives a restart: stop this
program and start it again and there is no record to find, so you are back
to the immediate-first-run case every time. Once you swap `inmem.NewKV()`
for something that outlives the process — [chapter 6](06-production.md)'s
Redis swap — that stops being true: restart your service in the middle of
an interval and converge finds the record, works out the interval hasn't
passed yet, and does not re-run the job just because the process did.

Next: [4. The other kind of job](04-worker.md) — work that only needs
doing once, for one thing that happened.
