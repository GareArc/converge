# 1. A first job

You have something that needs doing every so often — syncing inventory with the warehouse,
expiring sessions, pulling a table back into line with an upstream — and you
have more than one copy of your service running. Cron gives you the "every so
often" and stops there: all your copies fire at once, and keeping that from
happening becomes your problem. converge gives you both halves. You write the
function and say how often; converge calls it, and converge decides which copy
of your service calls it. Run three copies of your service and one of them
does the work, not all three. By the end of this chapter you will have the
function running, with nothing installed.

## The code

The whole program:

```go title=examples/guide/01-first-job/main.go
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

	err = reconcile.Periodic(rt, "sync-inventory", reconcile.Every(2*time.Second), func(ctx context.Context) error {
		fmt.Println("syncing inventory with the warehouse")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Run it

```sh
cd examples && go run ./guide/01-first-job
```

```
syncing inventory with the warehouse
syncing inventory with the warehouse
syncing inventory with the warehouse
syncing inventory with the warehouse
```

## What happened

1. The first line appeared straight away: converge did not wait out the first
   interval before calling your function. That is what happens the first time
   a [job](../glossary.md#job) ever runs — after that converge works from a
   record of when the job last ran, which [chapter 6](06-production.md)
   puts somewhere it survives a restart.
2. Three more followed, one every two seconds — at roughly two, four and six
   seconds in.
3. At seven seconds the context deadline passed, `rt.Run` returned, and the
   shell got its prompt back with status 0. Four lines and not five because
   the deadline landed between ticks. Had `rt.Run` returned an error,
   `log.Fatal` would have printed it and exited 1 — which is what happens
   further down this page.

## The principle

The two `inmem` lines are converge's own bookkeeping: where it writes down
which job last ran when, and which copy of your service is currently in
charge. You never read or write it yourself. Here it is kept in memory, which
is why this example needs nothing installed — and why it only holds up inside
one process, because there is nothing there for a second copy of your service
to read.

Swapping those two lines for Redis is the whole of
[chapter 6](06-production.md): with one shared place to keep the bookkeeping,
every copy of your service reads the same answer to "who is in charge", and
the promise at the top of this page holds across the deployment rather than
inside one process. How converge picks the one, how to ask for all of them
instead, and why your function should still be safe to run twice: that is
[chapter 5](05-run-modes.md).

## Other shapes

`reconcile.Periodic` is the short form: one function, called on an interval,
looking after exactly one thing. The [reconcile](../glossary.md#reconcile) in
the name is converge's word for work that fixes drift — your function reads
how things actually are and puts them right, rather than being handed a piece
of work and told to do it. Most work of that kind looks after many things at
once, one per customer or one per product, and that is the general form:
[chapter 2](02-ids.md).

## Try breaking it

Delete the `KV: inmem.NewKV(),` line and run it again:

```
2026/08/24 16:57:56 reconcile: job "sync-inventory": Schedule needs Options.KV
exit status 1
```

No lines, no partial run. converge checks that everything registered has what
it needs the moment `rt.Run` starts, and stops there: with nowhere to write
down when this job last ran, it will not guess and start ticking anyway. The
`Lease:` line is checked the same way — delete that one instead and the
failure has the same shape, naming `Options.Lease`, because converge cannot
tell which copy of your service is in charge and will not let all of them run
the work by default.

In production this shows up as a process that exits at startup with one log
line naming the option it is missing — never as a job that silently never
runs.

## A caveat

`Every(2*time.Second)` is here so the example finishes while you are watching
it, and the seven-second deadline is there so it exits at all. Real work runs
on something more like `Every(time.Hour)`, and a real service sets no deadline:
you pass `rt.Run` a context that is cancelled when your process is asked to
shut down, and it blocks until then. Carry the two-second interval into
production by accident and you will call your function eighteen hundred times
an hour.

Next: [2. Many things to check](02-ids.md) — one function looking after
ten thousand things instead of one.
