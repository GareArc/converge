# 5. More than one copy

You deploy three copies of your service, the way chapter 1 assumed without
saying how. Register `rebuild-cache` — the job below, one function that
rebuilds an in-memory cache every second — on all three copies exactly as
chapters 1 through 4 taught, and with nothing deciding between them all three
would run it: three rebuilds a second for one cache, not one. This chapter is
the dial that decides how many of your copies actually do the work for a job
like this one, and why converge already set it for you without asking. By the
end you will have run the same job two different ways and watched the
difference in how many times it actually happened.

## The code

The whole program:

```go title=examples/guide/05-run-modes/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func replica(runs *atomic.Int64, lease converge.Lease, kv converge.KV, mode converge.RunMode) *converge.Runtime {
	rt, err := converge.New(converge.Options{Lease: lease, KV: kv})
	if err != nil {
		log.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "rebuild-cache",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			runs.Add(1)
			return nil
		}),
		RunMode:  mode,
		Triggers: []reconcile.Trigger{reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Second))},
	})
	if err != nil {
		log.Fatal(err)
	}
	return rt
}

func runTwoCopies(mode converge.RunMode) (int64, int64) {
	lease, kv := inmem.NewLease(), inmem.NewKV()
	var first, second atomic.Int64
	runtimes := []*converge.Runtime{
		replica(&first, lease, kv, mode),
		replica(&second, lease, kv, mode),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for _, rt := range runtimes {
		wg.Add(1)
		go func(rt *converge.Runtime) {
			defer wg.Done()
			if err := rt.Run(ctx); err != nil {
				log.Println(err)
			}
		}(rt)
	}
	wg.Wait()
	return first.Load(), second.Load()
}

func main() {
	a, b := runTwoCopies(converge.OnOneReplica)
	fmt.Printf("OnOneReplica: %d runs in total, %d of them on a single copy\n", a+b, max(a, b))

	a, b = runTwoCopies(converge.OnAllReplicas)
	fmt.Printf("OnAllReplicas: %d runs in total, %d on each copy\n", a+b, a)
}
```

`replica` builds one copy of your service the way chapter 1 did — its own
`converge.Runtime`, registering `rebuild-cache` through `reconcile.Register`
the way chapter 2 introduced — except this time two copies share the same
`Lease` and the same `KV`, standing in for the one shared Redis every real
deployment's copies would actually use. `runTwoCopies` starts both, gives
them two and a half seconds against a schedule that fires every second, and
waits for them to finish. `main` calls it twice: once under
`converge.OnOneReplica`, once under `converge.OnAllReplicas`, and prints how
many times `rebuild-cache` actually ran each time.

## Run it

```sh
cd examples && go run ./guide/05-run-modes
```

```
OnOneReplica: 3 runs in total, 3 of them on a single copy
OnAllReplicas: 6 runs in total, 3 on each copy
```

## What happened

1. Nothing printed for about two and a half seconds: the first
   `runTwoCopies` call, run under `converge.OnOneReplica`, doesn't print
   anything itself — it only counts.
2. Then the first line: `OnOneReplica: 3 runs in total, 3 of them on a
   single copy`. Three ticks happened in that window — roughly at zero, one,
   and two seconds — and every one of them landed on the same copy; the
   other copy's own count never left zero.
3. Nothing printed for another two and a half seconds, while the second
   call ran the same two copies again, this time under
   `converge.OnAllReplicas`.
4. Then the second line: `OnAllReplicas: 6 runs in total, 3 on each copy`.
   The same three ticks, over the same window — but now both copies ran
   `rebuild-cache` on every one of them: three runs each, six in total, not
   three.
5. `main` returned, and the process exited with status 0.

## The principle

Chapter 1's two `inmem` lines included a [lease](../glossary.md#lease):
converge's own record of which copy of your service is currently in charge
of a job. Under `converge.OnOneReplica` — the default for every
[reconcile](../glossary.md#reconcile) job, including `rebuild-cache` here —
every copy tries to take that lease when it starts. Whichever copy gets it
first holds it, runs the schedule, and renews it on a timer to keep holding
it; the other copy tries too, finds the lease already taken, and does not
run the schedule at all while that stays true — which is why its own count
sat at zero for the whole run, not because it saw the work already done,
but because it never got to look. That is the mechanism chapter 1 promised
and did not name: not a vote between copies, not any copy checking what the
others are doing, just one copy holding something the rest cannot get.

`converge.OnOneReplica` is one of three [run mode](../glossary.md#run-mode)
values, and it is the one you get by not choosing: leave `RunMode` unset on
a reconcile job, the way chapters 1 through 3 did, and that is what you
already had. `rebuild-cache` above sets it explicitly only so that `main` can
run the same job both ways. `converge.OnAllReplicas`, the one this chapter's
second run used, is the opposite choice — it skips the lease entirely, so
every copy runs the schedule on its own, with nothing coordinating between
them, which is exactly why the count doubled instead of staying flat.

## Other shapes

There is a third run mode, `converge.SplitAcrossReplicas`: instead of one
copy doing all the work, or every copy doing all of it, the work is
divided between them — each message goes to exactly one copy, chosen by
the queue itself, not by converge. In v1 that exists only on the worker
[surface](../glossary.md#surface) — which of converge's two kinds of job
this one is — where the queue's own consumer groups do the dividing. Set
`RunMode: converge.SplitAcrossReplicas` on a reconcile spec — including
`rebuild-cache` above — and converge refuses to register the job at all,
the same way it refused `wait-for-cluster` in chapter 3 without
`AllowUnscheduled`: there is no list here for converge to divide the way a
queue divides messages between consumers, so v1 does not offer it on this
surface.

`converge.OnAllReplicas` means something different again on the worker
surface (chapter 4) than it does here. There, every copy gets its own
delivery of every message straight from the queue, with nothing shared
between them, and that delivery is at-most-once: no dead-lettering, and no
retry. Registering a `Retry` policy alongside `OnAllReplicas` is itself a
registration error, the same shape as the `SplitAcrossReplicas` refusal
above; and if the handler does return an error, converge does not
redeliver the message or set it aside in a
[dead-letter](../glossary.md#dead-letter-dlq) queue — that copy simply gets
no further attempt at it. `OnAllReplicas` is for work each copy is meant to
do for itself, like a local cache; a message meant to happen exactly once,
like chapter 4's `send-welcome`, is the wrong job for it.

## Try breaking it

Take `replica` and force `converge.OnAllReplicas` regardless of what is
passed in, and give `rebuild-cache` something with a real consequence
instead of a counter: an invoice ledger standing in for a real database
row, incremented and printed on every run. Run two copies of that against
the same one-second schedule for two and a half seconds:

```
issuing invoice #1
issuing invoice #2
issuing invoice #4
issuing invoice #3
issuing invoice #5
issuing invoice #6
total invoices issued: 6
```

Three ticks, six invoices: on every tick, both copies ran the job and both
added to the same ledger, so whatever real system that ledger stood for
gets billed twice for every event it should have billed once. Nothing
crashed and nothing logged an error — this is a program working exactly as
told, giving a wrong answer. converge cannot tell this case apart from the
in-memory cache `rebuild-cache` stood for up to now, where every copy doing
its own work is correct; choosing the right run mode is how you tell it
apart. Notice, too, that in the run behind this transcript `#4` printed
before `#3` — two copies writing to the same thing with nothing
coordinating the order between them, not even that.

## A caveat

`converge.OnOneReplica` does not mean one copy is in charge forever,
without exception. The lease that holds it has a lifetime, renewed on a
timer for as long as the holder keeps running; if the holder stalls
mid-pass, or dies without releasing it, the lease simply stops being
renewed and expires on its own, and another copy picks it up on its next
attempt. For a moment around that handover, the previous holder can still
be finishing a run on the same ID the new holder is about to start on —
`OnOneReplica` makes duplicate work rare, not impossible. A handler still
has to be safe to run twice regardless of which run mode you chose;
[7. Stale writes](07-versions.md) is how you protect a write against
exactly that, for the cases where "probably fine" is not good enough.

Next: [6. Going to production](06-production.md) — replacing the two
in-memory lines chapter 1 started with, so the lease and the schedule's own
bookkeeping are shared across real, separate processes, not goroutines in
one.
