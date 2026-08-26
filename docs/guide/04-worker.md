# 4. The other kind of job

Chapters 1 through 3 all answered the same question, in different shapes: is
everything as it should be? A schedule, or a [poke](../glossary.md#poke),
told your function which one thing to look at, and your function checked
the truth and fixed it if it needed fixing. This chapter answers a
different question — not "is everything as it should be", but "charge this
one order". A customer checked out once; the card should be charged once,
for that one checkout, not on some later pass that notices it hasn't
happened. By the end of this chapter you will have queued two of those and
watched converge deliver both, once each, with nothing installed.

## The code

The whole program:

```go title=examples/guide/04-worker/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

type ChargeOrder struct {
	OrderID string `json:"order_id"`
}

func main() {
	rt, err := converge.New(converge.Options{
		MQ:    inmem.NewMQ(),
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	chargeOrder := worker.NewTask[ChargeOrder]("charge-order", worker.TaskOpts{Queue: "payments"})

	err = worker.Handle(rt, chargeOrder, func(ctx context.Context, p ChargeOrder) error {
		fmt.Println("charging order", p.OrderID)
		return nil
	}, worker.HandleOpts{Concurrency: 1})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := worker.ProducerFrom(rt)
	if err != nil {
		log.Fatal(err)
	}
	for _, id := range []string{"ORD-4417", "ORD-4418"} {
		if err := chargeOrder.Enqueue(context.Background(), producer, ChargeOrder{OrderID: id}, worker.EnqueueOpts{}); err != nil {
			log.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`MQ: inmem.NewMQ()` is a third line chapters 1 through 3 had no use for: it
is where the [queue](../glossary.md#queue) itself lives. It is in memory here
like the two lines beside it, which is why this example still needs nothing
installed; [chapter 6](06-production.md) is where all three become Redis.

`chargeOrder` is a `worker.Task[ChargeOrder]`: a name, a payload type, and a
queue, agreed once and shared by whatever sends the message and whatever
handles it. `worker.Handle` registers the function that runs when a message
for `chargeOrder` arrives. `worker.ProducerFrom` gets you something that can
send messages on this runtime's own queues, and `chargeOrder.Enqueue` is how
you actually send one. `Concurrency: 1` keeps this example's two lines
printing in a fixed order — the default is 4, and at that setting either
line could come first.

## Run it

```sh
cd examples && go run ./guide/04-worker
```

```text
charging order ORD-4417
charging order ORD-4418
```

## What happened

1. Nothing printed the moment either `Enqueue` call returned — enqueuing
   only puts a message on the queue; nothing runs until `rt.Run` starts.
2. Both lines appeared together, right after `rt.Run` started:
   `ORD-4417` first, then `ORD-4418`, each printed once, in
   the order the two `Enqueue` calls happened.
3. Nothing printed again after that. Unlike chapters 1 through 3, nothing
   here walks a list every so often — a worker message is handled once, and
   once it's handled there's nothing left to hand it again.
4. At two seconds the deadline passed, `rt.Run` returned, and the shell got
   its prompt back with status 0 — the same clean exit as chapters 1
   through 3, just with nothing repeating in between.

## The principle

Chapters 1 through 3 all worked from something you could always re-read:
lose track of one round and the next round re-checks the same truth, so
nothing is actually lost. A worker message doesn't work that way —
`ChargeOrder{OrderID: "ORD-4417"}` doesn't point at something you can go
re-read later, it *is* the work: charge this order. Lose that message and
there is no later round that notices `ORD-4417` was never charged.
That's why converge treats it differently: if `chargeOrder`'s handler
returns an error, converge doesn't move on to the next message on the
[queue](../glossary.md#queue) — it redelivers the same one, with a delay
that grows each time it fails again. And if it keeps failing, converge
doesn't drop it either: it gets set aside as a
[dead-letter](../glossary.md#dead-letter-dlq), kept for a person to look at,
instead of thrown away.

## Outcomes

A handler can return more than success or failure. Returning `nil` means
the message is done. Returning an ordinary error means try again later,
with a growing delay between attempts. Two more values live in the `worker`
package, for the cases an error doesn't fit:

- `worker.Snooze{In: d}` — not yet, ask me again in `d`, without spending
  one of the message's retries.
- `worker.Discard{Reason: s}` — this message is not worth retrying; drop
  it.

The full outcome table — including what a panic does and how these
interact with visibility and the retry budget — is in the
[worker reference](../reference/worker.md).

## Try breaking it

Change the handler to return an error instead of `nil`, cap the retry
budget with `Retry: worker.RetryPolicy{MaxAttempts: 2}` so it stops
quickly, and enqueue only `ORD-4417` so there's nothing else
competing for attention. Run it again, giving it three seconds:

```text
charging order ORD-4417
charging order ORD-4417
```

Two attempts, not one and not more — `MaxAttempts: 2` bounded it exactly.
The first happened the moment `rt.Run` started, same as before. Failing it
did not crash the process or drop the message: converge redelivered it
after a short delay, ran the handler a second time, and then — the retry
budget spent — went quiet. Nothing prints a third time; the process runs
out its three seconds and exits with status 0, same as the working
version. `ORD-4417` didn't vanish, though — it's sitting in the
dead-letter queue this chapter's principle described, waiting for someone
to requeue it. Waiting only until this process exits, in this example: the
`MQ` here is `inmem`, so the dead-letter queue lives in memory alongside
it. [Chapter 6](06-production.md) puts all of this on Redis, which is what
makes "waiting for someone to requeue it" mean waiting for a person rather
than for the next line of `main`.

## A caveat

converge makes sure your handler is called at least once for every
message; it never silently drops one, but it may call your handler more
than once for the same message. If your process dies after charging the
card but before converge records that the message succeeded, converge
redelivers it, and your handler runs again for the same
`ChargeOrder{OrderID: "ORD-4417"}`. A handler can run twice for one
message, so make it safe to run twice — and this chapter's example is
deliberately the hard case. Charging a card twice is not a minor annoyance,
and "converge might call your handler again" is not a hypothetical. A real
`charge-order` handler earns that safety by sending the payment provider an
idempotency key derived from the order — `ORD-4417`, not a fresh UUID per
call — so the second charge is recognised as the first one and does
nothing. The handler here just prints a line, so it tolerates a second call
for free. Yours will not.

Next: [5. More than one copy](05-run-modes.md) — running three copies of
your service, and which one does the work.
