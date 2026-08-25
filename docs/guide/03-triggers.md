# 3. Reacting to events

Chapters 1 and 2 got their work from exactly one place: the schedule. That
schedule was a **trigger** — converge's word for anything that can say "look
at this ID, now". A schedule is the trigger you already know from cron. This
chapter adds the other one you probably already have in your system: a
message on a queue, published by whatever code knows that something changed.
By the end you will have one job woken two different ways, and you will have
watched the tempting shortcut that quietly breaks it.

This is the first chapter that needs something installed — a message has to
come from somewhere, and a real queue is the honest way to show it.

## The code

The whole program:

```go title=examples/guide/03-triggers/main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

func inboundKey(sku reconcile.ID) string {
	return "warehouse:" + string(sku) + ":inbound"
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	mq := convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{})

	rt, err := converge.New(converge.Options{
		Namespace: "guide-03",
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	skus := func(ctx context.Context) ([]string, error) {
		return []string{"SKU-1001", "SKU-1002"}, nil
	}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "sync-inventory",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			inbound, err := rdb.Get(ctx, inboundKey(id)).Int()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if inbound > 0 {
				fmt.Printf("%s: pallets still inbound: %d\n", id, inbound)
				if err := rdb.Decr(ctx, inboundKey(id)).Err(); err != nil {
					return err
				}
				return reconcile.CheckAgain{In: 500 * time.Millisecond}
			}
			fmt.Printf("%s: inventory matches the warehouse\n", id)
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.OnMessage("stock-events", reconcile.IDFromJSONField("sku"), reconcile.OnMessageOpts{MQ: mq}),
			reconcile.Schedule(reconcile.StringIDs(skus), reconcile.Every(5*time.Second)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	go func() {
		<-rt.Ready()
		select {
		case <-time.After(800 * time.Millisecond):
		case <-ctx.Done():
			return
		}
		if err := rdb.Set(ctx, inboundKey("SKU-1001"), 3, time.Minute).Err(); err != nil {
			log.Println("seed:", err)
			return
		}
		payload, err := json.Marshal(map[string]string{"sku": "SKU-1001"})
		if err != nil {
			log.Println("marshal:", err)
			return
		}
		if err := mq.Publish(ctx, "stock-events", converge.Message{Payload: payload}); err != nil {
			log.Println("publish:", err)
		}
	}()

	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

The `Triggers` list is the part to look at. It has two entries, and they are
the same kind of thing: each turns an outside event into a
[wake](../glossary.md#wake) — `reconcile.OnMessage` from a queue,
`reconcile.Schedule` from a list. Both feed the one `Reconciler` below them,
and nothing in that [reconcile](../glossary.md#reconcile) function knows or
cares which of them asked.

The goroutine at the bottom is standing in for the rest of your system —
the code that books a delivery into the warehouse and announces it. In a
real service that is a different program.

## Run it

This chapter needs a Redis to talk to. In a second terminal:

```sh
docker run --rm --name converge-guide -p 6379:6379 redis:7-alpine
```

Then:

```sh
cd examples && go run ./guide/03-triggers
```

```text
SKU-1001: inventory matches the warehouse
SKU-1002: inventory matches the warehouse
SKU-1001: pallets still inbound: 3
SKU-1001: pallets still inbound: 2
SKU-1001: pallets still inbound: 1
SKU-1001: inventory matches the warehouse
```

## What happened

1. The schedule fired the moment the program started — the immediate first
   fire from chapters 1 and 2 — and swept both SKUs. Neither had anything
   inbound, so both printed that they matched, and that was that.
2. About 800ms in, the goroutine wrote `3` into the warehouse's record for
   `SKU-1001` and published one message onto the `stock-events` queue.
3. `reconcile.OnMessage` read that message, pulled `SKU-1001` out of its
   `sku` field, and [woke](../glossary.md#wake) that one ID. `SKU-1002` was
   not woken: nothing happened to it, so nothing needed to look at it.
4. The reconciler ran and asked the warehouse what was true right now:
   three pallets inbound. It booked one in and returned
   `reconcile.CheckAgain{In: 500 * time.Millisecond}` — not done yet, look
   again shortly.
5. Two more rounds, half a second apart, each one asking the warehouse again
   rather than remembering anything. When the answer came back zero the
   function returned `nil` and the loop stopped.
6. At four seconds the deadline passed and `rt.Run` returned `nil`.

## The principle

`Triggers` is the primary surface. It is a list of the ways work can arrive,
declared once, next to the job — and a message trigger sits in it as an
equal to the schedule, not as a special case bolted on. That matters because
of what a wake actually carries: **an ID and nothing else.** The message says
`SKU-1001`; it does not say what changed, what to do about it, or what the
new quantity is. converge hands that ID to your reconciler, and your
reconciler goes and asks the warehouse.

That is why the same function serves both triggers without a branch in it.
It is also why losing a message is survivable — more on that in "A caveat".

`CheckAgain` is the supplementary half, and it is not a trigger: it is the
reconciler saying "I looked, I am not finished, look again in `d`". It is not
an error, so it does not touch the backoff a real failure would trigger — for
a bounded number of consecutive returns. After ten in a row converge decides
the loop is not converging and applies the failure backoff curve anyway,
reporting `BackoffFallback`. Ask for less than 250ms and converge raises it;
the shortest gap it will actually leave is between 250ms and 375ms.

The thing worth copying from this example is what is *not* in it. Every
decision the reconciler makes comes from a value it read on that pass. It
remembers nothing between calls. That is what makes it safe to wake from two
different triggers, safe to run twice, and safe to kill halfway.

## Other shapes

A [poke](../glossary.md#poke) is a third way in, and it is the odd one out:
the schedule and the queue are both part of your system's normal running,
while a poke is a human or an operator tool reaching in from outside to say
"look at this one now". It takes the same path and ends in the same
reconciler. Chapter 9 pokes a running job over HTTP.

`OnMessageOpts` also carries a `Delivery`, which decides whether every
replica sees each message or only one of them does. Left unset it follows
the job's [run mode](../glossary.md#run-mode), which is
[chapter 5](05-run-modes.md). And if neither a
schedule nor a queue fits — you have a Postgres `LISTEN`, a filesystem
watcher, a Kafka consumer you already run — `reconcile.Trigger` is a
one-method interface you can implement yourself;
[Scenario E](../cookbook/scenario-e-custom-trigger.md) does exactly that.

## Try breaking it

There is a shortcut that looks like it does the same thing and does not.
Instead of asking the warehouse, keep the count in a variable. Add
`attempts := 0` above `reconcile.Register`, and replace the body of the
reconciler with this:

```go
attempts++
fmt.Printf("%s: attempt %d\n", id, attempts)
if attempts < 3 {
	return reconcile.CheckAgain{In: 500 * time.Millisecond}
}
fmt.Printf("%s: inventory matches the warehouse\n", id)
return nil
```

It compiles, it runs, and it prints something that looks close enough to be
convincing:

```text
SKU-1001: attempt 1
SKU-1002: attempt 2
SKU-1001: attempt 3
SKU-1001: inventory matches the warehouse
SKU-1002: attempt 4
SKU-1002: inventory matches the warehouse
SKU-1001: attempt 5
SKU-1001: inventory matches the warehouse
```

Read the numbers. There is one counter and two SKUs, so `SKU-1002`'s first
ever look is "attempt 2". Neither SKU gets three passes: `SKU-1001` declares
itself finished on its second, `SKU-1002` on its second. And the message —
the whole point of the chapter, the wake that means something actually
changed — arrives when the counter is already past three, so it prints
"attempt 5" and does no work at all.

The counter measures the process, not the SKU. Every wake shares it, so it
answers a question nobody asked. It is also gone when the process restarts,
and wrong the moment a second replica has its own copy.

Now do it the other way around. Run the real version again and kill it with
Ctrl-C after the `pallets still inbound: 2` line, then ask the warehouse
what it thinks:

```sh
docker exec converge-guide redis-cli GET warehouse:SKU-1001:inbound
```

```text
1
```

One pallet still to book in — the work that was left is still there, written
down in the place that owns it. Start the program again and it picks up from
one, because the count was never in the program to lose.

## A caveat

The message is acked as soon as it becomes a wake, before your reconciler
runs — and if `IDFromJSONField` cannot find a `sku` field, the message is
discarded outright, reported as a `WakeDiscarded` with reason
`DiscardUndecodable`, and acked just the same. Either way it is gone. If the
reconcile that followed then fails, no message comes back to retry it.

This sounds alarming and is not, but only because of how the rest of the
chapter is built. A wake carries no information, so losing one loses no
information: the schedule sweeps every SKU on its own timetable and re-reads
the same truth the message would have pointed at. The queue makes converge
react in milliseconds instead of on the next sweep. It is an accelerator, not
the system of record.

That is the trade to keep in mind when you reach for a message trigger: it
buys latency, and it is never the thing that guarantees the work happens.
If you catch yourself needing a message to be processed exactly once, with
retries and a [dead-letter](../glossary.md#dead-letter-dlq) queue, you do not
want a reconcile trigger — you
want the other surface, which is [chapter 4](04-worker.md).

Next: [4. The other kind of job](04-worker.md) — work that only needs doing
once, for one thing that happened.
