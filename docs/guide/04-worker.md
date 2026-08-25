# 4. The other kind of job

Chapters 1 through 3 all answered the same question, in different shapes: is
everything as it should be? A schedule, or a [poke](../glossary.md#poke),
told your function which one thing to look at, and your function checked
the truth and fixed it if it needed fixing. This chapter answers a
different question — not "is everything as it should be", but "send this
person their email". A customer signed up once; the welcome email should go
out once, for that one signup, not on some later pass that notices it's
missing. By the end of this chapter you will have queued two of those and
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

type Welcome struct {
	Email string `json:"email"`
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

	sendWelcome := worker.NewTask[Welcome]("send-welcome", worker.TaskOpts{Queue: "email"})

	err = worker.Handle(rt, sendWelcome, func(ctx context.Context, p Welcome) error {
		fmt.Println("sending welcome email to", p.Email)
		return nil
	}, worker.HandleOpts{Concurrency: 1})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := worker.ProducerFrom(rt)
	if err != nil {
		log.Fatal(err)
	}
	for _, addr := range []string{"ada@example.com", "grace@example.com"} {
		if err := sendWelcome.Enqueue(context.Background(), producer, Welcome{Email: addr}, worker.EnqueueOpts{}); err != nil {
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

`sendWelcome` is a `worker.Task[Welcome]`: a name, a payload type, and a
queue, agreed once and shared by whatever sends the message and whatever
handles it. `worker.Handle` registers the function that runs when a message
for `sendWelcome` arrives. `worker.ProducerFrom` gets you something that can
send messages on this runtime's own queues, and `sendWelcome.Enqueue` is how
you actually send one. `Concurrency: 1` keeps this example's two lines
printing in a fixed order — the default is 4, and at that setting either
line could come first.

## Run it

```sh
cd examples && go run ./guide/04-worker
```

```
sending welcome email to ada@example.com
sending welcome email to grace@example.com
```

## What happened

1. Nothing printed the moment either `Enqueue` call returned — enqueuing
   only puts a message on the queue; nothing runs until `rt.Run` starts.
2. Both lines appeared together, right after `rt.Run` started:
   `ada@example.com` first, then `grace@example.com`, each exactly once, in
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
`Welcome{Email: "ada@example.com"}` doesn't point at something you can go
re-read later, it *is* the work: send this address a welcome email. Lose
that message and there is no later round that notices ada never got one.
That's why converge treats it differently: if `sendWelcome`'s handler
returns an error, converge doesn't move on to the next message on the
[queue](../glossary.md#queue) — it redelivers the same one, with a delay
that grows each time it fails again. And if it keeps failing, converge
doesn't drop it either: it gets set aside as a
[dead-letter](../glossary.md#dead-letter), kept for a person to look at,
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
quickly, and enqueue only `ada@example.com` so there's nothing else
competing for attention. Run it again, giving it three seconds:

```
sending welcome email to ada@example.com
sending welcome email to ada@example.com
```

Two attempts, not one and not more — `MaxAttempts: 2` bounded it exactly.
The first happened the moment `rt.Run` started, same as before. Failing it
did not crash the process or drop the message: converge redelivered it
after a short delay, ran the handler a second time, and then — the retry
budget spent — went quiet. Nothing prints a third time; the process runs
out its three seconds and exits with status 0, same as the working
version. `ada@example.com` didn't vanish, though — it's sitting in the
dead-letter queue this chapter's principle described, waiting for someone
to requeue it.

## A caveat

converge redelivers a worker message at least once — never fewer times
than it takes to get an acknowledgment, sometimes more. If your process
dies after sending the email but before converge records that the message
succeeded, converge redelivers it, and your handler runs again for the
same `Welcome{Email: "ada@example.com"}`. A handler can run twice for one
message, so make it safe to run twice: sending the same welcome email
twice is a minor annoyance, and worth checking for; charging a card twice
is not, and worth designing against. `sendWelcome`'s handler here just
prints a line, so it happens to tolerate that without any extra care —
real handlers usually need to earn it.

Next: [5. Run modes](05-run-modes.md) — running three copies of your
service, and which one does the work.
