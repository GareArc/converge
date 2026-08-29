# When the message is the work

Ask the question again:

> **Can you write a query that lists everything still to be done, without
> reading the queue?**

"Send the welcome email to ada@example.com." No — nothing in your database
records that a welcome email is owed, so no query lists the ones still to
send. The message is the only copy of the work.

That is a [worker](../glossary.md#worker) job, and everything about it
follows from the message being irreplaceable. It is delivered at least once.
It is retried when your handler fails. And when it can no longer be tried it
is kept on the **shelf** — a durable store a person can read — rather than
dropped.

If you find yourself writing a worker job whose payload is just an ID, stop:
that is a [notification](../glossary.md#notification) and a
[reconcile](../glossary.md#reconcile) job wearing the wrong clothes.
Chapter 3 is the cheaper answer.

## A task is a contract

The sender and the handler live in different binaries. What keeps them from
drifting is a **task**: one declaration naming the work, the payload type,
and which version of that payload shape this is.

```go
var sendEmail = worker.NewTask[EmailJob]("send-email", worker.TaskOpts{})
```

Declare it once, in a package both sides import. The sending side calls
`sendEmail.Enqueue`, the side running the job calls
`worker.Handle(rt, sendEmail, ...)`, and both are typed on `EmailJob` —
change the struct and the compiler finds every side that has to change with
it. Nothing else has to be agreed between the two: no queue name, no route,
no serialisation format. Payloads are JSON unless you set `TaskOpts.Codec`.

The sending side has exactly two knobs of its own, both on
`worker.EnqueueOpts`. `Delay` holds the message back before anyone can pick
it up — minutes, not days; a due date belongs in a column of yours, swept by
a [reconcile](../glossary.md#reconcile) job, and a delay needs an MQ that
can publish one. `Headers` are yours to attach, and they reach your handler
through `worker.MetaFromContext(ctx)`; converge owns every name beginning
`converge.`, and `Enqueue` returns an error on a header that trespasses
there rather than quietly overwriting it. That is the entire producer-side
surface: everything else about the job is declared where the job runs.

`TaskOpts.Version` is how you handle a payload shape that has to change
while messages of the old shape are still in flight. A message whose version
does not match the handler's is not decoded and not guessed at — it is set
aside, which the rest of this chapter is about.

## Three ways to stop, and one way to fail

Your handler returns an `error`. Returning `nil` means done. Returning an
ordinary error means *this failed, try it again* — converge waits, then
redelivers, with the wait growing from one second up to fifteen minutes and
jittered, so a thousand messages that failed together do not come back in
lockstep.

Three other returns are not failures at all. They are ordinary values,
reports rather than errors, and they end the message's life deliberately:

| Return | Means | What happens |
| --- | --- | --- |
| `worker.Snooze{In: d}` | not yet — come back later | acknowledged now, republished after `d`, costs no retries |
| `worker.Discard{Reason: s}` | this never needs doing | forgotten, on purpose, right now |
| `worker.Shelve{Reason: s}` | a person needs to see this | stopped now and kept for inspection |

The difference between `Discard` and `Shelve` is the whole point of having
both. An unsubscribed recipient means the email must not be sent and nobody
needs to be told: `Discard`. A malformed address means somebody wrote a bug
or bad data got in, and throwing the evidence away would hide it: `Shelve`.

Here they are in one program — an ordinary success, a `Discard`, and a
`Shelve`:

```go title=examples/scenarios/a06-transactional-email/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

const (
	demoWindow = 2 * time.Second
	namespace  = "notifications"
)

var (
	errInvalidAddress = errors.New("mailer: invalid address")
	errUnsubscribed   = errors.New("mailer: unsubscribed")
)

type EmailJob struct {
	To       string `json:"to"`
	Template string `json:"template"`
}

type mailbox struct {
	unsubscribed map[string]bool

	mu   sync.Mutex
	sent []string
}

func (m *mailbox) send(_ context.Context, j EmailJob) error {
	if !strings.Contains(j.To, "@") {
		return errInvalidAddress
	}
	if m.unsubscribed[j.To] {
		return errUnsubscribed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, j.To)
	return nil
}

func (m *mailbox) delivered() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := slices.Clone(m.sent)
	slices.Sort(out)
	return out
}

var sendEmail = worker.NewTask[EmailJob]("send-email", worker.TaskOpts{})

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mq := inmem.NewMQ()

	rt, err := converge.New(converge.Options{
		Namespace: namespace,
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	mailer := &mailbox{unsubscribed: map[string]bool{"quiet@example.com": true}}

	err = worker.Handle(rt, sendEmail, func(ctx context.Context, j EmailJob) error {
		switch err := mailer.send(ctx, j); {
		case errors.Is(err, errInvalidAddress):
			return worker.Shelve{Reason: "invalid address"}
		case errors.Is(err, errUnsubscribed):
			return worker.Discard{Reason: "unsubscribed"}
		default:
			return err
		}
	}, worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 5}, Timeout: 15 * time.Second})
	if err != nil {
		return err
	}

	p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: namespace})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	for _, j := range []EmailJob{
		{To: "ada@example.com", Template: "welcome"},
		{To: "not-an-address", Template: "welcome"},
		{To: "quiet@example.com", Template: "welcome"},
	} {
		if err := sendEmail.Enqueue(ctx, p, j, worker.EnqueueOpts{}); err != nil {
			return err
		}
	}

	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("delivered: %v\n", mailer.delivered())

	shelf, err := worker.ShelfFrom(rt, sendEmail.Name())
	if err != nil {
		return err
	}
	shelved, err := shelf.List(context.Background())
	if err != nil {
		return err
	}
	for _, m := range shelved {
		fmt.Printf("shelved %s after %d attempt(s): %s\n", m.MessageID, m.Attempt, m.Reason)
	}
	return nil
}
```

```sh
cd examples
go run ./scenarios/a06-transactional-email
```

Timestamps and run durations are trimmed from the log lines below.

```text
INFO  converge: run completed job=send-email id=efb0aee1f8693bd39030b98cb8f276f1 attempt=1 outcome=succeeded
INFO  converge: run completed job=send-email id=85e758f35ddef510260d83ca8a86f82e attempt=1 outcome=discarded err="worker: discarded: unsubscribed"
ERROR converge: run completed job=send-email id=0627bafbddf8a9ba0cc653c970c22cc3 attempt=1 outcome=shelved err="worker: shelved: invalid address"
delivered: [ada@example.com]
shelved 0627bafbddf8a9ba0cc653c970c22cc3 after 1 attempt(s): invalid address
```

Three messages, three different endings, no failures logged as failures.
Note the `id` on each line: that is the **message ID**, minted once when the
message was enqueued, and it stays with this piece of work through every
retry and every republish. It is the one value to search your logs for when
somebody asks what happened to a particular email.

## Retries, and the two numbers that count them

`worker.HandleOpts.Retry` is a `worker.RetryPolicy`:

```go
worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 5}, Timeout: 15 * time.Second}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `MaxAttempts` | 25 | how many real tries before the message is set aside |
| `MinBackoff` | 1s | the first wait between tries |
| `MaxBackoff` | 15m | the ceiling that wait grows to |
| `MaxAge` | 24h | how old a message may get before it is set aside regardless |

`MaxAttempts` is measured against the **logical attempt**: how many times
converge has genuinely tried to do this message's work. That is also the
number your handler sees, via `worker.MetaFromContext(ctx)`.

Underneath it there is a second count, the **transport delivery** —
`Delivery.Attempt()`, how many times the queue itself has handed this
in-flight message out. The two disagree on purpose. When converge
republishes a message as a fresh one, the transport's count starts over at
one; the logical attempt carries on from where it was, because it is written
into the message's **envelope**, the `converge.*` headers converge attaches
at enqueue and folds back on every republish. You never write those
headers and you should never need to read them. When the two numbers
disagree, the logical attempt is the one that means anything.

This is exactly what makes `Snooze` free. A **snooze** acknowledges the
current delivery and republishes the message after your delay, folding the
delivery back into the envelope so the logical attempt does not move. A
message can snooze all day without spending a retry — which is why a snooze
is bounded by `MaxAge` and never by `MaxAttempts`. There is one bound worth
knowing: converge honours the delay you asked for on the first ten snoozes
of a message and applies its own growing backoff after that, so a handler
that snoozes forever slows down instead of spinning.

Webhook delivery is the case this was built for: the merchant's endpoint
says 429 and tells you when to come back, and that is not a failure.

```go
err = worker.Handle(rt, deliverWebhook, func(ctx context.Context, w Webhook) error {
    resp, err := hooks.post(ctx, w)
    if err == nil && resp.StatusCode == http.StatusTooManyRequests {
        return worker.Snooze{In: resp.RetryAfter}
    }
    return err
}, worker.HandleOpts{
    RateLimit: converge.Rate{Events: 50, Per: time.Second},
    Retry:     worker.RetryPolicy{MaxAge: 24 * time.Hour},
})
```

`RateLimit` is a job-wide ceiling on how often the handler is entered — 50
per second across this job, not per merchant. The whole program is
[`examples/scenarios/a07-webhook-delivery/main.go`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a07-webhook-delivery/main.go).

`Timeout` on a worker job is the same **time limit** as everywhere else —
how long one run may take before its context is cancelled — and it does one
extra thing here: converge derives from it how long the transport waits
before handing the message to somebody else, so a cancelled run is not
redelivered before the cancellation has been noticed. Leave it unset and
that window is five minutes.

## The shelf

The shelf is where a message goes when converge will not try it again. There
are exactly six ways to arrive:

| Reason | Cause |
| --- | --- |
| `max attempts` | the retry budget ran out |
| `max age` | the message got older than `MaxAge` |
| `schema version` | the payload's version did not match the handler's |
| `undecodable` | the payload would not decode |
| `wrong surface` | the handler returned `reconcile.CheckAgain`, which belongs to the other kind of job |
| *your own string* | the handler returned `worker.Shelve{Reason: ...}` |

Nothing leaves the shelf on its own. A requeue is a deliberate act by a
person, which is the point: a message on the shelf is evidence, and it stays
evidence until somebody has looked at it.

Each **shelved message** keeps its payload, its headers, the reason it
stopped, and when that happened:

```go
shelf, err := worker.ShelfFrom(rt, sendEmail.Name())
if err != nil {
    return err
}
shelved, err := shelf.List(context.Background())
```

`Shelf` has five verbs — `List`, `Get`, `Requeue`, `Purge`, and `PurgeAll`.
`Requeue` republishes the message as a fresh one and deletes the record: the
message ID survives, so you can still follow it through the logs, but the
retry budget and the age clock both start over. Fix the endpoint, then
requeue, and the message has its whole life ahead of it again.

The shelf lives in `KV`, which is why a worker job that can be shelved needs
one. [Chapter 6](06-production.md) shows requeueing a real message and
reading how deep the shelf is.

## Next

You have now seen both kinds of job. [Chapter 5](05-run-modes.md) is the one
declaration they share: which of your replicas actually runs the thing.
