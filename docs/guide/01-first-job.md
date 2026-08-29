# Your first job

A **job** is one piece of background work you hand to converge: a function,
a name you choose, and two declarations — when it runs, and what it is
about. converge takes it from there, on every replica of your service.

There are two kinds of job. Which kind you have is not a matter of taste,
and you do not have to know the library to decide it.

## One question decides everything

> **Can you write a query that lists everything still to be done, without
> reading the queue?**

**Yes — you want [reconcile](../glossary.md#reconcile).** The truth already
lives in your own store, and that query is how the job finds its work: it
walks the store on a **schedule**, calls your function once per thing, and
your function makes the world match. A message can only ever say *look at
this one sooner*. You can lose every message converge sends and the job
still comes out right, because the next **sweep** runs the same query.

**No — the message is the work.** Something happened, a side effect has to
follow, and no query recovers what it was: send this receipt, deliver this
webhook. converge keeps the message, hands it to your function, and hands
it over again if your function fails. When the retries are spent it puts
the message on the **shelf** — a durable store where a person can look at
it — rather than dropping it. That kind of job is built from a **task**, a
typed contract shared by the code that sends the message and the code that
handles it, and [chapter 4](04-worker.md) is about it.

The same question from the other side is *if this message were lost, would
anything be wrong?* — if the query exists, nothing would be, because the
next run finds the same rows. Answer it first. It settles what starts a run,
what your function is handed, and what a failure costs.

## A job that runs every night

Billing generates invoices at 00:05 Tokyo time. Can you list the accounts
still to be invoiced without reading a queue? Yes — they are the rows with
no invoice for this month, and they are still there whether or not anything
told you it was 00:05. So this is a reconcile job.

Here is the whole program.

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const demoWindow = 2 * time.Second

type ledger struct {
	mu     sync.Mutex
	due    []string
	issued []string
}

func newLedger(due ...string) *ledger { return &ledger{due: due} }

func (l *ledger) generateDueInvoices(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.issued = append(l.issued, l.due...)
	l.due = nil
	return nil
}

func (l *ledger) invoiced() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.issued)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return err
	}

	rt, err := converge.New(converge.Options{
		Namespace: "billing",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	billing := newLedger("acct-1001", "acct-1002", "acct-1003")

	err = reconcile.Periodic(rt, "generate-invoices",
		reconcile.Cron("5 0 * * *", reconcile.CronOpts{Location: tokyo}),
		billing.generateDueInvoices,
		reconcile.PeriodicOpts{Timeout: 30 * time.Minute})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("invoices issued: %v\n", billing.invoiced())
	return nil
}
```

Run it and it logs this, with timestamps and run durations trimmed.

```text
INFO converge: lease changed job=generate-invoices held=true
INFO converge: run completed job=generate-invoices id="" attempt=1 outcome=succeeded
INFO converge: lease changed job=generate-invoices held=false
invoices issued: [acct-1001 acct-1002 acct-1003]
```

It ran immediately, not at 00:05. converge had no record of ever having run
this job, so it ran it once on startup and then went back to waiting for
00:05. That rule has no options: if one or more scheduled times passed while
the job was not running, the job runs once when it comes back, and then
carries on as normal.

## Reading the program

**`converge.New`** is where converge gets everything it needs from outside
your process — `MQ`, `Lease`, `KV`, and an `Observer` for events. The
`inmem` package supplies all four in process, which is why this program
needs nothing installed. [Chapter 6](06-production.md) swaps in Redis
without touching the job.

**`reconcile.Periodic`** registers the job under the name
`generate-invoices`, with the **schedule** it runs on, the function, and its
options. The name is not decoration: it is how other code addresses this
job, and it is what appears in every log line and every metric. `Namespace`
scopes it, so two services on one Redis can each have a
`generate-invoices` without colliding.

**`reconcile.Cron("5 0 * * *", reconcile.CronOpts{Location: tokyo})`** is
the schedule. Five cron fields, in a location you name; `reconcile.Every(d)`
is the other form. There is exactly one schedule per job and every reconcile
job needs one, because the schedule is the part that makes the job correct.

**`Timeout: 30 * time.Minute`** is the job's **time limit**: how long one
run may take before converge cancels the context it handed your function. A
call that hangs on a dead dependency gives the job back instead of holding
it forever. Leave it unset and a run may take as long as it takes — that is
what zero means here, not "no time at all".

**`rt.Run(ctx)`** starts every registered job and blocks until `ctx` is
cancelled. It returns `nil` on a clean shutdown; a non-`nil` return is
always a real failure, so it is safe to treat as one.

You did not say where this job runs, so converge ran it on **one replica**.
Start four copies of this service and 00:05 produces one set of invoices,
not four. Saying it should run on **all replicas** instead is one field, and
[chapter 5](05-run-modes.md) is about when you would want that.

## How other code reaches a job

Code elsewhere in your system — an HTTP handler, a CLI, another service —
can say exactly two things to a job, and nothing else:

- **`Notify`** — *look at this one sooner.*
  [Chapter 3](03-notifications.md).
- **`Enqueue`** — *do this.* [Chapter 4](04-worker.md).

Both address the job through the value you declared it with — a
`reconcile.Job` or a `worker.Task` — never through a string. A
[notification](../glossary.md#notification) lands on the job's
**notifications**, which may be lost; a worker message
lands on the task's **queue**, which may not. converge derives both names
unless you declare one, and nothing else about a job — how often it runs,
how long a run may take, when it stops — can be set from outside. All of it
is declared where the job is registered, which is the file you just read.

## Next

[Chapter 2](02-ids.md) keeps the same shape and gives one job ten thousand
things to look after, one at a time.
