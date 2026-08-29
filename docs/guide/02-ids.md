# One job, many things

Chapter 1's job had nothing to look after but itself: one function, one run,
nothing to name. Most real [reconcile](../glossary.md#reconcile) work is not
like that. You have ten thousand orders, or every merchant, or every tenant,
and each one needs the same treatment separately.

That is what an **ID** is: the name of one unit of reconcile work — one
order, one merchant, one tenant. A job responsible for ten thousand orders
has ten thousand IDs, and converge treats each of them on its own: its own
run, its own failure, its own retry.

## Jobs are fixed, IDs come and go

Jobs are written in code and registered before `rt.Run`. You cannot add or
remove one while the process is running, because a job that appeared on one
replica would be invisible to the others.

An ID is the opposite, and it is the only thing in converge that starts and
stops by itself. It appears when it appears in your data, is reconciled for
as long as it is there, and leaves when your data stops listing it. Nothing
has to be registered, torn down, or cleaned up.

This is worth internalising, because it dissolves a whole category of
question. "I need a job per customer" is one job whose IDs are customers.
"I need a one-off job that runs until tenant X is migrated" is the ID `X` in
a static migration job — it stops being listed when it is done, and the job
outlives it. You do not need a job factory.

## The full form

`reconcile.Periodic` is a shorthand. The full form is
`reconcile.Register` with a `reconcile.Spec`, and here it is expiring
unpaid orders thirty minutes after checkout.

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

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow      = 2 * time.Second
	statusPending   = "pending"
	statusCancelled = "cancelled"
)

type orderBook struct {
	now func() time.Time

	mu     sync.Mutex
	placed map[string]time.Time
	state  map[string]string
}

func newStore(now func() time.Time) *orderBook {
	return &orderBook{now: now, placed: map[string]time.Time{}, state: map[string]string{}}
}

func (b *orderBook) create(id string) { b.placeAt(id, b.now()) }

func (b *orderBook) placeAt(id string, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.placed[id] = at
	b.state[id] = statusPending
}

func (b *orderBook) status(id string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state[id]
}

func (b *orderBook) unpaidOlderThan(age time.Duration) func(ctx context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) {
		cutoff := b.now().Add(-age)
		b.mu.Lock()
		defer b.mu.Unlock()
		var stale []string
		for id, at := range b.placed {
			if b.state[id] == statusPending && at.Before(cutoff) {
				stale = append(stale, id)
			}
		}
		slices.Sort(stale)
		return stale, nil
	}
}

func (b *orderBook) cancelIfUnpaid(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state[id] == statusPending {
		b.state[id] = statusCancelled
	}
	return nil
}

func expireUnpaidOrders(orders *orderBook) reconcile.Spec {
	return reconcile.Spec{
		Job:       reconcile.NewJob("expire-unpaid-orders", reconcile.JobOpts{}),
		Reconcile: func(ctx context.Context, id reconcile.ID) error { return orders.cancelIfUnpaid(ctx, string(id)) },
		Triggers: []reconcile.Trigger{reconcile.Schedule(
			reconcile.StringIDs(orders.unpaidOlderThan(30*time.Minute)), reconcile.Every(time.Minute))},
		Timeout: 10 * time.Second,
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
		Namespace: "shop",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	store := newStore(time.Now)
	store.placeAt("o-1001", time.Now().Add(-45*time.Minute))
	store.create("o-1002")

	if err := reconcile.Register(rt, expireUnpaidOrders(store)); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("o-1001 placed 45m ago: %s\n", store.status("o-1001"))
	fmt.Printf("o-1002 placed just now: %s\n", store.status("o-1002"))
	return nil
}
```

Running it prints:

```text
o-1001 placed 45m ago: cancelled
o-1002 placed just now: pending
```

The important line is the one that decides what this job is about:

```go
Triggers: []reconcile.Trigger{reconcile.Schedule(
    reconcile.StringIDs(orders.unpaidOlderThan(30*time.Minute)), reconcile.Every(time.Minute))},
```

A **trigger** is a source of IDs to look at. `reconcile.Schedule` is the one
every reconcile job must have: once per **cadence** period — here
`reconcile.Every(time.Minute)` — it performs a **sweep**, one walk of your
ID source that queues every ID the source yields. A sweep only queues work;
your `Reconcile` function runs afterwards, once per ID.

Notice what the job does *not* know. It does not know that `o-1001` exists,
it does not remember that it cancelled it, and nothing had to tell it. Your
query is the source of truth; converge asks it again every minute.

That is also why `cancelIfUnpaid` checks the status before writing. Your
function will be called for the same ID more than once — after a retry,
after a restart, or simply because your query still lists it — and converge
does not promise otherwise. Being safe to run twice is your job; it is the
only thing converge asks of your function.

## Where IDs come from

An ID source is a function converge calls at the start of every sweep. There
are four ways to build one, and they differ only in the shape of your query:

| Constructor | Your function returns | Use when |
| --- | --- | --- |
| `reconcile.StringIDs` | `[]string` | your query already returns strings |
| `reconcile.IDs` | `[]reconcile.ID` | you build IDs yourself |
| `reconcile.IDsByPage` | one page plus a cursor | the list is too big to hold |
| `reconcile.SingleID` | nothing | there is only one thing to do — this is what `Periodic` uses |

`IDsByPage` is the one to reach for on anything unbounded. converge calls it
with the empty cursor, then with whatever cursor you returned, until you
return an empty one, and it queues each page as it arrives rather than
waiting for the whole list. It keeps your cursor in `KV` between pages, so a
sweep interrupted by a restart resumes where it stopped instead of starting
over, and a page that returns an error is retried with the same cursor
rather than skipped.

## Doing several at once

Provisioning every tenant over SCIM is the same shape as expiring orders,
with a paged source and a slow remote call per tenant:

```go
err = reconcile.Register(rt, reconcile.Spec{
    Job:         reconcile.NewJob("scim-provision", reconcile.JobOpts{}),
    Reconcile:   scim.reconcileTenant,
    Triggers:    []reconcile.Trigger{reconcile.Schedule(reconcile.IDsByPage(tenants.withSCIM), reconcile.Every(5*time.Minute))},
    Concurrency: 16,
    Timeout:     time.Minute,
})
```

`Concurrency` is how many IDs this job may be running at once **on this
replica**. It defaults to 1, which is why chapter 1's job and the order
expiry above run strictly one at a time. Raise it when the work is
per-ID and mostly waiting on something else.

`Concurrency` is a per-replica number by design. converge does not split one
reconcile job's IDs across your replicas — that is a deliberate non-goal,
not a missing feature, and [chapter 5](05-run-modes.md) explains what it
buys you.

## When a run fails, and when it is not finished

Return an error and that ID is a **failing ID**: it waits before being tried
again, and each consecutive failure lengthens the wait, from one second up
to a ceiling of fifteen minutes. It is a ceiling and not a bench — a thing
that has been broken for a week still costs a handful of calls an hour and
never stops being retried. Other IDs are unaffected; one bad merchant does
not stop the other nine thousand.

Sometimes a run did not fail, it just is not finished — you asked Kubernetes
to apply a namespace and it is still coming up. Return
`reconcile.CheckAgain` instead of an error, and converge comes back to that
ID after the delay you name without counting a failure against it:

```go
Reconcile: func(ctx context.Context, id reconcile.ID) error {
    ready, err := k8s.apply(ctx, desired.of(id))
    if err != nil || ready {
        return err
    }
    return reconcile.CheckAgain{In: 15 * time.Second}
},
```

`CheckAgain` is honest about its bound: converge honours your delay for the
first ten deferrals of an ID in a row and starts spacing them out after
that, so a thing that will never be ready costs you less and less rather
than the same forever.

## When the intent moves under you

If what an ID is *supposed* to look like can change while your function is
mid-run, point `Spec.Versions` at a `reconcile.VersionSource` — a counter
you already have, usually a column that moves whenever somebody edits that
ID. converge stores no versions of its own.

Two things follow. converge reads the counter before your run and again
after it, and if it moved, the run does not count as done — the ID is
reconciled again, because whatever your function decided was decided from
state that has since changed. And you can carry the counter into your own
conditional write, then return `reconcile.ErrOutdated` when the database
refuses it; converge treats that as a deferral rather than a failure, so a
lost race costs a re-run and not a place in the backoff queue. That is what
makes a one-time backfill re-runnable.

## Next

The job above notices a cancelled order within a minute. If a minute is too
long, [chapter 3](03-notifications.md) shows how the rest of your system
tells it to look sooner.
