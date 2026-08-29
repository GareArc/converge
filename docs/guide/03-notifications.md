# Telling a job to look sooner

A [reconcile](../glossary.md#reconcile) job is already correct without this
chapter. The [schedule](../glossary.md#schedule) walks every
[ID](../glossary.md#id) once per period, so anything that changed is picked
up by the next [sweep](../glossary.md#sweep) at the latest. What the
schedule is not is *fast*: chapter 2's job notices a cancelled order within
a minute, and the operator who just fixed a merchant's credentials would
rather not wait a minute to find out whether it worked.

A **notification** closes that gap. It is a message that says *look at this
ID sooner*, and it carries nothing else — no instructions, no payload, no
authority. If it arrives, that ID runs at the next opportunity instead of
waiting for the sweep. If it is lost, the sweep gets there anyway.

That is the whole bargain, and it is why notifications are allowed to be
cheap: **lossy by contract, deduplicated, and unordered.** Nothing in
converge treats a lost notification as an incident, because the schedule
already covers the case.

They do one thing beyond hurrying: a notification **resets that ID's
backoff**. An ID that has been failing is serving out a wait computed before
you knew anything new. A notification is new information — you fixed the
merchant's credentials, the operator retried the upload — so the ID runs at
once rather than serving out a penalty that is now meaningless. That reset
is the only bypass in the library. There is no separate operator verb, and
there is nothing else to learn.

## A notification has no verb

If you catch yourself wanting to put a verb on a notification — `"type":
"rotate"`, `"action": "delete"` — one of two things is true. Either the verb
is derivable from your store and you have not written that column yet: the
workspace exists or it does not, the credential is expired or it is not, and
the function can look. Or the verb really is an instruction nothing can
re-derive, and what you have is a [worker](../glossary.md#worker) task you
have not declared yet. A notification carries an ID and nothing else, and
that is not a limitation to work around: it is what makes losing one free.

## A second trigger

Every job has one **inbox**: one place it receives things, named after the
job and namespaced by the library. You never name it and nobody routes
anything to it — senders address the job by the name you registered.

A reconcile job does not read its inbox unless you say so, and you say so
with a second [trigger](../glossary.md#trigger):

```go
Triggers: []reconcile.Trigger{
    reconcile.Schedule(reconcile.IDsByPage(merchants.page), reconcile.Every(15*time.Minute)),
    reconcile.Notifications(),
},
```

Triggers all feed one deduplicated queue of pending IDs, so a schedule and a
stream of notifications for the same ID collapse into one run rather than
racing. `reconcile.Notifications` takes no configuration because there is
nothing to configure: it reads the job's own inbox, over the `MQ` you
already gave `converge.New`.

## Notifying from another binary

The process that notices a change is usually not the process that runs the
job. An API handler onboards a merchant; a separate binary runs the
reconcile job. They share three things and nothing else: the `MQ` backend,
the job's name, and — for worker jobs — the payload shape.

The producer side needs no `Runtime` at all:

```go
p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: namespace})
if err != nil {
    return err
}

p.Notify(ctx, "merchant-stripe", string(newMerchant))
```

`Namespace` has to match the one the `Runtime` was built with, because the
namespace and the job name together are what name the inbox. That is the
whole coupling.

Here is both halves in one runnable program: the reconcile job, and a
goroutine standing in for the API binary that onboards a merchant *after*
the first sweep has already listed the directory — so the only way that
merchant can be reconciled in this run is the notification.

```go title=examples/scenarios/a03-merchant-sync/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow       = 2 * time.Second
	pollStep         = 5 * time.Millisecond
	namespace        = "payments"
	merchantPageSize = 2
	newMerchant      = reconcile.ID("m-1004")
)

type merchantDirectory struct {
	mu     sync.Mutex
	ids    []reconcile.ID
	listed bool
}

func newMerchantDirectory(ids ...string) *merchantDirectory {
	return &merchantDirectory{ids: reconcile.ToIDs(ids...)}
}

func (d *merchantDirectory) page(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("merchant page cursor %q: %w", cursor, err)
		}
		start = at
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	end := min(start+merchantPageSize, len(d.ids))
	next := ""
	if end < len(d.ids) {
		next = strconv.Itoa(end)
	} else {
		d.listed = true
	}
	return slices.Clone(d.ids[start:end]), next, nil
}

func (d *merchantDirectory) add(id reconcile.ID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ids = append(d.ids, id)
}

func (d *merchantDirectory) fullyListed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.listed
}

type stripeSync struct {
	mu    sync.Mutex
	syncs map[reconcile.ID]int
}

func newStripeSync() *stripeSync { return &stripeSync{syncs: map[reconcile.ID]int{}} }

func (s *stripeSync) syncMerchant(_ context.Context, id reconcile.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncs[id]++
	return nil
}

func (s *stripeSync) report() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines := make([]string, 0, len(s.syncs))
	for id, n := range s.syncs {
		lines = append(lines, fmt.Sprintf("%s synced %d time(s)", id, n))
	}
	slices.Sort(lines)
	return lines
}

func awaitFirstSweep(ctx context.Context, rt *converge.Runtime, merchants *merchantDirectory) error {
	select {
	case <-rt.Ready():
	case <-ctx.Done():
		return ctx.Err()
	}
	for !merchants.fullyListed() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollStep):
		}
	}
	return nil
}

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

	stripe := newStripeSync()
	merchants := newMerchantDirectory("m-1001", "m-1002", "m-1003")

	err = reconcile.Register(rt, reconcile.Spec{
		Job:       reconcile.NewJob("merchant-stripe", reconcile.JobOpts{}),
		Reconcile: stripe.syncMerchant,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDsByPage(merchants.page), reconcile.Every(15*time.Minute)),
			reconcile.Notifications(),
		},
		Concurrency: 8,
		Timeout:     20 * time.Second,
	})
	if err != nil {
		return err
	}

	p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: namespace})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	onboarded := make(chan error, 1)
	go func() {
		if err := awaitFirstSweep(ctx, rt, merchants); err != nil {
			onboarded <- err
			return
		}
		merchants.add(newMerchant)
		onboarded <- p.Notify(ctx, "merchant-stripe", string(newMerchant))
	}()

	if err := rt.Run(ctx); err != nil {
		return err
	}
	if err := <-onboarded; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	for _, line := range stripe.report() {
		fmt.Println(line)
	}
	fmt.Printf("%s was added after the sweep had listed the directory, so its run came from Notify\n", newMerchant)
	return nil
}
```

```sh
cd examples
go run ./scenarios/a03-merchant-sync
```

```text
m-1001 synced 1 time(s)
m-1002 synced 1 time(s)
m-1003 synced 1 time(s)
m-1004 synced 1 time(s)
m-1004 was added after the sweep had listed the directory, so its run came from Notify
```

`m-1004` was invisible to the sweep and still got synced. Delete the
`Notifications` trigger and it would have been synced fifteen minutes later
instead — slower, never wrong. That is the difference between a latency
accelerator and a correctness guarantee, and it is worth keeping straight:
the schedule is the guarantee, every other trigger is an accelerator. Switch
them all off and the job still converges.

## What a producer cannot do

A producer has two verbs — `Notify` and `Enqueue` — and no control
authority. It cannot make a job run now, pause it, change how often it
sweeps, or ask it how it is doing. Everything about a job's life is declared
where the job is registered. This is not an oversight to be worked around;
it is what keeps a job's behaviour readable from one file.

## Reading a queue another system writes

Sometimes the thing that knows an ID changed is not yours, and it is not
going to learn your conventions. A Python service pushes JSON onto a Redis
list; you want that to hurry a reconcile job along.

`reconcile.NotificationsFrom` is the one place in the whole surface where a
raw queue name appears, and it is used exactly as given:

```go
reconcile.NotificationsFrom(foreignQueue, convredis.NewListMQ(rdb), reconcile.IDFromJSON("workspace_id")),
```

Three things are different from `Notifications`:

- **The source is named.** `foreignQueue` here is the literal string
  `"enterprise:workspace:sync:queue"`. converge does not namespace it,
  prefix it, or own it.
- **You supply the `id` function**, because converge has no idea what shape
  that system's messages are. `reconcile.IDFromJSON("workspace_id")` reads
  one string field out of a JSON object; `reconcile.RawID()` takes the whole
  payload as the ID. Anything that fails to decode is dropped and reported,
  never guessed at.
- **You supply the `mq`**, because a foreign queue is usually not the
  transport your own jobs use. Here it is a Redis list rather than the
  stream the runtime is wired to.

Everything downstream is identical: a decoded ID goes into the same
deduplicated queue as a sweep or a plain notification, and the schedule
still backs it up. The whole program is
[`examples/scenarios/a14-foreign-queue/main.go`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a14-foreign-queue/main.go)
— it needs a Redis to talk to, and tells you so and exits cleanly if there
is not one.

## Next

Everything so far has been *level-triggered*: converge told your function
which thing to look at, and your function worked out what to do from the
state it found there. [Chapter 4](04-worker.md) is the *edge-triggered*
other half, where the message does not point at the work — it is the work.
