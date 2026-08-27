# Testing a job

Background work is the part of a service people give up on testing. It runs
on a clock you do not control, on a replica you did not pick, and the only
way to see it happen seems to be to wait. `convergetest` exists so that none
of that is true.

The harness gives you a whole `Runtime` backed by in-process
implementations, a clock you move by hand, and verbs that wait for the
system to settle instead of for the wall clock. No Redis, no `time.Sleep`,
no flakes.

## The test for chapter 2's job

Here is the entire test for the order-expiry job you read in
[chapter 2](02-ids.md):

```go title=examples/scenarios/a02-expire-orders/main_test.go
package main

import (
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/reconcile"
)

func TestUnpaidOrdersAreCancelled(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	store := newStore(h.Clock().Now)
	if err := reconcile.Register(rt, expireUnpaidOrders(store)); err != nil {
		t.Fatal(err)
	}
	store.create("o-1")
	h.Clock().Advance(31 * time.Minute)
	store.create("o-2")
	h.Drain(t)
	if got := store.status("o-1"); got != statusCancelled {
		t.Fatalf("order o-1 status = %q, want %q", got, statusCancelled)
	}
	if got := store.status("o-2"); got != statusPending {
		t.Fatalf("order o-2 status = %q, want %q", got, statusPending)
	}
}
```

```sh
cd examples
go test ./scenarios/a02-expire-orders
```

Three lines carry the whole test: `o-1` is created, the clock jumps 31
minutes, `o-2` is created. At the moment the job looks, one order is 31
minutes old and the other is brand new. `h.Drain(t)` then waits for converge
to notice, run its [sweep](../glossary.md#sweep), and settle, and the
assertions say what should have happened to each order.

There is no sleeping, no polling loop, and no retry-until-it-passes. Run it
a thousand times and it gives the same answer.

## What the harness gives you

`convergetest.New(t)` builds the harness; `h.Build(t)` builds a `Runtime`
wired to it. Register your jobs on that `Runtime` exactly as you would in
`main` — the job under test is the real job, not a copy of it.

The `Runtime` starts lazily, the first time you use a verb that needs it
running. That is why the test above can register a job and seed the store
after calling `h.Build(t)` and before anything starts.

| Verb | What it does |
| --- | --- |
| `h.Clock()` | the fake clock; `Advance(d)` moves it and releases whatever was waiting on it |
| `h.Drain(t)` | wait until nothing is queued and nothing is running |
| `h.Notify(job, id)` | hurry one [ID](../glossary.md#id) along, the way a [notification](../glossary.md#notification) would |
| `h.Sweep(t, job)` | force one [sweep](../glossary.md#sweep) now, then drain |
| `h.Stop(t)` | shut the runtime down and return what `Run` returned |
| `h.Events()` | every event converge reported while the test ran |

Plus assertions that poll rather than sleep: `h.AssertReconciled(t, job,
id)`, `h.AssertEnqueued(t, task, payload)`, and the free functions
`convergetest.Await`, `AdvanceUntil`, and `AssertStable`. The harness's `MQ`,
`KV`, and `Lease` are exported fields if you need to reach past them.

## The three verbs that matter, and when

**`Advance` then `Drain` is the pattern.** `h.Clock().Advance(31 *
time.Minute)` makes the [schedule](../glossary.md#schedule) fall due;
`h.Drain(t)` waits until the sweep has finished, every queued ID has been
reconciled, and nothing is left running. It is a *settle*, not a timeout: it
returns as soon as the system is quiet and fails the test if it never is.

**`Sweep` is for when you do not want to think about the clock.** It forces
one sweep of a named job immediately and then drains. Reach for it when the
test is about what your function does, not about when it runs.

**`Notify` is for testing the accelerator.** It puts an ID in line the same
way a producer's `Notify` would, without a producer and without a second
binary — useful for checking that a job reacts to an ID your ID source has
not started listing yet.

## Write the job so it can be tested

The test above is short because the program was written with two small
habits, and both are worth copying.

**Return the `Spec` from a function.** `expireUnpaidOrders(store)` builds the
whole job declaration and takes its dependency as an argument, so `main` and
the test register the identical job against different stores. There is no
"test mode", no global, and nothing to keep in sync.

**Take the clock as a dependency.** `newStore(now func() time.Time)` is
handed `time.Now` by `main` and `h.Clock().Now` by the test. That single
argument is why `Advance(31 * time.Minute)` makes an order look 31 minutes
old rather than making the test wait 31 minutes. Any code of yours that
compares timestamps needs this; converge's own time already goes through the
same clock.

## What not to test

Do not test that converge retries, that a [lease](../glossary.md#lease)
moves, or that a sweep happens on time. That is converge's contract with
you, it is covered by converge's own suite, and asserting it again in your
repository just means your tests fail when converge improves.

Test what your function does with what it is given: that an unpaid order is
cancelled and a paid one is not, that a
[shelved message](../glossary.md#shelved-message) carries the reason you
expect, that the second run of the same [ID](../glossary.md#id) is harmless.
That last one deserves a test of its own — being safe to run twice is the
one thing converge asks of you and cannot check for you.

## The end

Seven chapters, one question:

> **If this message were lost, would anything be wrong?**

No — [reconcile](../glossary.md#reconcile), and the truth lives in your
store. Yes — [worker](../glossary.md#worker), and the truth lives in the
message. Everything else in this library is a consequence of that answer.

- Every word with a specific meaning is in the [glossary](../glossary.md).
- Every program is under `examples/scenarios/`, and all fifteen run.
