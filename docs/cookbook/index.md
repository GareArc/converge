# The cookbook

The [guide](../guide/index.md) teaches the model in order, one idea per
chapter. This is the other half: six problems people actually bring to a
background-work library, each one worked end to end, and each one ending
somewhere the guide stops short of.

Every page opens by naming the chapter it assumes and the program under
`examples/scenarios/` it is drawn from, so you can run the thing you are
reading about:

```sh
cd examples
go run ./scenarios/a08-transcode
```

These pages use converge's vocabulary without re-teaching it: a
[reconcile](../glossary.md#reconcile) job, a
[notification](../glossary.md#notification), a job's
[inbox](../glossary.md#inbox), the [shelf](../glossary.md#shelf). Each of
those is one paragraph in the [glossary](../glossary.md), and every page
links the words it leans on.

## The pages

1. **[Work that takes a while](durable-work.md)** — half an hour of
   transcoding on a surface built for half a second of work: a run that is
   not ready to start yet, a [time limit](../glossary.md#time-limit) long
   enough to be dangerous, and a payload that has to change shape while
   messages are already in flight.
2. **[Waiting for something to become true](event-driven.md)** — you asked
   another system to make something so and it said *working on it*. How to
   wait for the answer without writing a poll loop, and what a deferred run
   costs.
3. **[A queue somebody else owns](foreign-queue.md)** — JSON you did not
   design, on a queue you cannot change, read as a
   [notification](../glossary.md#notification) about what to look at rather
   than as instructions to follow.
4. **[Jobs that end](lifecycle.md)** — a migration with a last row in it, the
   [stop condition](../glossary.md#stop-condition) that says so, and why this
   is the one thing in converge that cannot be undone.
5. **[Outbox and inbox](outbox-inbox.md)** — the two patterns for making a
   database write and a message agree about what happened, and which half of
   each one converge owns.
6. **[The safety net](safety-net.md)** — the job nobody sends anything to and
   nothing waits on, which exists so that nothing is ever *permanently*
   wrong.

## If you are looking for something else

- The [guide](../guide/index.md) is the place to start if you have not
  written a converge job yet.
- The reference has the exact signatures and defaults:
  [kernel](../reference/kernel.md), [reconcile](../reference/reconcile.md),
  [worker](../reference/worker.md), [operations](../reference/operations.md),
  and [adapters and test support](../reference/adapters.md).
- [Converge terms in other systems](../reference/prior-art.md) maps this
  vocabulary onto the one you already know.
