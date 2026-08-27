# Waiting for something to become true

> Assumes [chapter 3, telling a job to look sooner](../guide/03-notifications.md).
> The program is
> [`a13-namespace-reconciler`](../../examples/scenarios/a13-namespace-reconciler/main.go).

A whole family of work has the same shape: you ask another system to make
something so, and it says *working on it*. Kubernetes accepts a namespace and
schedules it. A certificate authority accepts an order and validates it. A
provisioning API returns 202 and a URL to poll. The operation takes an unknown
amount of time, it can fail halfway, and — this is the part that matters —
you can always ask again what the current state is.

That last sentence is the decision. The answer is re-readable, so a message
about it is only a [notification](../glossary.md#notification), and this is a
[reconcile](../glossary.md#reconcile) job. It is worth saying out loud
because the work *feels* event-driven: something happened, and something has
to follow. It is still level-triggered, because the thing that happened left a
row behind.

## The loop

One function does both halves. It declares what should be true — `apply` is
idempotent by construction, so calling it on every visit is free — and then
looks at whether it is yet. [Chapter 2](../guide/02-ids.md) has the five
lines; the shape is that when the cluster has not finished converging, the
function returns `reconcile.CheckAgain{In: 15 * time.Second}` rather than an
error, because it has not failed.

That `In` is a statement about the dependency, not a retry budget: pick
roughly how long that system usually takes to settle. Fifteen seconds for a
namespace, a minute for a certificate. Getting it wrong costs latency in one
direction and wasted calls in the other, and nothing worse.

## Two triggers, two different jobs to do

```go
Triggers: []reconcile.Trigger{
    reconcile.Schedule(reconcile.IDsByPage(customers.page), reconcile.Every(30*time.Minute)),
    reconcile.Notifications(reconcile.NotificationsOpts{}),
},
```

Half an hour is a long time to notice a new customer, and it is deliberately
long: the [sweep](../glossary.md#sweep) is not how this job is supposed to be
fast. It is the floor — the guarantee that a namespace nobody notified anyone
about still gets built. Notifications are how the same job reacts in the
second after the signup handler commits.

Split that way, both settings are easy. The period answers "if every
notification were lost, how late may we be?" The notification answers
"how fast do we want the good case to be?" Neither is a compromise between
the two.

Run the program and the last customer arrives after the sweep has already
listed the customers, so the only thing that can reconcile it is the
notification:

```text
run completed job=customer-namespace id=c-5003 attempt=1 outcome=succeeded
run completed job=customer-namespace id=c-5001 attempt=1 outcome=succeeded
run completed job=customer-namespace id=c-5002 attempt=1 outcome=deferred
run completed job=customer-namespace id=c-5004 attempt=1 outcome=succeeded
cust-c-5001 applies=1 ready=true rechecks-asked=0
cust-c-5002 applies=1 ready=false rechecks-asked=1
cust-c-5003 applies=1 ready=true rechecks-asked=0
cust-c-5004 applies=1 ready=true rechecks-asked=0
```

## What a deferral costs, and what it does not

Nothing, is the short answer, and that is the point of having a value
separate from an error. A `CheckAgain` **clears** the ID's consecutive
failure count and stamps its last success, so an ID that failed three times
and then reported "not ready" has its backoff curve reset — the next real
failure starts at one second again, not at eight. A deferred ID is not a
[failing ID](../glossary.md#failing-id) and is not counted in
`JobStats.Failing`.

Four edges are worth knowing before you build a long poll out of this:

- **`In: 0` is not "immediately".** Every deferral delay is floored at 250ms
  and jittered a little above it, because a zero-delay deferral is a spin
  loop.
- **Your delay is honoured ten times out of every eleven.** On the eleventh
  consecutive deferral converge substitutes a delay from its own 1s-to-15m
  curve, then starts the count of ten over, stepping one place further along
  that curve each time it substitutes. The curve starts at one second, so
  against a fifteen-second `In` the first few substitutions are *shorter*
  than the delay you asked for, not longer. This is a bound against
  `In: 0` spinning; a namespace that will never be ready only starts costing
  less once the curve has climbed past your own delay.
- **A deferred ID is not protected from being pulled forward.** Any trigger
  moves it to the front — a sweep as much as a notification. This is the
  opposite of failure backoff, which only a notification bypasses. If your
  schedule is short and your `In` is long, the schedule wins.
- **A worker outcome returned here is a plain failure.** Returning
  `worker.Snooze` from a reconcile function is not reinterpreted as a
  deferral: the ID goes into failure backoff with your value as the error.
  The mirror case is worse, and it is in
  [durable work](durable-work.md#the-two-surfaces-do-not-share-outcomes).

## Reading it in production

A healthy job of this shape logs warnings. `Deferred` is levelled at warn by
`converge.LogObserver`, alongside `Retrying` — so a namespace that is calmly
coming up produces the same log level as one that is failing. The deferral
line carries no `err=` field and the failure line does, which is the only
thing separating them in a log.

The metrics separate them properly. `converge.run.duration` carries
`converge.status`, which is derived from whether an error was attached, and
`converge.outcome` separately: a deferral is `status=ok, outcome=deferred`.
Alert on the outcome attribute, or on `JobStats.Failing`, and not on the log
level.

If you want to know how long an ID has been stuck in this loop, converge does
not count that for you. A deferral is not an attempt, `JobStats` has no field
for it, and a deferred ID is not in `failing_ids` either — it is not failing.
Keep the count in your own row, next to whatever `apply` wrote, and treat it
as your data rather than the library's.
