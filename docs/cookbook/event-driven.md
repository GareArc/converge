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
separate from an error. A deferred ID is not a
[failing ID](../glossary.md#failing-id): it is not counted in
`JobStats.Failing`, it does not appear in `failing_ids`, and it *clears*
whatever consecutive-failure count the ID had built up.

The mechanics converge puts around a deferral — the floor under `In`, the
substitution that keeps `In: 0` from spinning, and what a pending trigger
does to an ID waiting one out — are in the
[reconcile reference](../reference/reconcile.md#checkagain-and-erroutdated).
Two of them decide the numbers on this page.

**A sweep pulls a deferred ID forward exactly as hard as a notification
does.** Against the thirty-minute schedule above that never shows. Shorten
the schedule to a minute and the schedule, not your `In`, becomes the poll
interval — so if you are tuning `In` and seeing no change, that is where it
went. (Failure backoff is the opposite: only a notification bypasses that.)

**The substitution is a bound on spinning, not a way to give up.** It never
stops visiting the ID and never sets it aside; it only stretches the delays
it substitutes for yours, up to a fifteen-minute ceiling. Nothing in converge
escalates a `CheckAgain` loop with no end, so noticing one is your own code's
job.

And the value from the other surface is not a synonym for this one.
`worker.Snooze` returned from a reconcile function is a plain failure rather
than a deferral; the mirror case is worse, and it is in
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
not count that for you: a deferral is not an attempt, and nothing in `Stats`
has a field for it. Keep the count in your own row, next to whatever `apply`
wrote, and treat it as your data rather than the library's.
