# Converge terms in other systems

A lookup table for readers arriving from Kubernetes, controller-runtime, or
Kafka. Nothing in the guide or the rest of the reference depends on this page;
it exists so that a familiar concept under an unfamiliar name is one search
away.

The left column is converge's own vocabulary, which the
[glossary](../glossary.md) defines. The right column is what other systems
call the nearest thing — those names are theirs, not converge's, and they are
not interchangeable in this project's code, messages, or documentation.

## Terminology map

| Converge | Known elsewhere as |
| --- | --- |
| `Schedule` (the trigger) | resync / periodic sweep (Kubernetes) |
| `reconcile.CheckAgain{In: d}` | `RequeueAfter` (controller-runtime) |
| run modes | coordination postures; `OnOneReplica` is leader election |
| `reconcile.Version` / `VersionSource` | generation and observedGeneration (Kubernetes); fencing token (Kleppmann) |
| `reconcile.ID` | workqueue key (Kubernetes) |
| queue | topic (Kafka); stream (Redis) |
| the shelf | dead-letter queue (SQS, RabbitMQ, Sidekiq's dead set) |
| failing ID | an item in a rate-limited workqueue (Kubernetes) |

## Two places the mapping is not one-to-one

**The shelf is only half of what "dead-lettering" usually covers.** In most
systems a message that runs out of retries and a piece of work that keeps
failing end up in the same place under the same name. converge keeps them
apart, because getting out is not the same operation:

- A [failing ID](../glossary.md#failing-id) on the reconcile surface is never
  set aside at all. It keeps being retried at a floor rate forever, and a
  notification returns it to the front of the queue.
- A [shelved message](../glossary.md#shelved-message) on the worker surface
  is set aside durably and does not come back on its own. A
  [requeue](operations.md#requeueing-a-shelved-message) is a deliberate act by
  a person.

converge has no dead-letter queue and no word for one: the shelf is the whole
of what it offers on the worker surface, and a reconcile ID that keeps failing
is never set aside at all. Search this documentation for either half by the
converge name — shelf, or failing ID — because the other name appears nowhere
but this page.

**A lease is not a distributed lock.** `OnOneReplica` uses one to keep
duplicate work rare, and converge says plainly that a lease is an efficiency
device and never a correctness device — there is a window in which two
replicas both believe they hold it. Correctness comes from your function being
safe to run twice. Systems that call the same mechanism a lock often imply
more than that.
