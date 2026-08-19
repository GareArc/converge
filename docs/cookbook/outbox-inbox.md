# Outbox and inbox recipes

Two recipes for the same underlying problem: converge's queue boundary
doesn't span your database transaction. Both compose the two surfaces —
worker for the durable unit of work, reconcile for the drain loop.

## Outbox

*From [Scenario C](scenario-c-durable-work.md): producer-side durability.*

A plain `Enqueue` call is not atomic with your DB transaction: a rollback
still sends the email; a failed enqueue after commit loses it. When the
enqueue must ride the transaction, use the outbox pattern:

1. In the **same transaction** as the domain write, insert the payload into
   an outbox table. Commit = the work durably exists.
2. A small reconciler (`"outbox-drain"`, `OnOneReplica`, `Every(2s)` + a poke
   from the committing handler) reads unsent rows, `Enqueue`s them, marks
   them sent. At-least-once end to end; the worker's idempotency absorbs
   the duplicates.

## Inbox

*From [Scenario D](scenario-d-foreign-queue.md): consumer-side durability
over a foreign queue you cannot change.*

When the foreign queue carries true verbs (data you cannot re-read) and you
*can't* change the producer, use the **inbox pattern** — the outbox's mirror
image: a minimal consumer moves each foreign message into a durable inbox
table (that move is the only lossy step, kept tiny), and a reconciler
converges the table — processing each row exactly like the outbox drain
above. You get durability and retries from the table, not from hand-rolled
queue machinery.
