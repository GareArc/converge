# Worker

## Outcomes

`worker`'s outcome table (see [Outcome values](concepts.md#outcome-values)
for the shared mechanics — detection, wrapping, wrong-surface protection):

| Return | Engine does |
|---|---|
| `nil` | ack — done forever |
| `worker.Snooze{In: d}` | redeliver after `d`, logical attempt counter untouched |
| `worker.Discard{Reason: s}` | ack + observer event — work no longer relevant |
| ctx canceled by shutdown | **neutral** — Ack + immediate republish, logical attempt preserved, no failure counted; redelivered promptly, not after `Visibility` |
| any other error | attempt++, redeliver with backoff |
| attempts reach `Retry.MaxAttempts`, or age exceeds `Retry.MaxAge` | → DLQ (payload + final error kept). `MaxAge` (default 24h) is the snooze ceiling — a message can't snooze forever |
| panic | recovered, treated as error |

While a worker handler runs, the engine **automatically extends visibility**
(heartbeats at `Visibility/3`) — a 20-minute export with 5-minute visibility
is safe; reclaim-by-another-worker happens only when the process actually
dies.

## Logical attempt vs. transport delivery

`Meta.Attempt` (what your handler sees) and the MQ port's own delivery count
are two different numbers, and the difference matters for retry logic:

- **Logical attempt** — what `Meta.Attempt` reports: the `converge.attempt`
  header's base plus the current transport delivery count. This is what
  `Retry.MaxAttempts` counts against.
- **Transport delivery** — `Delivery.Attempt()`, how many times the MQ port
  itself has redelivered the in-flight message; climbs with every Nack. A
  `Snooze` republishes as a **fresh** message, so its transport delivery
  resets to one — the logical attempt is what survives that reset, carried
  forward in the `converge.attempt` header.

If the `converge.attempt` header is corrupt or unparseable, the guard that
caps consecutive no-backoff Snooze loops (see
[Outcome values](concepts.md#outcome-values)) self-heals: the count resets
rather than failing the message, so a corrupted header costs at most one
extra loop, never a stuck message.

## Run modes

Workers default to `SplitAcrossReplicas` (see
[Run modes and concurrency](run-modes.md)), but `OnAllReplicas` is also
valid for per-replica handlers. Under `OnAllReplicas` the worker semantics
change: delivery is at-most-once **per replica** (each replica's own
subscription, not a shared competing-consumer group), `Snooze` and any other
error simply drop the message with a `RunCompleted` event instead of
retrying, and there is no KV bookkeeping, no DLQ, and no `Retry` policy in
play — there is no shared state to protect. Use it only for jobs whose
double-processing across replicas is harmless (invalidate a local cache),
never for jobs that need at-least-once-with-retry.
