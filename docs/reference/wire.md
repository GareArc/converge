# Wire reference

The compatibility contract for anything that is not Go. Every name on this
page — field, header, payload shape, derivation — is fixed from this change
on and changes only with a major version. If you are writing to converge
from Python, a shell script, or another team's service, this page is the
whole of what you need; if you are writing Go, import the job value and none
of this is your concern.

- [Two channels, two words](#two-channels-two-words)
- [Where a channel name comes from](#where-a-channel-name-comes-from)
- [The logical message](#the-logical-message)
- [A worker queue entry](#a-worker-queue-entry)
- [A reconcile notification](#a-reconcile-notification)
- [A list element](#a-list-element)
- [Redis Streams encoding](#redis-streams-encoding)
- [Three things a producer must never do](#three-things-a-producer-must-never-do)

## Two channels, two words

| | reconcile | worker |
| --- | --- | --- |
| the channel | **notifications** | **queue** |
| carries | a pointer: an ID, or "all" | the only copy of the work |
| if an entry is lost | nothing; the next sweep covers it | the work is lost |
| declared as | `reconcile.JobOpts.Notifications` | `worker.TaskOpts.Queue` |
| the Go verb | `Notify` / `NotifyAll` | `Enqueue` |
| may an operator flush it? | yes | **never** |

That last row is the operator's rule, and it is why the derived names differ:
a key containing `/converge/notifications/` may be deleted, trimmed, or
flushed with nothing worse than a few minutes' latency; a key containing
`/converge/queue/` holds work that exists nowhere else.

## Where a channel name comes from

A channel name is either **declared** on the job or task — used exactly as
written, not namespaced, not prefixed — or **derived**:

```text
<namespace>/converge/notifications/<job name>
<namespace>/converge/queue/<task name>
```

With an empty namespace the leading segment is dropped:
`converge/queue/<task name>`. The Go side can print either with
`job.NotificationsName(namespace)` or `task.QueueName(namespace)`; ask the
team that owns the Go service to print it at startup rather than deriving it
yourself.

Declared names may be anything the backend accepts. converge refuses only
what is invisible — leading or trailing whitespace, and control characters,
with the offending byte named.

## The logical message

Every entry on either channel is one logical message:

| part | meaning |
| --- | --- |
| `payload` | bytes; the body |
| `kind` | a string; read only as an input to the synthetic message ID |
| `headers` | string to string; the `converge.*` names below are the library's |

How those three are laid onto a particular backend is that backend's
encoding, [below](#redis-streams-encoding). Everything else on this page is
stated against the logical message, so it holds on any backend.

## A worker queue entry

| part | required | value |
| --- | --- | --- |
| `payload` | yes | the body, in the task's codec — JSON unless the task declared another |
| `kind` | no | Go producers set the task name; intake reads it only to derive a synthetic `converge.message-id`, so varying it varies that ID |
| `headers` | no | see below |

Headers a Go producer stamps, and what intake does when each is absent:

| header | absent means |
| --- | --- |
| `converge.attempt` | attempt base 0; the logical attempt your handler sees is `0 + the transport's delivery count`, which starts at 1 |
| `converge.schema-version` | **no claim made**; intake goes on to decode. (A present value that does not match the handler's version shelves the message with reason `schema version`.) |
| `converge.enqueued-at` | the transport's own enqueue time |
| `converge.message-id` | `anon-` plus the first 16 bytes of SHA-256 over `kind` then `payload`, hex |
| `converge.snoozes` | 0 |

Two of those are compared, not parsed loosely:

- **`converge.schema-version` is compared byte for byte** against the
  decimal rendering of the handler's version. Write `1`, not `01`, not `1.0`,
  not ` 1`. A value that does not match exactly is a version mismatch, and
  the message is shelved with reason `schema version` before it ever reaches
  your handler. If you are not tracking the Go side's version, send no such
  header at all: an absent header is no claim, and the payload goes straight
  to the codec.
- **`converge.attempt` must be a non-negative decimal integer.** A value that
  is not shelves the message with reason `undecodable`.

The synthetic `converge.message-id` is a pure function of `kind` and
`payload`, so two entries carrying the same bytes are **one identity**: each
one runs, but they share a single shelf record, and the later shelving
overwrites the earlier. A producer whose distinct entries can be
byte-identical must set its own `converge.message-id`.

A producer that sets any `converge.*` header is **trusted**: setting one
means taking over that field. Most producers should set none. The minimum
entry is the payload alone (plus `enq` on Redis Streams — see
[the encoding](#redis-streams-encoding)).

## A reconcile notification

| part | required | value |
| --- | --- | --- |
| `payload` | yes | `{"id":"<id>"}` to say *look at this one*, or `{"all":true}` to say *look at everything* |
| `kind` | no | Go notifiers set `converge.notification`; intake never reads it |
| `headers` | no | none are read |

`{"id":""}`, `{}`, `{"all":false}` with no `id`, and an `id` alongside
`"all":true` are undecodable: acknowledged and dropped, with a
`NotificationDropped` event, never interpreted. Unknown fields are ignored,
so `{"id":"ws-1","reason":"rotated"}` is fine — and so is
`{"id":"ws-1","all":false}`, which is just that ID. Only `"all":true`
conflicts with an `id`.

`{"all":true}` on a job with one ID runs that ID; on a job with many it
starts a sweep now, on every `Schedule` trigger the job has and without
moving the cadence. It is for the producer that just changed many IDs at
once and would otherwise notify each.

## A list element

A Redis list read with `convredis.ListTrigger` carries **only** the
payload — any bytes. The job's ID function owns decoding. Producers
`LPUSH`; the adapter `BRPOP`s, and the pop is destructive: an element the
ID function rejects is gone. A list can be a reconcile job's source and
can never be a worker's queue.

## Redis Streams encoding

One stream entry per logical message, at the key named by the channel:

| field | required | holds |
| --- | --- | --- |
| `payload` | yes | `payload`, as a string |
| `enq` | **yes** | when the entry was enqueued, RFC 3339 with an offset, nanoseconds optional |
| `kind` | no | `kind` |
| `headers` | no | `headers` as a JSON object of string to string |

**`enq` is the one field a foreign producer must not forget.** The adapter
parses it to build the delivery's enqueue time, and an entry whose `enq` is
missing or unparseable cannot be decoded at all: the adapter acknowledges it
and moves on, with no event and no shelf. `2026-08-28T12:00:00Z` and
`2026-08-28T12:00:00.123456789+08:00` both parse; `2026-08-28 12:00:00` does
not.

The minimum a Python producer writes to a worker task whose queue is
`dify:credential:rotate`:

```text
XADD dify:credential:rotate * payload '{"workspace_id":"ws-1","provider":"openai"}' enq '2026-08-28T12:00:00Z'
```

and to a reconcile job whose notifications are `dify:workspace-credentials`:

```text
XADD dify:workspace-credentials * payload '{"id":"ws-1"}' enq '2026-08-28T12:00:00Z'
```

Headers, when you must set them:

```text
XADD dify:credential:rotate * payload '{...}' enq '...' headers '{"converge.message-id":"order-7781"}'
```

The stream key is exactly the channel name. The adapter's bookkeeping keys
are not yours to write, and not yours to collide with either:
`convredis:p:<channel>:<group>` is the pending index,
`convredis:a:<channel>:<group>` the attempt counters, and
`convredis:d:<channel>` the delayed set.

## Three things a producer must never do

1. **Set a `converge.*` header it does not understand.** They are trusted,
   so a wrong `converge.attempt` spends the message's retries, a wrong
   `converge.schema-version` shelves it unread, and a `converge.enqueued-at`
   far enough in the past shelves it for `max age` on arrival.
2. **Write to a derived channel without matching the namespace exactly.**
   `payments/converge/queue/send-email` and `payment/converge/queue/send-email`
   are two keys, and the second is read by nothing, reported by nothing, and
   — with `Retention` unset — trimmed by nothing. Prefer a declared name for
   any channel a foreign producer writes to.
3. **Declare a channel name beginning `convredis:p:`, `convredis:a:` or
   `convredis:d:`.** Those are the Streams adapter's own key prefixes, and a
   channel named into that space collides with another channel's bookkeeping
   — loudly, as a Redis `WRONGTYPE` error, because those keys are sorted sets
   and a hash while a channel is a stream.
