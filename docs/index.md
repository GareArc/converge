# converge documentation

converge is a Go library that gives your services one model for all
background work. Every job you register is one of two kinds: a
[reconcile](glossary.md#reconcile) job, which answers "is everything as it
should be?" — you tell converge what to check and how often, and it calls
your function once per thing, which looks at how things actually are and
fixes what's wrong; or a worker job, which answers "do this one specific
thing that just happened?" — something sends a message, converge hands it to
your function, and tries it again if that function fails.

One question decides which kind you have:

> **Can you write a query that lists everything still to be done, without
> reading the queue?**

Yes means reconcile — the truth is in your store and a message is only a
[notification](glossary.md#notification). No means worker — the truth is in
the message, so it is durable, retried, and set aside on the
[shelf](glossary.md#shelf) rather than dropped.

## Start here

**[The guide](guide/index.md)** teaches the model in seven chapters. Every
program in it is whole and runnable, and almost none of them need Redis, a
database, or a container.

1. [Your first job](guide/01-first-job.md) — the question above, and a
   complete converge program.
2. [One job, many things](guide/02-ids.md) — one job responsible for ten
   thousand customers, one [ID](glossary.md#id) at a time.
3. [Telling a job to look sooner](guide/03-notifications.md) — the job's
   [notifications](glossary.md#notifications), `Notify` from another binary,
   and reading a source some other system already writes.
4. [When the message is the work](guide/04-worker.md) — tasks, retries, the
   three ways to stop early, and the shelf.
5. [Where a job runs](guide/05-run-modes.md) — three values, one rule, and
   what converge deliberately does not do across replicas.
6. [Taking it to production](guide/06-production.md) — Redis, logs,
   readiness, the numbers converge will and will not report, and jobs that
   finish.
7. [Testing a job](guide/07-testing.md) — a harness, a fake clock, and no
   `time.Sleep`.

## When you have a specific problem

**[The cookbook](cookbook/index.md)** works seven of them end to end.

- [Work that takes a while](cookbook/durable-work.md) — half an hour of
  transcoding on a surface built for half a second of work.
- [Waiting for something to become true](cookbook/event-driven.md) — polling
  another system without writing a poll loop.
- [A queue somebody else owns](cookbook/foreign-queue.md) — JSON you did not
  design, on a queue you cannot change.
- [Jobs that end](cookbook/lifecycle.md) — a migration with a last row in it,
  and the [stop condition](glossary.md#stop-condition) that says so.
- [Outbox and inbox](cookbook/outbox-inbox.md) — making a database write and
  a message agree about what happened.
- [The safety net](cookbook/safety-net.md) — the job nobody sends anything
  to, which exists so nothing is ever permanently wrong.
- [Credential sync from a Python service](cookbook/python-producer.md) — a
  producer that cannot import your package, and the one `XADD` it writes.

## When you want the exact signature

The reference covers every exported name, its defaults, and what it costs.

- [Kernel](reference/kernel.md) — `Options`, `Runtime`, `Scope`, the
  ports, the events, and the stats types.
- [Reconcile](reference/reconcile.md) — `Spec`, `Register`, `Periodic`,
  triggers, cadences, ID sources, and versions.
- [Worker](reference/worker.md) — `Task`, `Enqueue`, `Handle`, retries, the
  three outcomes, and the shelf.
- [Operations](reference/operations.md) — the debug routes, the JSON, how
  stale each number is, requeueing a shelved message, and destroying a job.
- [Wire](reference/wire.md) — what a producer in any language writes: channel
  names, the two payload shapes, the headers, and the Redis Streams
  encoding.
- [Adapters and test support](reference/adapters.md) — `inmem`, convredis,
  convotel, convkratos, `portcheck`, and `convergetest`.
- [Converge terms in other systems](reference/prior-art.md) — the same ideas
  under the names Kubernetes, controller-runtime and Kafka give them.

## Reference for words

- [Glossary](glossary.md) — every converge-specific word, defined once, in
  plain language.
- [Internals](https://github.com/GareArc/converge/blob/main/CONTEXT.md) — the
  terminology contract and contributor conventions this project holds itself
  to; start with
  [`AGENT.md`](https://github.com/GareArc/converge/blob/main/AGENT.md) for
  the verification commands.
