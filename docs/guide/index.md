# The guide

converge gives a service one model for all background work. This guide
teaches that model in seven chapters. Each one ends where the next begins,
and every program in it is a real file under `examples/scenarios/` that you
can run.

## Start with one question

Before you reach for an API, answer this about the work you have:

> **If this message were lost, would anything be wrong?**

**No.** The truth lives in your own store, and a message is only a note
saying *look at this one sooner*. Losing it costs a little latency and never
correctness. That is a [reconcile](../glossary.md#reconcile) job.

**Yes.** The truth lives in the message. Something happened, a side effect
has to follow, and no amount of re-reading your database will tell you what
it was. That is a [worker](../glossary.md#worker) job: the message is
durable, it is retried, and it ends up on the
[shelf](../glossary.md#shelf) for a person to look at rather than being
dropped.

Every chapter below either teaches that distinction or builds on it.

## The chapters

1. **[Your first job](01-first-job.md)** — the question above, and a
   complete converge program you can run.
2. **[One job, many things](02-ids.md)** — a single job responsible for ten
   thousand customers, one [ID](../glossary.md#id) at a time, and where the
   list of IDs comes from.
3. **[Telling a job to look sooner](03-notifications.md)** — the
   [inbox](../glossary.md#inbox), `Notify` from a different binary, and
   reading a queue some other system already writes.
4. **[When the message is the work](04-worker.md)** — tasks, retries, the
   three ways to stop early, and the shelf.
5. **[Where a job runs](05-run-modes.md)** — three values, one rule, and
   what converge deliberately does not do across replicas.
6. **[Taking it to production](06-production.md)** — Redis, logs, readiness,
   the numbers converge will and will not report, and jobs that finish.
7. **[Testing a job](07-testing.md)** — a harness, a fake clock, and no
   `time.Sleep`.

## Before you start

- Every converge-specific word is defined once in the
  [glossary](../glossary.md). If a word on these pages looks like it is
  carrying more weight than usual, it is, and the glossary says why.
- The programs live in the `examples` module. Run them from there:

```sh
cd examples
go run ./scenarios/a01-nightly-invoices
```

- Almost nothing here needs Redis, a database, or a container: the `inmem`
  package supplies everything converge needs in process, so the programs run
  and their tests pass with no services at all. Exactly one scenario is the
  exception — `a14-foreign-queue`, which chapter 3 links to rather than
  shows, reads a real Redis list and tells you so if there is not one.
