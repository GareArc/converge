# Converge

converge is a Go library that gives a service one model for all background
work. You register a function under a name and declare two things about it —
when it runs, and what it is about — and converge runs it correctly across
every replica of your service, bringing leasing, restart-safe scheduling,
bounded retries with backoff, and observability with it. It never owns your
data, and it is not a workflow engine, a bus abstraction, or a scheduler
service.

## Install

```sh
go get github.com/GareArc/converge
```

The core module is stdlib-only (plus a parse-only cron dependency) — it has
no Redis, no database driver, nothing to configure to start using it against
`converge/inmem`. Backends live in their own modules:

```sh
go get github.com/GareArc/converge/adapters/redis   # convredis: MQ, Lease, KV
go get github.com/GareArc/converge/adapters/otel    # convotel: Observer over OpenTelemetry metrics
go get github.com/GareArc/converge/bridges/kratos   # convkratos: Runtime as a kratos transport.Server
```

## Choosing a surface

One question tells the two kinds of job apart:

> **If this message were lost, would anything be wrong?**

- **No — [reconcile](docs/glossary.md#reconcile).** The truth lives in your
  store. Your function reads the store and makes the world match it. A
  message is only a [notification](docs/glossary.md#notification) — "look at
  this one sooner" — and the [schedule](docs/glossary.md#schedule) visits
  everything anyway, so notifications are cheap, unordered, deduplicated,
  and allowed to vanish.
- **Yes — worker.** The truth lives in the message. Something happened and a
  side effect must follow. The message *is* the work, so it is durable,
  retried, and moved to the [shelf](docs/glossary.md#shelf) for a person to
  look at when the retries run out.

## Non-goals

- **No exactly-once execution.** converge retries on failure — your function
  has to be safe to run twice, not just once.
- **No workflow orchestration and no sagas.** A job is one function, not a
  sequence of steps coordinated across failures; that is what a tool like
  Temporal is for.
- **No built-in batching.** A reconcile function is called once per thing
  it's checking; group things yourself inside the call if a downstream
  service needs bulk requests.
- **No ordering guarantee between worker messages, and none per key.**
  Messages for the same job can be handled at the same time, in any order.
- **No scheduling work for a specific clock time, and no cancelling it once
  queued.** A due time belongs in your own row, checked by a reconcile job;
  a worker delay covers minutes, never days.
- **No jobs that exist for a single invocation, and no registering or
  removing jobs while the process runs.** Jobs are written in code; the only
  thing that comes and goes at runtime is an [ID](docs/glossary.md#id).
- **No pause, detach, or resume.** A job is in one of three states — not
  started, running, or finished for good — and if it is ever going to
  finish, you say so when you register it, with a
  [stop condition](docs/glossary.md#stop-condition).
- **No producer-side control of consumers.** Whoever sends a job something
  can say "look at this" and "do this", and nothing else; everything about a
  job's life is declared where the job runs.
- **No splitting one reconcile job's work across replicas.** Concurrency
  inside a single replica is the answer converge gives.
- **No per-customer rate limits.** A rate limit applies to a whole job, not
  to one customer within it.

## License

MIT — see [LICENSE](LICENSE).
