# Converge

converge is a Go library providing one runtime for two kinds of background
job, with the infrastructure both kinds need across replicas built in:
leader election, schedule state that survives restarts, bounded retries with
backoff, and [dead-letter](docs/glossary.md#dead-letter-dlq) storage.

- A [reconcile](docs/glossary.md#reconcile) job answers "is everything as it
  should be?" It takes a set of IDs and a
  [cadence](docs/glossary.md#cadence), and calls one function per ID; that
  function reads current state and corrects it. A message on this surface is
  a [hint](docs/glossary.md#hint) that something may have changed — never
  the work itself.
- A worker job answers "do this one thing that just happened." The message
  is the work: it is delivered at least once, retried with backoff when the
  handler returns an error, and dead-lettered once the retry budget is
  spent.

The package layout makes the model explicit:

```go
import (
    "github.com/GareArc/converge"           // runtime, Options, run modes
    "github.com/GareArc/converge/reconcile" // "is everything as it should be?"
    "github.com/GareArc/converge/worker"    // "do this one specific thing that just happened?"
)
```

A file's imports declare its model: code importing `worker` but not
`reconcile` is provably pure queue-processing, and vice versa.

## Install

```sh
go get github.com/GareArc/converge
```

The core module is stdlib-only (plus a parse-only cron dependency) — it has
no Redis, no database driver, nothing to configure to start using it against
`converge/inmem`. Backends live in their own modules:

```sh
go get github.com/GareArc/converge/adapters/redis   # convredis: MQ, Lease, KV, ListTrigger
go get github.com/GareArc/converge/adapters/otel    # convotel: Observer over OpenTelemetry metrics
go get github.com/GareArc/converge/bridges/kratos   # convkratos: Runtime as a kratos transport.Server
```

## Two examples

Both are copied verbatim from the guide, and both run with nothing
installed.

**A reconcile job**, from [chapter 1](docs/guide/01-first-job.md): one
function, called on a schedule, that only one copy of your service runs. The
in-memory bookkeeping it uses is process-local, so that holds inside one
process; [chapter 6](docs/guide/06-production.md) swaps those two lines for
Redis and it holds across replicas.

```go title=examples/guide/01-first-job/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func main() {
	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Periodic(rt, "sync-inventory", reconcile.Every(2*time.Second), func(ctx context.Context) error {
		fmt.Println("syncing inventory with the warehouse")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

```sh
cd examples && go run ./guide/01-first-job
```

```
syncing inventory with the warehouse
syncing inventory with the warehouse
syncing inventory with the warehouse
syncing inventory with the warehouse
```

**A worker job**, from [chapter 4](docs/guide/04-worker.md): two messages,
sent once each and delivered to your handler.

```go title=examples/guide/04-worker/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

type ChargeOrder struct {
	OrderID string `json:"order_id"`
}

func main() {
	rt, err := converge.New(converge.Options{
		MQ:    inmem.NewMQ(),
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	chargeOrder := worker.NewTask[ChargeOrder]("charge-order", worker.TaskOpts{Queue: "payments"})

	err = worker.Handle(rt, chargeOrder, func(ctx context.Context, p ChargeOrder) error {
		fmt.Println("charging order", p.OrderID)
		return nil
	}, worker.HandleOpts{Concurrency: 1})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := worker.ProducerFrom(rt)
	if err != nil {
		log.Fatal(err)
	}
	for _, id := range []string{"ORD-4417", "ORD-4418"} {
		if err := chargeOrder.Enqueue(context.Background(), producer, ChargeOrder{OrderID: id}, worker.EnqueueOpts{}); err != nil {
			log.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

```sh
cd examples && go run ./guide/04-worker
```

```
charging order ORD-4417
charging order ORD-4418
```

Both examples use `converge/inmem`. Swapping those storage lines for Redis
— so the guarantees hold across a real deployment instead of inside one
process — is [chapter 6](docs/guide/06-production.md).

## Choosing a surface

If a job's answer to "what needs to happen" is "make sure everything is as
it should be," it belongs on the [reconcile](docs/glossary.md#reconcile)
surface: it runs on a schedule, so a change it never hears about is still
caught by the next pass. If the answer is "do this one specific thing that
just happened," it belongs on the worker surface: the message is the only
copy of that work, so converge retries it until the handler succeeds and
retains it if that never happens.

## Non-goals

- **No exactly-once execution.** converge retries on failure — your
  function has to be safe to run twice, not just once.
- **No workflow orchestration.** A job is one function, not a saga —
  coordinating a sequence of steps across failures is what a tool like
  Temporal is for.
- **No built-in batching.** A reconcile function is called once per thing
  it's checking; group things yourself inside the call if a downstream
  service needs bulk requests.
- **No ordering guarantee between worker messages by default.** Messages
  for the same job can be handled at the same time, in any order —
  getting them in order takes both `OnOneReplica` and `Concurrency: 1`,
  not just one.
- **No scheduling work for a specific clock time, and no cancelling it
  once queued.** A worker delay is relative ("run this in ten minutes");
  cancelling a reminder means checking your own state before you act on
  it.
- **No per-customer rate limits.** A rate limit applies to a whole job,
  not to one customer within it.

Batching's precise shape is on [`reconcile`](docs/reference/reconcile.md);
ordering and delay are on [`worker`](docs/reference/worker.md); rate
limits are on [`converge`][apiref].

## Documentation

- [Guide](docs/guide/index.md) — a numbered path through ten chapters; the
  first six are the core path, from one scheduled job to running in
  production, and the rest cover stale writes, testing, and watching it
  run.
- [Cookbook](docs/cookbook/scenario-a-safety-net.md) — six worked
  scenarios, plus the outbox/inbox recipes.
- [Reference][apiref] — the API reference: `Options`, what a backend
  implements, and where the shipped adapters live.
- [Glossary](docs/glossary.md) — every converge-specific word, defined
  once.

For contributors, [`AGENT.md`](AGENT.md) documents the verification
commands this project runs, and [`CONTEXT.md`](CONTEXT.md) is the
terminology contract the docs and code follow.

## License

MIT — see [LICENSE](LICENSE).

[apiref]: docs/reference/kernel.md
