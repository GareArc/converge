# Converge

converge gives services **one model for all background work**: a
**level-triggered reconcile surface** (messages are hints; handlers re-read
truth and converge state) and an **edge-triggered worker surface** (the
message is the work; at-least-once with retry and dead-lettering) on one
hexagonal kernel. The kernel owns the ports (MQ, Lease, KV, Clock,
Observer), the runtime, and the shared value types; surfaces and adapters
plug in around it.

```go
import (
    "github.com/GareArc/converge"           // the kernel: runtime, ports, run modes
    "github.com/GareArc/converge/reconcile" // the level-triggered model
    "github.com/GareArc/converge/worker"    // the edge-triggered model
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
```

## Ten-minute tour

The smallest real job: a function that runs hourly on exactly one replica —
what you'd previously build with a cron entry plus a Redis lock:

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/GareArc/converge"
    convredis "github.com/GareArc/converge/adapters/redis"
    "github.com/GareArc/converge/debughttp"
    "github.com/GareArc/converge/reconcile"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    rt, err := converge.New(converge.Options{
        Lease: convredis.NewLease(rdb), // needed by the OnOneReplica run mode
        KV:    convredis.NewKV(rdb),    // engine state: last-fire times, dead-letter marks
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := reconcile.Periodic(rt, "license-refresh", reconcile.Every(time.Hour), refreshLicense); err != nil {
        log.Fatal(err)
    }

    http.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
    go http.ListenAndServe(":6060", nil)

    // Blocks until ctx cancels; then stops intake, drains in-flight work,
    // and releases leases. Returns nil on a clean shutdown.
    if err := rt.Run(ctx); err != nil {
        log.Fatal(err)
    }
}

func refreshLicense(ctx context.Context) error {
    // re-read truth, converge, return nil on success
    return nil
}
```

No Redis handy? `converge/inmem` provides in-memory `MQ`, `Lease`, and `KV` —
swap the two adapter lines and the whole program runs self-contained (single
process only; for development and tests).

What this bought you over cron+lock:

- **One replica runs it** (default run mode): lease + heartbeat + hand-off on
  crash. Other replicas skip, they don't fail.
- **A missed tick isn't lost**: last-fire time is persisted; if the leader
  crashes across the hourly boundary, the new leader runs the missed pass
  (`MissedTick` policy, default `RunOnce`).
- **Failure handling**: an error return retries with exponential backoff and
  jitter — it doesn't wait an hour for the next tick.
- **Panic recovery**: a panic is an error, not a dead service.
- **Introspection**: the job appears at `:6060/debug/jobs` with its schedule,
  last run, and effective settings.
- **A staleness alarm** — once you wire an `Observer`: time-since-last-
  success per job, the metric that catches silently dead loops.

`reconcile.Periodic` is sugar for the one-unit case. The rest of the guide
uses the full `Spec`, which is where IDs, triggers, and the interesting jobs
live — continue with the [full guide](docs/guide/index.md).

## Documentation

- [Guide](docs/guide/index.md) — concepts, the reconcile and worker
  surfaces, run modes, version tracking, testing, operations.
- [Cookbook](docs/cookbook/scenario-a-safety-net.md) — six worked scenarios,
  plus the outbox/inbox recipes.
- [Reference](docs/reference/kernel.md) — the condensed kernel, reconcile,
  worker, and adapters API.
- [`CONTEXT.md`](CONTEXT.md) — the project's canonical terminology; binding
  for identifiers, docs, and messages throughout.

## Verify

```sh
make check                      # gofmt gate, vet, dependency gate, tests with -race
go test -race -count=2 ./...    # what CI effectively runs; -count=2 shakes ordering assumptions
```

Every change must leave `make check` green.

## License

MIT — see [LICENSE](LICENSE).
