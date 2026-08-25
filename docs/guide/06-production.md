# 6. Going to production

Every chapter so far has kept converge's bookkeeping in memory. That was the
right trade for learning — nothing to install, nothing to clean up — and it
is the wrong trade for a deployment, because in-memory bookkeeping is
private to one process. Two replicas each keep their own, so each one
believes it is in charge and each one runs the job.

This chapter is the other end of that. It is not a two-line swap dressed up
as a service: it is the whole composition root of something you could
actually deploy — configuration from the environment, all three ports
backed by Redis, metrics, a middleware that logs every run, an HTTP
endpoint to inspect it with, and a shutdown that finishes what it started.
By the end you will have run it, restarted it mid-interval and watched it
refuse to double-fire, and looked inside it while it was running.

It is longer than the chapters before it. That length is the point: this is
what the previous five chapters were building toward, and nothing in it is
decoration.

## The code

Configuration first, in its own file, because a service that hardcodes its
Redis address is a service you cannot deploy twice:

```go title=examples/guide/06-production/config.go
package main

import (
	"os"
	"time"
)

type Config struct {
	RedisAddr    string
	Namespace    string
	DebugAddr    string
	SyncEvery    time.Duration
	DrainTimeout time.Duration
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func configFromEnv() Config {
	return Config{
		RedisAddr:    env("REDIS_ADDR", "localhost:6379"),
		Namespace:    env("CONVERGE_NAMESPACE", "shop"),
		DebugAddr:    env("DEBUG_ADDR", "localhost:6060"),
		SyncEvery:    10 * time.Second,
		DrainTimeout: 20 * time.Second,
	}
}
```

Every value has a working default, so the program runs with no environment
set at all — and every value can be overridden, so the same binary runs in
staging and production. That is the whole trick, and it is the reason this
is a separate file rather than a literal in the middle of `main`.

Then the composition root:

```go title=examples/guide/06-production/main.go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type ChargeOrder struct {
	OrderID string `json:"order_id"`
}

var chargeOrder = worker.NewTask[ChargeOrder]("charge-order", worker.TaskOpts{Queue: "payments"})

func logRuns(next converge.Handler) converge.Handler {
	return func(ctx context.Context, run converge.Run) error {
		start := time.Now()
		err := next(ctx, run)
		log.Printf("%s/%s id=%q took=%s err=%v",
			run.Surface, run.Job, run.ID, time.Since(start).Round(time.Millisecond), err)
		return err
	}
}

func newObserver() (converge.Observer, func(), error) {
	exporter, err := stdoutmetric.New()
	if err != nil {
		return nil, nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Minute))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	observer, err := convotel.NewObserver(provider.Meter("shop"))
	if err != nil {
		return nil, nil, err
	}
	return observer, func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			log.Println("metrics shutdown:", err)
		}
	}, nil
}

func registerJobs(rt *converge.Runtime, rdb *redis.Client, cfg Config) error {
	skus := func(ctx context.Context) ([]string, error) {
		return []string{"SKU-1001", "SKU-1002"}, nil
	}

	err := reconcile.Register(rt, reconcile.Spec{
		Name: "sync-inventory",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			inbound, err := rdb.Get(ctx, "warehouse:"+string(id)+":inbound").Int()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if inbound > 0 {
				return rdb.Decr(ctx, "warehouse:"+string(id)+":inbound").Err()
			}
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.OnMessage("stock-events", reconcile.IDFromJSONField("sku"), reconcile.OnMessageOpts{}),
			reconcile.Schedule(reconcile.StringIDs(skus), reconcile.Every(cfg.SyncEvery)),
		},
	})
	if err != nil {
		return err
	}

	return worker.Handle(rt, chargeOrder, func(ctx context.Context, p ChargeOrder) error {
		return nil
	}, worker.HandleOpts{
		Concurrency: 4,
		Retry:       worker.RetryPolicy{MaxAttempts: 5},
	})
}

func main() {
	cfg := configFromEnv()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	observer, shutdownMetrics, err := newObserver()
	if err != nil {
		log.Fatal(err)
	}
	defer shutdownMetrics()

	rt, err := converge.New(converge.Options{
		Namespace:    cfg.Namespace,
		MQ:           convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
		Lease:        convredis.NewLease(rdb),
		KV:           convredis.NewKV(rdb),
		Observer:     observer,
		Middleware:   []converge.Middleware{logRuns},
		DrainTimeout: cfg.DrainTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := registerJobs(rt, rdb, cfg); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
	debug := &http.Server{Addr: cfg.DebugAddr, Handler: mux}
	go func() {
		if err := debug.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println("debug server:", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("converge starting: namespace=%q redis=%s debug=%s", cfg.Namespace, cfg.RedisAddr, cfg.DebugAddr)
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
	log.Println("converge stopped cleanly")

	if err := debug.Shutdown(context.Background()); err != nil {
		log.Println("debug shutdown:", err)
	}
}
```

Read it as four groups rather than eighty lines.

**The ports.** `MQ`, `Lease` and `KV` are all `convredis` now, pointed at
one client. These are the three lines chapters 1 through 5 kept in memory,
and swapping them is what makes every promise this guide has made hold
across a deployment rather than inside a process.

**What watches it.** `Observer` is `convotel`, turning converge's events
into OpenTelemetry instruments — [chapter 10](10-observability.md) is
entirely about this, and here it is simply in place. `Middleware` wraps
every run of every job on both surfaces; `logRuns` is fifteen lines and
gives you a line per run with the job, the ID, the duration and the error.

**The jobs.** One of each. `sync-inventory` is chapter 3's job, triggered
both ways — a message for latency, a schedule for certainty. `charge-order`
is chapter 4's, with a real retry budget. `registerJobs` takes the runtime
and returns an error, so the wiring is testable without `main`.

**Getting in and out.** `debughttp.ReadOnlyHandler` exposes what the
runtime knows over HTTP. `signal.NotifyContext` turns SIGINT and SIGTERM
into the cancellation `rt.Run` already understands, and `DrainTimeout` is
how long converge will wait for in-flight work before giving up on it.

## Run it

```sh
docker run --rm --name converge-guide -p 6379:6379 redis:7-alpine
```

```sh
cd examples && go run ./guide/06-production
```

```text
2026/08/25 16:30:49 converge starting: namespace="shop" redis=localhost:6379 debug=localhost:6060
2026/08/25 16:30:49 reconcile/sync-inventory id="SKU-1001" took=1ms err=<nil>
2026/08/25 16:30:49 reconcile/sync-inventory id="SKU-1002" took=0s err=<nil>
2026/08/25 16:30:59 reconcile/sync-inventory id="SKU-1001" took=0s err=<nil>
2026/08/25 16:30:59 reconcile/sync-inventory id="SKU-1002" took=0s err=<nil>
^C
2026/08/25 16:31:02 converge stopped cleanly
```

One report from the metrics exporter also prints at exit, when the deferred
shutdown flushes it. It is a page of JSON and it is
[chapter 10](10-observability.md)'s subject, so it is left out above.

## What happened

1. `configFromEnv` read nothing from the environment and fell back to every
   default, so the service came up against `localhost:6379` in the `shop`
   namespace.
2. The runtime took the [lease](../glossary.md#lease) and the schedule
   fired at once, sweeping both SKUs. Every one of those runs went through
   `logRuns` on the way, which is where those lines come from — the
   reconciler itself prints nothing.
3. Ten seconds later the schedule came round again. Nothing else did
   anything: no message arrived on `stock-events`, and no order was
   enqueued, so `charge-order` sat idle with nothing to do. An idle worker
   job is not a broken one.
4. Ctrl-C cancelled the context `signal.NotifyContext` owns. `rt.Run`
   returned `nil` — a clean stop, not a failure — the lease was released,
   and `converge stopped cleanly` printed.

## Surviving a restart

[Chapter 1](01-first-job.md) mentioned that converge keeps a record of when
each schedule last fired, and that it lives somewhere durable only once you
get here. This is what that record buys: restart the service mid-interval,
and converge works out that the interval has not elapsed instead of firing
again because the process is new. Now there is a Redis to remember it in,
so it can be tested.

Start it, let the first sweep happen, and stop it three seconds in:

```text
2026/08/25 16:31:52 converge starting: namespace="shop" redis=localhost:6379 debug=localhost:6060
2026/08/25 16:31:52 reconcile/sync-inventory id="SKU-1001" took=1ms err=<nil>
2026/08/25 16:31:52 reconcile/sync-inventory id="SKU-1002" took=0s err=<nil>
^C
2026/08/25 16:31:55 converge stopped cleanly
```

Start it again straight away. It comes up, says so — and then does nothing
at all for seven seconds:

```text
2026/08/25 16:31:55 converge starting: namespace="shop" redis=localhost:6379 debug=localhost:6060
2026/08/25 16:32:02 reconcile/sync-inventory id="SKU-1001" took=3ms err=<nil>
2026/08/25 16:32:02 reconcile/sync-inventory id="SKU-1002" took=0s err=<nil>
```

`16:31:52` plus ten seconds is `16:32:02`. The restart did not reset the
clock and did not earn an extra sweep: converge read the schedule's record
on the way up, saw that three of the ten seconds had been used, and waited
out the other seven. Do this with a deployment that restarts every pod in
sequence and the difference is a job that fires once per interval instead
of once per pod.

The record it read is one key:

```sh
docker exec converge-guide redis-cli --scan --pattern '*sched*'
```

```text
shop/converge/reconcile/sync-inventory/sched/1/last
```

The `1` is the trigger's index in the `Triggers` list, and the schedule is
the second entry — `OnMessage` is the first. Change the order of that list
and the key changes with it.

## Looking inside it

While it is running, in another terminal:

```sh
curl -s localhost:6060/debug/jobs/ | python3 -m json.tool
```

```json
{
    "jobs": [
        {
            "job": "sync-inventory",
            "surface": "reconcile",
            "run_mode": "OnOneReplica",
            "queue": "",
            "paused": false,
            "settings": {
                "concurrency": "1",
                "schedule": "every 10s",
                "triggers": "on-message stock-events + schedule"
            },
            "queue_depth": 0,
            "parked": 0,
            "last_success": "2026-08-25T23:33:49.74309Z",
            "consecutive_fails": 0
        },
        {
            "job": "charge-order",
            "surface": "worker",
            "run_mode": "SplitAcrossReplicas",
            "queue": "payments",
            "paused": false,
            "settings": {
                "concurrency": "4",
                "retry": "5 attempts, backoff 1s..15m, max-age 24h",
                "schema-version": "1",
                "visibility": "5m"
            },
            "queue_depth": 0,
            "parked": 0,
            "last_success": "",
            "consecutive_fails": 0
        }
    ]
}
```

Both jobs are listed, each reporting what it actually is rather than what
the config said — the [run mode](../glossary.md#run-mode) it resolved to,
the triggers that are
attached, when it last succeeded. `ReadOnlyHandler` is exactly what its
name says; the handler that can *change* a running job is
[chapter 9](09-operations.md).

## The principle

converge asks for four things — an `MQ`, a `Lease`, a `KV`, and optionally
an `Observer` — and has opinions about none of them. Everything in this
file that is not those four lines is yours: which Redis, which metrics
backend, what your middleware logs, where the debug endpoint listens,
whether config comes from the environment or a file or a flag.

That is why the swap from chapter 1 to here is a swap and not a rewrite.
The jobs did not change. `sync-inventory` is the same reconciler with the
same triggers; `charge-order` is the same handler. What changed is where
the bookkeeping lives, and the bookkeeping was never something your code
touched.

The corollary is worth saying plainly: **a converge job is not coupled to
Redis.** It is coupled to three interfaces. Chapters 1 through 5 satisfied
them from memory, this chapter satisfies them from Redis, and
[chapter 8](08-testing.md) satisfies them from a test harness with a fake
clock — the same jobs, unmodified, in all three.

## Try breaking it

Point it at a Redis that is not there:

```sh
REDIS_ADDR=localhost:6399 go run ./guide/06-production
```

```text
2026/08/25 16:32:28 converge starting: namespace="shop" redis=localhost:6399 debug=localhost:6060
2026/08/25 16:32:28 dial tcp [::1]:6399: connect: connection refused
exit status 1
```

It refuses to run. That is the good outcome, and it is worth dwelling on
for a second, because the alternative would be much worse: a service that
came up, could not reach its lease, and therefore quietly believed it was
in charge. Every replica would believe the same thing. The failure you want
from a misconfigured lease is the loud one at startup, and `log.Fatal` on
`rt.Run`'s return is what gives it to you.

Note where it failed. `converge.New` succeeded — building the adapters
does not talk to Redis — and so did `registerJobs`. The connection is not
attempted until `rt.Run` tries to take the lease, so the error arrives from
`Run`, not from construction.

## A caveat

`DrainTimeout` is a deadline, not a promise. On shutdown converge stops
taking new work and waits up to that long for what is already running to
finish; a handler still going when it expires is abandoned, not killed —
the goroutine keeps running, and the process exits underneath it.

This matters most for the worker surface, where an abandoned handler is
also an unacked message: it goes back on the queue and is delivered again,
to whichever replica is still up. That is at-least-once doing exactly what
[chapter 4](04-worker.md) said it would, and it is the second reason that
chapter asked for an idempotency key. Set `DrainTimeout` from how long your
slowest handler actually takes, and make sure your deployment's own
termination grace period is longer — Kubernetes' default is 30 seconds, so
a `DrainTimeout` above that is a number that never gets used.

Next: [7. When something else changed it first](07-versions.md) — what
happens when the thing you are reconciling moves while you are reconciling
it.
