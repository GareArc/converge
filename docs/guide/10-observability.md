# 10. Seeing what it is doing

Chapter 9 gave you a way to ask a running job questions from outside the
process. This chapter is about the question nobody thinks to ask, because
nothing prompts it: a job that has quietly stopped running altogether. If
your background jobs failing silently would be a bad day for you, this
chapter is for you. A [reconcile](../glossary.md#reconcile) pass that stops
firing, or a worker that stops consuming, does not raise an error — the
process is still up, nothing crashed, there is just nothing happening.
Nothing pages you, because from inside the process nothing looks wrong. By
the end of this chapter you will have watched converge's own events turn
into named numbers in an exporter's output, and you will know which one of
those numbers to alert on so that silence itself gets you paged.

## The code

The whole program:

```go title=examples/guide/10-observability/main.go
package main

import (
	"context"
	"log"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func main() {
	exporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Second))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())

	observer, err := convotel.NewObserver(provider.Meter("converge-docs"))
	if err != nil {
		log.Fatal(err)
	}

	rt, err := converge.New(converge.Options{
		Lease:    inmem.NewLease(),
		KV:       inmem.NewKV(),
		Observer: observer,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Periodic(rt, "sync-inventory", reconcile.Every(500*time.Millisecond), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`sync-inventory` is the same job chapter 1 started with, on the same
schedule. Everything above `converge.New` is plain OpenTelemetry setup, not
anything converge-specific: `stdoutmetric.New` builds an **exporter** — the
thing that takes whatever metrics were collected and sends them somewhere,
here just standard out — and `sdkmetric.NewPeriodicReader` asks that
exporter to report once a second. `sdkmetric.NewMeterProvider` ties the two
together into a **provider** you can hand out named **meters** from — a
meter being just the handle `convotel` registers its instruments against.
None of that is converge. The one line that is:
`convotel.NewObserver(provider.Meter(...))` wraps that meter as a
`converge.Observer` — and passing the result as
`Observer: observer` in `converge.Options` is what actually connects it. A
built observer that never reaches `Options.Observer` reports nothing; more
on that in "A caveat" below.

## Run it

```sh
cd examples && go run ./guide/10-observability
```

The stdout exporter prints one pretty-printed JSON report every second,
four times over the program's three seconds. Here is the first report,
trimmed to the lines that carry the point — the process-identifying
`Resource` block, the histogram's bucket bounds, and every timestamp are
cut:

```json
{
  "ScopeMetrics": [
    {
      "Scope": { "Name": "converge-docs" },
      "Metrics": [
        {
          "Name": "converge.run.duration",
          "Description": "Duration of reconcile and worker runs.",
          "Unit": "s",
          "Data": {
            "DataPoints": [
              {
                "Attributes": [
                  { "Key": "converge.job", "Value": { "Type": "STRING", "Value": "sync-inventory" } },
                  { "Key": "converge.status", "Value": { "Type": "STRING", "Value": "ok" } },
                  { "Key": "converge.surface", "Value": { "Type": "STRING", "Value": "reconcile" } }
                ],
                "Count": 2
              }
            ]
          }
        },
        {
          "Name": "converge.lease.transitions",
          "Description": "Job lease acquisitions and losses.",
          "Data": {
            "DataPoints": [
              {
                "Attributes": [
                  { "Key": "converge.acquired", "Value": { "Type": "BOOL", "Value": true } },
                  { "Key": "converge.job", "Value": { "Type": "STRING", "Value": "sync-inventory" } }
                ],
                "Value": 1
              }
            ]
          }
        }
      ]
    }
  ]
}
```

## What happened

1. The moment `go run` started, `sync-inventory` fired its first pass
   immediately, then again every 500ms after — the same immediate first
   fire every scheduled job in this guide has had since chapter 1. Taking
   charge of the job called `observer.Observe` with a
   [lease](../glossary.md#lease) acquisition once; each completed pass
   called it again with a completed run.
2. About a second in, the periodic reader's first export printed. Both
   `converge` metric names are already there: `converge.run.duration` has
   recorded two runs (`"Count": 2`), each carrying `converge.job`,
   `converge.status`, and `converge.surface`; `converge.lease.transitions`
   shows the one lease acquisition that let `sync-inventory` start
   running at all. converge's own metric names showed up in the exporter's
   output within a second of the job running, without this program doing
   anything to make that happen beyond the four setup lines above.
3. The reader kept exporting once a second — three more reports, each with
   higher counts than the last, while the schedule kept firing underneath.
4. Three seconds in, the context deadline passed, `rt.Run` returned `nil`,
   and the deferred `provider.Shutdown` flushed and stopped the reader.

## The principle

converge does not know about OpenTelemetry, Prometheus, or any other
metrics system — the kernel only ever reports what happened, as a typed
event, to whatever `Observer` you gave it in `Options`. Where those events
go from there is entirely your choice:
`convotel` turns them into OpenTelemetry instruments, as this chapter just
showed, but an `Observer` is one method, `Observe(Event)`, and nothing
stops you writing your own that logs, counts in memory, or feeds a
completely different metrics system instead.

## Other shapes

This chapter's example only shows two of the six things `convotel` can
report — a run and a lease transition, not an ID going
[parked](../glossary.md#parked) or a message reaching the
[dead-letter](../glossary.md#dead-letter-dlq) store — because
`sync-inventory` never fails, so no ID ever parks, and this program runs
no worker job, so no message ever reaches that store. The complete
inventory, and the only names this chapter will use, is this:

| Instrument | Kind | Meaning |
|---|---|---|
| `converge.run.duration` | histogram | How long a run took; carries `converge.status` |
| `converge.parked` | counter | Reconcile IDs parked after repeated failure |
| `converge.dead_letters` | counter | Worker messages moved to the dead-letter store |
| `converge.discarded` | counter | Work items dropped without running |
| `converge.lease.transitions` | counter | Job lease acquisitions and losses |
| `converge.anomalies` | counter | Misconfiguration and guard-rail signals that should stay at zero |

A **histogram** buckets a number by size instead of just counting it —
`converge.run.duration` is the one histogram, and its bucket counts are
what tells you a run took, say, between 50ms and 75ms. A **counter** only
ever goes up; the other five are counters. Each one also carries a set of
key/value **attributes** — you already saw them as `"Attributes"` in "Run
it" above. Beyond the two this chapter's run produced, the attributes
available across all six are `converge.job`,
`converge.surface`, `converge.status`, `converge.queue`, `converge.reason`,
`converge.kind`, and `converge.acquired` — which ones apply depends on the
instrument, exactly as `converge.status` only shows up on
`converge.run.duration` above.

Given that table, here is the one alert every job using `convotel` should
have. It is not "alert when this job last succeeded too long ago" — no
metric here carries a last-success timestamp, because `convotel` exports
only counters and a histogram, never a gauge, so there is nothing to hold
a point-in-time value like that. What you can alert on instead is a number
that has stopped moving: **the count of successful runs of this job has
not increased in the last N minutes.** In terms of the table above, that
is `converge.run.duration`'s count, for this job's `converge.job`,
filtered to `converge.status="ok"`. A job that has stopped running emits
nothing at all — so the only observable trace of that is a number that
used to climb and now doesn't. The exact query, and why `convotel` is
built this way instead of shipping a staleness gauge, is in
[`convotel`'s reference entry](../reference/adapters.md#adapters-otel-convotel).

## Try breaking it

Delete `Observer: observer,` from `converge.Options` above and run it
again:

```sh
go run ./guide/10-observability
```

```
# github.com/GareArc/converge/examples/guide/10-observability
guide/10-observability/main.go:25:2: declared and not used: observer
```

Go's compiler catches the now-unused `observer` before the program ever
starts — that half of the mistake never reaches production. Discard the
value without declaring a new one: `err` already exists in this scope
(from `stdoutmetric.New` above), so change
`observer, err := convotel.NewObserver(...)` to the plain assignment
`_, err = convotel.NewObserver(...)` — no `:=`, since `_` never counts as
the "new variable" a short declaration requires. Run it again. This time
it builds, `sync-inventory` runs on its schedule exactly as before, and
nothing calls `log.Fatal` or panics. Its stdout reports come back like
this (trimmed to the JSON, as above — stderr can separately print one
unrelated line here too, if the periodic reader's own shutdown happens to
land in the middle of an export; that is an OpenTelemetry-internal timing
artifact of shutting the reader down, not anything `Observer` or `Options`
produced, and it is not shown below):

```json
{
  "ScopeMetrics": []
}
```

Every report, for the whole three seconds: an empty `ScopeMetrics` list.
The exporter is still running, the periodic reader is still asking it to
print once a second, and `convotel.NewObserver` still built its six
instruments against the meter — but with no `Observer` in `Options`, none
of `sync-inventory`'s runs ever reached it, so there was never anything
for those instruments to report. The program gives no sign that reporting
has gone missing — "A caveat" below shows the other way this happens.

## A caveat

`convotel.NewObserver` returns two values, `(converge.Observer, error)`,
not one. Writing the field assignment inline —

```go
rt, err := converge.New(converge.Options{
	Observer: convotel.NewObserver(meter),
})
```

— does not compile: the compiler refuses a multi-value call in a
single-value context, the same way it would refuse assigning any two-return
function straight into a struct field. That failure is loud and immediate,
so it gets caught before the code ever ships.

The quieter version compiles clean:

```go
obs, _ := convotel.NewObserver(meter)

rt, err := converge.New(converge.Options{
	Observer: obs,
})
```

`NewObserver` returns `(nil, err)` on failure — a meter that rejects one of
its six instrument registrations, for instance. `obs, _ := ...` throws that
error away. If it happened, `obs` is `nil`, and a `nil` `Observer` in
`Options` is exactly what "Try breaking it" above just showed you: converge
silently swaps in its own no-op, `sync-inventory` keeps running, and
reporting is gone with nothing anywhere saying so. The version worth
writing checks the error, the same as every other line in this chapter's
example:

```go
obs, err := convotel.NewObserver(meter)
if err != nil {
	log.Fatal(err)
}
```

Next: there is no chapter 11. From here, the
[cookbook](../cookbook/scenario-a-safety-net.md) has worked scenarios that
put everything in this guide together — [Scenario F](../cookbook/scenario-f-composition.md)
wires an `Observer` into a full composition root alongside `MQ`, `Lease`,
and `KV` — and the
[reference](../reference/adapters.md#adapters-otel-convotel) has `convotel`'s
complete instrument table, its attribute values, and the exact query behind
the alert in "Other shapes" above.
