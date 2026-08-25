# Scenario F: Composition roots

> Assumes [chapter 06, going to production](../guide/06-production.md).

A kratos service:

```go
func main() {
    rdb := redis.NewClient(...)

    obs, err := convotel.NewObserver(meter)
    if err != nil {
        log.Fatal(err)
    }

    rt, err := converge.New(converge.Options{
        Namespace: "shop", // isolates this service's leases/KV/queues
        MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
        Lease:     convredis.NewLease(rdb),
        KV:        convredis.NewKV(rdb),
        Observer:  obs,
        Middleware: []converge.Middleware{
            tracing.Middleware(),  // spans around every run, both surfaces
            logging.Middleware(),  // job/ID-scoped logger into ctx
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    app := buildApp(rt, rdb, db) // wire constructs modules; they self-register

    // Read-only introspection and mutating ops are SEPARATE handlers —
    // mount OpsHandler only behind your admin auth. DLQ payload display is
    // opt-in (payloads may contain user data).
    adminMux.Handle("/debug/jobs/", debughttp.OpsHandler(rt, debughttp.OpsOpts{}))
    publicMux.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))

    k := kratos.New(kratos.Server(httpSrv, grpcSrv, convkratos.Server(rt)))
    k.Run()
}
```

`obs, err := convotel.NewObserver(meter)` is checked before it is wired in —
an inline `Observer: convotel.NewObserver(meter)` does not compile, and
`obs, _ := convotel.NewObserver(meter)` compiles but throws the error away;
see [chapter 10's caveat](../guide/10-observability.md) for what a discarded
`Observer` construction error costs later.

A framework-free service (gateway, collector) skips the bridge:
`rt.Run(ctx)` directly.

For what `OpsHandler`'s verbs actually do — [poke](../glossary.md#poke),
run-pass, pause/resume, [DLQ](../glossary.md#dead-letter-dlq) ops, and which
replica's response you get back — see
[Operations reference → Ops verbs](../reference/operations.md#ops-verbs).

## Configuration

Converge has **no config machinery on purpose**. Every tunable is a plain
field; your composition root plumbs values from whatever config system the
service already uses. Wire *quantities* to config (periods, budgets,
concurrency); keep *semantics* in code (queues, codecs, run modes, trigger
wiring).
