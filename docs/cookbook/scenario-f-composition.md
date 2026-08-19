# Scenario F: Composition roots

A kratos service:

```go
func main() {
    rdb := redis.NewClient(...)

    rt, err := converge.New(converge.Options{
        Namespace: "enterprise-server", // isolates this service's leases/KV/queues
        MQ:        convredis.NewStreamsMQ(rdb),
        Lease:     convredis.NewLease(rdb),
        KV:        convredis.NewKV(rdb),
        Observer:  convotel.NewObserver(meter),
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

A framework-free service (gateway, collector) skips the bridge:
`rt.Run(ctx)` directly.

For what `OpsHandler`'s verbs actually do — poke, run-pass, pause/resume,
DLQ ops, and which replica's response you get back — see
[Operations → Ops verbs](../guide/operations.md).

## Configuration

Converge has **no config machinery on purpose**. Every tunable is a plain
field; your composition root plumbs values from whatever config system the
service already uses. Wire *quantities* to config (periods, budgets,
concurrency); keep *semantics* in code (queues, codecs, run modes, trigger
wiring).
