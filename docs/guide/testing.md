# Testing

`converge/convergetest` drives the **real engine** over in-memory ports with
a fake clock — no Redis, no sleeps, no flakes:

```go
h := convergetest.New(t)              // inmem MQ/Lease/KV + fake clock
rt, err := converge.New(h.Options())

_, err = credcheck.NewReconciler(rt, fakeRepo) // register the REAL module

h.Wake("workspace-credentials", "ws_42") // inject a hint
h.Drain(t)                               // run everything due, synchronously

h.Clock.Advance(24 * time.Hour)          // next cron boundary passes
h.RunPass(t, "workspace-credentials")    // one full scheduled pass, now

h.AssertReconciled(t, "workspace-credentials", "ws_42")
h.AssertParked(t, "app-runner", "app_13")
h.AssertEnqueued(t, jobs.SendInvite, wantPayload)
h.MQ.FailNextPublish(errBoom)            // failure injection, per port
h.Lease.Expire("app-runner")             // simulate leader loss mid-run
```

Rules: the kit never reimplements engine semantics — dedup, backoff, DLQ
transitions are production code paths; only the ports and clock are fakes.
`converge/inmem` (the same ports without the test harness) also serves local
development. Both live in the core module, stdlib-only.

The statements on this page are not just documentation — they are
**contract-pinned**: `convergetest/guide_test.go` runs this exact shape of
code (registration, `Wake`, `Drain`, `RunPass`, the assertion helpers,
failure injection) against the real reconcile and worker engines, with
per-statement discrimination, so this page cannot silently drift from what
the harness actually does.

## Harness lifecycle

A few properties of `convergetest.New(t)` worth knowing before you build on
it:

- **`Run` starts lazily**, on the first verb that needs the engine running
  (`Wake`, `Drain`, `RunPass`, an assertion) — not at construction. You never
  call `rt.Run` yourself in a harness test.
- **One runtime per harness.** `h.Options()` is meant to construct exactly
  one `*converge.Runtime`; building a second runtime against the same
  harness is not a supported topology.
- **`LeaseTTL` is pinned huge** (effectively "never expires on its own") so
  that a fake clock jumping forward by hours or days in one `Advance` call
  never accidentally expires a lease out from under a running test. Leader
  loss in tests is therefore always **explicit**: call `h.Lease.Expire(name)`
  to simulate a crash or hand-off, rather than relying on TTL decay.
- **`Observer` is owned by the harness's recorder.** The harness wires its
  own `Observer` to capture events for assertions; if your test needs to
  inspect events, use the harness's recorder rather than supplying your own
  `Options.Observer`.

## Custom ports, runtime access, and explicit stop

`convergetest.NewWith(t, convergetest.Options{...})` widens `New(t)` with
namespace, lease TTL, drain timeout, and port overrides:

```go
type Options struct {
	Namespace    string
	LeaseTTL     time.Duration
	DrainTimeout time.Duration
	MQ           func(*convergetest.Clock) converge.MQ
	KV           func(*convergetest.Clock) converge.KV
	Lease        func(*convergetest.Clock) converge.Lease
}
```

`New(t)` is exactly `NewWith(t, Options{})`: every zero value resolves to
today's default — Namespace `"test"`, the pinned huge `LeaseTTL`, and
wrapped in-memory MQ/KV/Lease ports. `MQ`, `KV`, and `Lease` are constructor
funcs over the harness's own fake `*Clock`, so a custom port still runs on
harness time. A constructor can also ignore the `*Clock` it's handed and
close over a port built elsewhere — the supported way to share one MQ, KV,
or Lease across two harnesses (simulating two replicas, or a successor
picking up after a restart).

- **`h.MQ`, `h.KV`, and `h.Lease` are nil when you supply a constructor.**
  Those public fields — `h.MQ` a recording wrapper, `h.KV` and `h.Lease` the
  raw ports — exist only for the default ports the harness builds itself;
  when you hand it a custom `MQ`, `KV`, or `Lease`, the harness isn't
  holding that port, so it never fabricates a stand-in for one it doesn't
  own. Capture the concrete value your constructor returns into an outer
  variable instead. Two consequences follow for a custom `MQ`, both by
  design rather than by crash: **`Drain`'s quiet check degrades to
  `hook.Quiet(rt)` alone**, since `h.MQ.Idle()` is unavailable; and
  **`h.AssertEnqueued` fails the test with a clear message** instead of a
  nil-pointer panic, since it needs `h.MQ`'s recorded publishes. Express
  both MQ-idle and was-it-enqueued conditions through `convergetest.Await`
  instead when testing over a custom `MQ`.
- **`h.Build(t) *converge.Runtime`** is `converge.New(h.Options())` with the
  error check folded in — it Fatals the test on a construction error, so
  call sites need none of their own. It never starts the runtime; use it
  everywhere you'd otherwise write the `rt, err := converge.New(h.Options());
  if err != nil { t.Fatal(err) }` boilerplate, which is most tests — you
  still register reconcilers or worker handlers on the returned runtime
  before anything drives it.
- **`h.Runtime(t) *converge.Runtime`** returns the attached runtime,
  lazy-starting it like any other verb — useful when a surface (`debughttp`,
  for example) needs to mount handlers directly on the runtime rather than
  going through a harness verb. Use `Build` to construct without starting,
  `Runtime` when you want the harness to ensure it's running.
- **`h.Stop(t) error`** cancels the runtime, drives it to a clean stop, and
  returns `Run`'s error instead of failing the test on it. Calling `Stop`
  marks the harness settled, so the `Cleanup` the harness registers on
  first use skips its own stop-and-check — `Stop` is safe to call from
  inside a test body without a later, duplicate failure.
- **After `Stop`, `h.Events()` still works; every other verb Fatals.**
  Reading recorded state is meaningful once the runtime has exited —
  `Events()` returns the recorder's final snapshot. Driving verbs
  (`Runtime`, `Wake`, `Drain`, `RunPass`, and the `Assert*` family) all
  require a live runtime, so calling any of them after `Stop` Fatals with a
  message naming the actual cause — that the harness was explicitly
  stopped — rather than the misleading "exited early" wording a genuine
  mid-test crash produces.
