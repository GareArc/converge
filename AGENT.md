# AGENT.md

Guidance for coding agents and new contributors. `CONTEXT.md` (canonical
terminology) is part of the contract — read it before changing behavior.

## Rules

1. **No comments in Go source.** The library is small and comments drift as
   versions move. Semantics live in the documentation, `CONTEXT.md`, and the
   contract suites — never in the code. If code seems to need explanation,
   simplify the code or extend the documentation instead.

## What this is

converge gives services one model for all background work: a
**level-triggered reconcile surface** (messages are hints; handlers re-read
truth and converge state) and an **edge-triggered worker surface** (the
message is the work; at-least-once with retry and dead-lettering) on one
hexagonal kernel. The kernel owns the ports (MQ, Lease, KV, Clock,
Observer), the runtime, and the shared value types; surfaces and adapters
plug in around it.

## Verify

```sh
make check                      # gofmt gate, vet, dependency gate, race tests — every module
go test -race -count=2 ./...    # per module (., adapters/redis, examples); ./... does not cross
                                # module boundaries, workspace or not
```

Every change must leave `make check` green. There is no change small enough
to skip it.

## Dependency policy (CI-enforced)

The core module imports **stdlib, this module, and `robfig/cron/v3`
(parse-only) — nothing else**. `scripts/depcheck.sh` fails the build
otherwise. Do not add dependencies to the core; backend integrations belong
in separate modules under `adapters/` and framework glue under `bridges/`
(each with its own `go.mod`). `inmem` exists so everything runs and tests
without external services.

## Design rules

- **Sealed value types.** Bounded choices are structs with an unexported
  kind field (`RunMode`, `DeliveryMode`), not exported int enums or strings.
  Callers can only use the named values; the set cannot be forged or
  extended from outside.
- **Zero values are honest defaults.** An unset option means "the documented
  default" (`LeaseTTL` 30s, nil `Clock` → wall clock, zero `Rate` →
  unlimited). You cannot configure an actual zero. Never fabricate a name or
  value for absent state — `Surface(0).String()` is `"unknown"`, not a guess.
- **Absence is not an error.** `KV.Get` returns `(val, ok, err)`; deleting
  an absent key succeeds; `Lease.TryAcquire` returns `(nil, false, nil)`
  when held elsewhere. Errors mean the operation itself failed.
- **Ports are contracts.** What implementations must do (backlog retention,
  CAS clears TTL, visibility semantics) is pinned by the
  `convergetest/portcheck` subtests, which every adapter runs identically.
  If you change port behavior, update the suite in the same commit.
- **Sealed seams stay sealed.** The engine `job` interface is unexported;
  surfaces register through `internal/hook`. Control-flow outcomes embed the
  sealed `internal/sig.Signal`. Only `JobDeps` is exported from the seam. Do
  not export these to make something "easier to test".
- **Semantic time goes through `converge.Clock`.** Wall-clock use in
  production code is a bug outside of scheduling internals (poll intervals).
  Tests drive `convergetest.Clock.Advance`; `time.Sleep`-based assertions
  are forbidden (bounded polling helpers like `assertNoDelivery` are the
  only exception).
- **Locks never wrap foreign code.** Mutations hold the mutex; callbacks
  (`deliver`, job methods) run outside it. Snapshot state under the lock,
  then call.
- **Run returns nil on clean shutdown.** A non-nil return from
  `Runtime.Run` is always a real failure. Jobs return nil on clean stop;
  `context.Canceled` is tolerated, nothing else is.
- **Options structs, no magic values.** Multi-parameter surfaces take an
  options struct (`portcheck.KVOptions{Advance: …}`); bounded values get
  named constants (`DefaultVisibility`), not inline literals.
- **Extract at two occurrences, not one.** No abstractions for hypothetical
  future consumers; inline until the second real caller exists.

## Style

- **gofmt is authoritative for Go files** (tabs; run it, don't argue with
  it). Non-Go files use 2-space indentation per `.editorconfig`; Makefile
  recipes are hard tabs.
- **No comments (Rule 1).** Names and structure carry intent; documentation
  and the contract suites carry semantics.
- **Terminology follows `CONTEXT.md`.** ID (not key), poke vs hint, parked
  (reconcile) vs dead-lettered (worker), lease (not lock), queue (not
  topic/stream). The _Avoid_ lists there are binding for identifiers, docs,
  and messages.

## Testing

- TDD: write the failing test first; table-driven where shapes repeat.
- Everything runs under `-race`; every shared mutation is lock-protected,
  and tests order cross-goroutine reads through channels.
- Port behavior belongs in the exported `portcheck` suites (so adapters
  inherit it); implementation-specific behavior (e.g. inmem's stale-delivery
  fencing) belongs in that package's own tests.
- Capability subtests auto-skip via type assertion (`base.(converge.GroupConsumer)`)
  and via nil option hooks (`Advance == nil` skips time-dependent subtests).
- `adapters/redis` integration tests run when `CONVREDIS_TEST_ADDR` is set
  and **flush DB 9** of that instance on every open — point it only at a
  disposable Redis (CI's service container, a local throwaway).
- Tests future-proof inputs: unknown enum values, foreign types through the
  registration seam, and expired/stale handles all have explicit cases.

## Commits

Conventional prefixes (`feat:` / `fix:` / `test:` / `chore:`), subject only
unless a body genuinely adds context. **Never add attribution or session
trailers of any kind** (no `Co-Authored-By`, no generator tags, no session
URLs). Never push without explicit maintainer approval.

## Repo map

```
/               kernel: ports (mq.go, lease.go, kv.go, clock.go, observer.go),
                value types (message.go, runmode.go, rate.go), runtime
                (converge.go, runtime.go, jobdeps.go, middleware.go, stats.go)
internal/sig    sealed control-signal detection
internal/hook   registration seam between kernel and surface engines
internal/ctl    control-plane primitives: ops verbs, Request/Response, KV/queue keys
internal/mw     middleware chain composition
internal/backoff, internal/tokenbucket, internal/durfmt, internal/pausegate
                shared engine primitives: jitter, rate limiting, duration
                rendering, pause gating
inmem/          stdlib-only port implementations (dev/test; single process)
convergetest/   test harness (Harness/New/Options, Drain/Wake/RunPass, asserts,
                Await/AdvanceUntil/AssertStable, Recorder, recording MQ and
                Lease wrappers), fake clock; portcheck/ = exported port
                contract suites
reconcile/, worker/          surface engines
debughttp/      HTTP introspection (ReadOnlyHandler) and ops (OpsHandler) over
                hook.Inspect / hook.ControlDispatch / worker.DLQFrom
adapters/redis (convredis)   MQ/Lease/KV/ListTrigger over Redis Streams — separate module
examples/                    runnable programs (tour, worker) — separate module
bridges/                     separate modules (later plans)
docs/superpowers/            local-only working docs — gitignored, never commit
```

The API spec for surfaces still landing lives in local working docs; the
committed source of truth is the code, its doc comments, `CONTEXT.md`, and
the portcheck suites.
