# Agent guide

Guidance for coding agents and new contributors. `CONTEXT.md` (canonical
terminology) is part of the contract — read it before changing behavior.

## Rules

1. **No comments in Go source.** The library is small and comments drift as
   versions move. Semantics live in the documentation, `CONTEXT.md`, and the
   contract suites — never in the code. If code seems to need explanation,
   simplify the code or extend the documentation instead.

## What this is

converge gives services one model for all background work: a
**level-triggered reconcile surface** (a message only names an ID; the
handler re-reads the caller's store and converges state) and an
**edge-triggered worker surface** (the message is the work; at-least-once
with retry, shelved when the retries run out) on one hexagonal kernel. The
kernel owns the ports (MQ, Lease, KV, Clock, Observer), the runtime, and the
shared value types; surfaces and adapters plug in around it.

The prose that teaches this lives in `docs/`: the seven-chapter guide
(`docs/guide/`), the six cookbook pages (`docs/cookbook/`), the six-page API
reference (`docs/reference/`), and `docs/glossary.md`. `README.md` and
`docs/index.md` are the two front doors and link all of it.

## Verify

```sh
set -e
make check                      # gofmt gate, vet, dependency gate, race tests, scenarios — every module
for m in . adapters/redis adapters/otel bridges/kratos examples; do  # ./... does not cross module boundaries
  (cd "$m" && go test -race -count=2 ./...)
done
(cd website && pnpm install --frozen-lockfile && pnpm run docs:build)  # the site, when docs/ changed
```

`make check` ends by running all fifteen scenarios under `examples/scenarios`
in parallel (`scripts/scenarios.sh`, about 13s): they are the acceptance
criteria and the source of every tagged snippet in the documentation, so
compiling them is not enough. `a14-foreign-queue` exits 0 without a Redis; it
says so and skips its Redis path.

The VitePress build is the only check that catches a page that renders on
GitHub but breaks the site — an unescaped angle bracket, or a link out of
`docs/` that the site cannot resolve. Run it whenever `docs/` changes.

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
  kind field (`RunMode`, `Outcome`, `State`, `StopCondition`), not exported
  int enums or strings.
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
  CAS clears TTL, redelivery semantics) is pinned by the
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
- **Terminology follows `CONTEXT.md`.** ID (not key), notification (not
  wake, hint or poke), sweep (not pass or tick), failing ID (reconcile) vs
  shelved message (worker), lease (not lock), time limit (not visibility),
  stop condition (not pause or disable). The _Avoid_ lists there are binding
  for identifiers, error strings, and documentation alike — a word listed
  there is wrong in code as well as in prose, and the only sentence allowed
  to use one is the sentence saying the concept is gone.

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
                value types (message.go, runmode.go, rate.go, lifecycle.go),
                runtime (converge.go, runtime.go, jobdeps.go, middleware.go,
                stats.go), producer (producer.go), slog Observer
                (logobserver.go)
internal/sig    sealed control-signal detection
internal/hook   registration seam between kernel and surface engines
internal/wiring the one crossing of hook's any-typed seam: typed runtime
                dependencies, job listing, failing IDs, and option attachment
internal/keys   the KV and queue key layout — every key string is built here
internal/notice the envelope on a reconcile notification: one ID, encoded once
internal/clockctx
                run deadlines derived from converge.Clock, not the wall clock
internal/mw     middleware chain composition
internal/docscheck
                the documentation gates: tagged-snippet equality, terminology
                coverage, heading-slug agreement, link resolution
internal/backoff, internal/tokenbucket, internal/durfmt
                shared engine primitives: the retry curve (jitter is its
                unexported detail), rate limiting, duration rendering
inmem/          stdlib-only port implementations (dev/test; single process)
convergetest/   test harness (Harness/New/NewWith/Options/Build/Runtime/Stop,
                Notify/Drain/Sweep, asserts, Await/AdvanceUntil/AssertStable,
                Recorder, recording MQ wrapper), fake clock; portcheck/ =
                exported port contract suites; versions/ = a fixed
                VersionSource for tests
reconcile/, worker/          surface engines
debughttp/      read-only HTTP introspection (ReadOnlyHandler) over
                wiring.Jobs and wiring.FailingIDs — no mutating routes
adapters/redis (convredis)   MQ over Redis Streams; Lease and KV over plain
                             keys; a list-backed MQ (NewListMQ) for queues
                             another system writes — separate module
adapters/otel (convotel)     Observer over OpenTelemetry metrics; observable
                             gauges read from `Runtime.Stats()` on collection
                             — separate module
examples/                    runnable programs (scenarios/a01..a15, the fifteen
                             acceptance scenarios) — separate module
bridges/kratos (convkratos)  Runtime as a kratos transport.Server — separate module
docs/                        the documentation: guide/, cookbook/, reference/,
                             glossary.md, index.md
website/                     VitePress site built from docs/ — holds no content
docs/superpowers/            local-only working docs — gitignored, never commit
```

The committed source of truth is the code, `CONTEXT.md`, the portcheck
suites, and `docs/`. `internal/docscheck` gates the last of those: a `go`
block tagged `title=<path>` must equal that entire file byte for byte,
`docs/glossary.md` and `CONTEXT.md` must define exactly the same terms,
`README.md` and every page under `docs/guide/` and `docs/cookbook/` must
gloss a term before using it, every internal link and same-page anchor must
resolve, and every heading must produce the same anchor on GitHub as on the
VitePress site.

`AGENT.md` itself is covered by the link and snippet gates but deliberately
not by the gloss gate: it is written for people who have already read
`CONTEXT.md`, so requiring a glossary link before each term would be noise.
Nothing can gate the repo map below against the tree — check it by hand when
you add or remove a package.
