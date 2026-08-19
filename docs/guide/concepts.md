# Concepts

## IDs — work items

> **`reconcile.ID` names one unit of work** — one workspace, one app, one
> deploy. It is not a Redis key and not a queue name: one queue carries
> messages about thousands of IDs, and the engine dedups, retries, and
> re-checks per ID.

```go
type ID string // a named string type: reconcile.ID("ws_42") is a type
               // conversion, not a function call

id := reconcile.JoinID(tenantID, appID)      // escaping join — always round-trips
tenant, app, err := reconcile.Split2(id)     // errors on wrong arity — never panics
parts := reconcile.SplitID(id)               // raw variant when arity varies
```

**Wake handling — the full state machine.** Per ID, at most one queued entry
and one running execution, with these transitions:

| ID state | hint / schedule wake | manual poke | version change |
|---|---|---|---|
| idle | enqueued | enqueued | enqueued |
| queued | collapsed (no-op) | collapsed | collapsed |
| running | one more run after this one | one more run after | one more run after |
| in backoff | collapsed into the pending retry (does **not** bypass backoff) | **bypasses** backoff, runs now | bypasses, runs now |
| delayed (`CheckAgain`) | pulls the run forward to now | pulls forward | pulls forward |
| parked | dropped + `WakeDiscarded` event | **revives** | **revives** |
| paused | dropped + `WakeDiscarded` event | dropped + event | dropped + event |

Hints never bypass backoff (a hot event source must not defeat backoff
against a failing downstream); pokes and version changes do — they carry
human or producer intent. Nothing is ever dropped *silently*: discarded wakes
are counted and evented.

IDs travel through triggers, logs, metrics, the debug endpoint, and across
language boundaries. Keep them to a single raw ID where possible; use
`JoinID` for composites in Go. An ID that crosses a language boundary must be
a single raw ID or a documented encoding.

## ID sources — "which IDs exist?"

A scheduled pass must visit *every* ID each period, so it needs an
enumeration of the ID space, consulted fresh each time (never a boot-time
snapshot). Three constructors:

```go
// 1. The job IS the unit — one anonymous work item.
reconcile.SingleID()

// 2. DEFAULT: return the whole list, called fresh at every pass.
//    Right up to tens of thousands of IDs. StringIDs keeps your repo layer
//    converge-free — it returns plain []string.
reconcile.StringIDs(repo.AllWorkspaceIDs) // func(ctx) ([]string, error)
reconcile.IDs(fn)                         // same, for func(ctx) ([]reconcile.ID, error)

// 3. Large ID spaces: cursor pagination. Return a page and the next cursor;
//    empty cursor = done. The cursor MUST be a stable keyset cursor
//    (e.g. WHERE id > $last ORDER BY id) — offset pagination over a mutable
//    table skips rows and silently voids the visit-every-ID guarantee.
reconcile.IDsByPage(func(ctx context.Context, cursor string) ([]reconcile.ID, string, error) {
    rows, next, err := repo.PageDeployments(ctx, cursor, 200)
    return deployIDs(rows), next, err
})
```

The engine holds one page in memory and spreads pages across the schedule
window. If a source errors mid-pass, the pass resumes from the last good
cursor on retry; if a pass is still running when the next is due, the next is
skipped and a `PassOverrun` event fires — sustained overruns mean the period,
`Concurrency`, or the handler needs attention.

## Outcome values

Your return value is your **report to the engine**. Special outcomes are
returned as values — struct literals that implement `error` — the same way
`fs.SkipDir` steers a directory walk. Convention used throughout: a budget of
N means the handler runs **at most N times**.

The concrete outcome tables are per surface — see
[reconcile outcomes](reconcile.md) and [worker outcomes](worker.md) — but the
mechanics below are shared by both.

Guard rails on the no-backoff outcomes: zero or sub-floor delays
(`CheckAgain{}`, `Snooze{}`, `ErrOutdated` loops) are floored at 250ms with
jitter (range **[250ms, 375ms]**), and more than 10 consecutive no-backoff
requeues of one ID fall back to normal backoff with an event — a race you
lose every time at CPU speed is a bug, not control flow. Backoff itself
jitters in **[d/2, d]** for the current delay `d`.

Outcome values are detected with `errors.As` (structs) and `errors.Is`
(`ErrOutdated`); value and pointer returns both match; when a wrapped chain
contains several, the outermost signal wins. Wrapping with
`fmt.Errorf("context: %w", err)` is always safe. Outcome structs require
keyed fields (`Snooze{In: d}`) — unkeyed literals do not compile.

**Wrong-surface protection.** Outcome types implement an unexported converge
signal interface — only converge's own types can be signals, and each engine
knows its own. A signal from the *other* surface (`worker.Snooze` out of a
reconciler, `reconcile.CheckAgain` out of a worker) is a programming error:
the reconcile ID is parked / the worker message goes to the DLQ, immediately,
with a `WrongSurfaceSignal` event — never silently reinterpreted, never
retried into oblivion.

## Terminology map

This is a condensed mapping for readers who know the prior art; the
project's canonical, binding terminology lives in
[`CONTEXT.md`](../../CONTEXT.md), and this guide follows it throughout.

| Converge | Known elsewhere as |
|---|---|
| `Schedule` (the trigger) | resync / periodic sweep (Kubernetes) |
| `CheckAgain{In: d}` | `RequeueAfter` (controller-runtime) |
| run modes | coordination postures; `OnOneReplica` = leader election |
| `Version` / `Tracker` | generation / observedGeneration (k8s), fencing token (Kleppmann) |
| `reconcile.ID` | workqueue key (Kubernetes) |
| queue | topic (Kafka) |
| parked (reconcile) / dead-lettered (worker) | both are "dead-lettering" elsewhere; converge keeps the words apart because revival differs — a parked ID revives on poke or version change, a DLQ'd message only via ops requeue |
