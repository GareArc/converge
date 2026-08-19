# Version tracking

Protecting against stale writes: a version is a counter on *intent*.
Producers mark it when desired state changes; the reconciler reads it before
working and marks it applied after.

```go
tracker := reconcile.NewTracker(kv, "app-runner") // namespace must equal Spec.Name —
                                                  // validated at registration

// producer side — after saving new desired state. Do NOT drop this error:
// a lost bump silently disables both stale-write protection and parked-ID
// revival for this change. The schedule does not cover it.
if _, err := tracker.MarkChanged(ctx, id); err != nil {
    return err
}

// reconciler side — the order is a RULE, not a style choice:
v, err := tracker.Latest(ctx, id)         // 1. read the version FIRST
if err != nil {
    return err
}
cfg, err := loadTruth(ctx, id)            // 2. then read truth
if err != nil {
    return err
}
apply(ctx, cfg, v)                        // 3. pass v as the guard on writes
return tracker.MarkApplied(ctx, id, v)    // 4. refused with ErrOutdated if
                                          //    the version moved past v
```

**What this gives you — precisely.** If the downstream write honors `v` as a
condition ("apply only if applied_version ≤ v" — a `WHERE` clause, a CAS, a
conditional API call), stale writes are **prevented**. If the downstream
cannot check a version (most plain REST APIs), the refused `MarkApplied`
gives you **detection** — the engine immediately re-runs with fresh state, so
staleness is corrected, not silently recorded as done. Know which of the two
your job has.

`ErrOutdated` is a control-flow signal: immediate requeue, no backoff.

Jobs whose truth already has a version column implement the one-method
`VersionSource` interface over their own DB instead of `Tracker`. Wiring a
`VersionSource` into the spec also enables **parked-ID revival**: a parked
ID automatically retries when its version advances.

Version tracking is opt-in per job. A job that recomputes cheap state from
truth in one atomic write doesn't need it.

## Namespace rules

`NewTracker(kv, namespace)` validates its namespace: it must be non-empty and
must not contain `/` (the `/`-restriction avoids one job's tracker keys
aliasing another's when names compose). When a `Tracker` is wired into
`Spec.Versions`, its namespace must additionally equal `Spec.Name` —
mismatches fail registration.

Two services sharing one KV backend must use distinct job/tracker names —
distinct intent streams need distinct namespaces; the tracker namespace *is*
the cross-process coordination key, so this is not optional plumbing.

## What "version wakes" means in v1

Version wakes exist for **parked-ID revival only** in v1. A version advance
on an ID that is idle, queued, running, or in backoff does **not** bypass
backoff or pull the run forward the way a poke does — backoff retries
re-read truth on their own schedule, so the gap this leaves is latency
(bounded by the backoff ceiling), never wrongness. Only a parked ID reacts
directly to a version change, by reviving.

## Parked marks

A parked ID's mark is **durable**, written to KV per job, and anchored at
the **run-start version** (the version observed when the run that caused
parking began, not the latest version at park time) — a `MarkChanged` that
lands mid-final-run still revives the ID. A KV outage stalls hint intake the
same way it stalls scheduled passes: both apply a uniform block-and-retry
posture rather than treating hint delivery as best-effort while passes
block.

Poke-triggered revival deletes the durable mark **best-effort**: if the
delete fails, the ID re-parks after the run and self-heals on the next poke
or version advance — no operator action needed.

`MarkApplied` called after `Forget` returns `nil`: once the tracker has
forgotten an ID (typically because the entity was deleted), there is no
newer intent left to lose, so a trailing `MarkApplied` is a no-op rather
than an error.
