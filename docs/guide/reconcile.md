# Reconcile

## Triggers and the schedule

A **trigger** is a source of suspicion: it wakes IDs that *might* be out of
sync. It never carries data and never runs work. All triggers are independent
peers feeding one dedup queue.

```go
Triggers: []reconcile.Trigger{
    reconcile.Schedule(reconcile.StringIDs(repo.AllAppIDs), reconcile.Every(5*time.Minute)),
    reconcile.OnMessage("app-events", reconcile.IDFromJSONField("app_id"), reconcile.OnMessageOpts{}),
    myCustomTrigger,
}
```

- **`Schedule(ids, cadence)` is the correctness guarantee** — conditional and
  stated honestly: *every ID the source enumerates is visited once per
  period, provided the pass completes within the period and the source's
  cursor is stable.* The engine tells you when the condition fails
  (`PassOverrun`).
- **Everything else is a latency accelerator.** Delete every other trigger
  and the system stays correct, just slower.
- A reconciler with no periodic trigger fails registration unless the spec
  sets `AllowUnscheduled: true` — a visible, reviewable opt-out.
- Trigger outage degrades to schedule latency, never to wrongness.
  Undecodable hint messages are counted, evented, and dropped. A trigger
  whose `Run` returns is restarted with backoff between failures (doubling
  from a minimum toward a maximum, resetting after a sustained healthy run) —
  a flapping trigger source degrades to schedule latency, it does not spin
  the process.

A **paused** job still runs its schedule pass on schedule and still
enumerates the ID space — pause suppresses wake *application*, not the
enumeration cost. Budget for the schedule's ID-source cost (DB queries,
cursor pages) even while a job is paused; see
[Operations](operations.md#pause-and-resume) for the ops-verb side of pausing
a job cluster-wide.

**Cadences.** `Every(d)` is anchored to a *persisted* epoch in `KV` — a
crashlooping process does not reset the clock, and a missed boundary is made
up per the `MissedTick` policy (`RunOnce` by default: run the one missed
pass, don't replay a backlog). If the persisted last-fire value is corrupt or
unparseable, the schedule re-anchors to now rather than failing closed or
guessing a past time. `Cron(expr, CronOpts{})` uses the standard 5-field
dialect (no seconds, no `@daily`), **UTC by default**; set
`CronOpts{Location: tz}` explicitly for wall-clock schedules — DST is honored
per Go's time package.

The three `MissedTick` policies, precisely: `Skip` skips **all** missed
boundaries and resumes from now — no makeup passes at all. `RunOnce`
(default) collapses any missed backlog into exactly one makeup pass,
regardless of how many boundaries were missed. `Catchup` runs one full pass
**per missed boundary** — never N runs per ID within a pass; a "pass" always
means one complete visit of the ID space for one boundary.

**Which replicas run a trigger?** The engine decides by run mode, and your
trigger code never needs to know: under `OnOneReplica`, triggers run on the
leader only (during lease handoff, hint consumption pauses briefly — the
schedule covers the gap); under `OnAllReplicas`, triggers run everywhere.

**Message delivery mode.** `OnMessageOpts{Delivery: ...}` chooses
`converge.Group` (competing consumers — each message wakes one replica) or
`converge.Broadcast` (every replica sees every message). The default follows
the run mode, and mismatches are registration errors: `OnAllReplicas`
requires `Broadcast` — a per-replica cache invalidated on only one replica is
the bug this knob exists to prevent.

ID extraction is declarative for the common cases:

```go
reconcile.RawID()                                  // the whole payload is the ID
reconcile.IDFromJSONField("workspace_id")          // {"workspace_id": "X"} → ID("X")
reconcile.IDFromJSONFields("tenant_id", "app_id")  // → JoinID of the fields, in order
type IDFunc func(payload []byte) (ID, error)       // escape hatch
```

## Outcomes

`reconcile`'s outcome table (see [Outcome values](02-ids.md)
for the shared mechanics — detection, wrapping, wrong-surface protection):

| Return | Engine does |
|---|---|
| `nil` | success — backoff reset, staleness clock stamped, sleep until next wake |
| `reconcile.CheckAgain{In: d}` | deliberate revisit in `d` — no backoff penalty, not a failure |
| `reconcile.ErrOutdated` (from `MarkApplied`) | lost a race to newer intent — immediate requeue, no backoff, not a failure |
| ctx canceled by shutdown or lease loss | **neutral** — no backoff stamp, no failure count, no `RunCompleted` event, and it does not count in `Stats()`; the ID re-runs after restart/handoff |
| any other error | per-ID exponential backoff + jitter (jitter range **[d/2, d]** of the current delay); counted as failure |
| failure count reaches `DeadLetterAfter` | ID parked; revived by version change or poke (see [IDs — work items](02-ids.md)) |
| panic | recovered, treated as error |

## Version tracking, briefly

Version tracking (`Tracker`, `VersionSource`, stale-write protection, and
parked-ID revival) has its own page: [Version tracking](versions.md).
