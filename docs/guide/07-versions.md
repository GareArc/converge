# 7. Stale writes

Chapters 1 through 6 are the core path: a job on a schedule, a list of IDs
it looks after, ways to check something sooner than the schedule would,
the other kind of job for one-time work, how many of your copies actually
run any of it, and somewhere durable to keep all of that bookkeeping.
Finish those six and you have enough to ship.
This chapter opens the second half of the guide — things you reach for
once your situation calls for them, not before every job. Read it if two
copies of your service could ever apply the same change at once, or if a
slow pass could still be running when the thing it is applying has
already changed again underneath it. If neither can happen to your job,
skip ahead to [8. Testing your jobs](08-testing.md).

Chapter 5 left this open: `converge.OnOneReplica` makes duplicate work
rare, not impossible, so your handler still has to be safe to run twice.
This is that fix, for the specific case where "probably fine" is not
good enough — a write with a real consequence, guarded so a second,
overlapping attempt at it can be caught and refused instead of quietly
landing on top of the first. The new piece is a [version](../glossary.md#version):
a counter on intent, going up every time somebody changes what one ID is
supposed to look like, independent of whether anything has gotten around
to applying it yet. By the end of this chapter you will have watched a
write get refused because the version moved while it was still in
flight.

## The code

The whole program:

```go title=examples/guide/07-versions/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func main() {
	kv := inmem.NewKV()
	rt, err := converge.New(converge.Options{Lease: inmem.NewLease(), KV: kv})
	if err != nil {
		log.Fatal(err)
	}

	tracker := reconcile.NewTracker(kv, "apply-config")

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "apply-config",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			version, err := tracker.Latest(ctx, id)
			if err != nil {
				return err
			}
			fmt.Printf("applying %s at version %d\n", id, version)

			if _, err := tracker.MarkChanged(ctx, id); err != nil {
				return err
			}

			err = tracker.MarkApplied(ctx, id, version)
			if errors.Is(err, reconcile.ErrOutdated) {
				fmt.Println("refused: config changed while we were applying it")
				return nil
			}
			return err
		}),
		Versions:         tracker,
		AllowUnscheduled: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		<-rt.Ready()
		if err := rt.Poke("apply-config", "app-1"); err != nil {
			log.Println("poke:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`tracker := reconcile.NewTracker(kv, "apply-config")` creates a
[Tracker](../glossary.md#tracker): converge's own version counter, kept
in the same KV as everything else. It is wired in twice — into
`Spec.Versions`, so converge knows this job carries version tracking at
all, and closed over directly by the handler, so the handler can read and
write it itself. `AllowUnscheduled: true` is the same opt-out chapter 3
used: `apply-config` has no schedule, only the one `rt.Poke` call below.

Inside the handler, `tracker.MarkChanged(ctx, id)` stands in for a
second, concurrent piece of code: in a real deployment that call lives
wherever new desired state gets saved — an API handler, a config sync —
not inside the reconciler that applies it. Placing it here, between the
version read and the `MarkApplied` call that checks it, is what lets one
process demonstrate the race a second copy would ordinarily cause: by
the time `MarkApplied` runs, the version has already moved past what the
handler read at the top.

## Run it

```sh
cd examples && go run ./guide/07-versions
```

```
applying app-1 at version 0
refused: config changed while we were applying it
```

## What happened

1. Nothing happened until the [poke](../glossary.md#poke) landed:
   `apply-config` has no schedule, so the one `rt.Poke("apply-config",
   "app-1")` call in the goroutine above is the only thing that ever
   calls it.
2. `applying app-1 at version 0` printed: the handler's first read of the
   tracker, for an ID it had never seen before, came back `0`.
3. `refused: config changed while we were applying it` printed
   immediately after, on that same call: the handler's own
   `MarkChanged` had already moved the version to `1` by the time
   `MarkApplied(ctx, id, 0)` ran, so the guarded write was refused.
4. The handler returned `nil` — the refusal was caught and handled, not
   passed back up — so nothing retried. Two seconds later the context
   deadline passed and `rt.Run` returned, and the shell got its prompt
   back with status 0.

## The principle

The order those three tracker calls happen in is a rule, not a style
choice: read the version, then read whatever truth you are about to
apply, then perform that write guarded by the version you already have,
then call `MarkApplied` with that same version. `apply-config` above has
no real downstream to write to, so its `fmt.Printf` line stands in for
both reading truth and the guarded write — a job with an actual write in
the middle keeps all four as separate steps. Get the order wrong and the
guarantee breaks — see "Try breaking it" below. converge does not enforce
the order for you, and `MarkApplied` writes nothing: it re-reads the
current version and returns `ErrOutdated` if it has moved past the one you
pass. It is a check at the end of the sequence, not a record of the work.

What that refusal actually buys you depends on your downstream. If the
write itself can honor the version as a condition — a `WHERE
applied_version <= v` clause, a compare-and-set, a conditional API call —
then a stale write never lands at all: this is **prevention**. Most
writes cannot do that; a plain REST call, once sent, cannot be told to
check a version on the way in. For those, `MarkApplied` refusing is
**detection**: the write may already have gone out, but converge tells you
it was guarded by a version that is no longer current. Know which of the
two your job has before you rely on it.

`apply-config` above chose to swallow the refusal — it caught
`errors.Is(err, reconcile.ErrOutdated)` and returned `nil`, ending the
pass quietly. Returning the error instead, unwrapped, would have
converge requeue the ID right away, without the backoff curve a real
failure gets: `ErrOutdated` is not treated as something having gone
wrong, only as a sign that the ID needs looking at again soon. Which of
the two you want depends on whether the next pass should pick up the
changed config as soon as possible, or wait for whatever normally wakes
this ID.

## Other shapes

`Tracker` is the version source converge provides for free. If your own
truth already has a column that moves whenever intent changes — not a
timestamp like `updated_at`, but a counter that only advances when
somebody changes what the ID should look like — implement the
one-method `VersionSource` interface over that column directly and skip
`Tracker` entirely. Either way, wiring `Spec.Versions` is what turns the
guarantee on.

Wiring `Spec.Versions` also turns on [parked](../glossary.md#parked)-ID
revival: an ID converge gave up on after repeated failures does not come
back on its own — not even on the schedule's own next pass — until
something pokes it. Giving up on a failing ID is itself something you opt
into, by setting `Spec.DeadLetterAfter`; left at its default, a failing ID
retries forever instead, so no job in this guide ever parks one. With
version tracking wired in, a version advance revives a parked ID too, the
same as a poke would.

`Tracker`'s namespace is not cosmetic, either. Registering it into
`Spec.Versions` requires the namespace to equal the job's own `Spec.Name`
— `apply-config` above uses `"apply-config"` for both — because the
namespace is what keeps two jobs' version counters from colliding in the
same KV. `NewTracker` itself also refuses an empty namespace, or one
containing `/`: a slash in the namespace could make one job's tracker
keys look like another's, since the namespace and the ID share the same
slash-separated key underneath. Get either wrong and registration fails
before the job ever runs.

## Try breaking it

Move the `tracker.MarkChanged` call above `tracker.Latest`, so the
concurrent change lands before the version is read instead of after, and
run it again:

```
applying app-1 at version 1
```

No second line. `MarkApplied` no longer refuses, because the version it
is asked to compare against was already `1` by the time it was read —
whatever changed the tracker is folded into what the handler thinks is
"current," instead of showing up as a change at all. The guard can only
catch a version that moves *after* you looked; read the version last and
there is nothing left for it to catch, even though the exact same
concurrent change happened. That is the entire reason "The principle"
above puts "read the version" first, before anything else that could
move it.

## A caveat

This one is about the producer side, not the reconciler: whatever code
saves new desired state and then calls `tracker.MarkChanged` must not
drop that call's error. Losing it is not like losing a message that never
arrives, where the cost is only a wait until the schedule comes back
around. If the bump never lands, the tracker's own record of "latest"
never moves, so nothing about this change is ever detectable as stale —
a pass that already captured the old version has its guarded write
succeed instead of refused, silently, because as far as the tracker is
concerned nothing happened. If the ID was parked, it stays parked:
parked-ID revival needs a poke or a version change, and a dropped
`MarkChanged` produces neither. The regular schedule does not reach in
and fix either of those on its own — losing this one call is permanent
for that change, not something the next pass quietly corrects.

Next: [8. Testing your jobs](08-testing.md) — running your handler
through the real engine instead of faking it.
