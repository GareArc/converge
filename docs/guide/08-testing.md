# 8. Testing your jobs

Chapter 7 opened the second half of this guide: things you reach for once
your situation calls for them, not before every job. This one is for anyone
who has shipped a job with `reconcile.Register` or `worker.Handle` and
cannot afford for it to fail silently the next time somebody touches its
handler. Every earlier chapter proved its own example the same way — `go
run` it, read the lines it prints, decide for yourself whether that looks
right — and that stops being enough the moment someone other than you can
change the code, or the moment "watch it run once" isn't something your
build can do for every commit. If eyeballing `go run` output is still
enough for what you ship, skip ahead to
[9. Running it in production](09-operations.md). By the end of this
chapter you will have a Go test that runs the real engine, not a stand-in
for it, and watched it catch a broken handler the way it would catch a
real bug.

`converge/convergetest` is what makes that test possible: a harness that
runs the real engine — the same registration, the same triggers, the same
[reconcile](../glossary.md#reconcile) loop earlier chapters exercised with
`go run` — against in-memory storage and a clock you control, so a test is
fast and deterministic without faking your handler.

## The code

The job under test is the same shape chapters 1 through 3 used —
`reconcile.Register`, a `Reconciler`, a `Schedule` trigger — with its one
side effect, appending to a slice, made visible to the test through a
`*Store` passed in from outside:

```go title=examples/guide/08-testing/job.go
package testingguide

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
)

type Store struct {
	Synced []string
}

func Register(rt *converge.Runtime, store *Store) error {
	return reconcile.Register(rt, reconcile.Spec{
		Name: "sync-tenants",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			store.Synced = append(store.Synced, string(id))
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(ctx context.Context) ([]string, error) {
				return []string{"acme", "globex"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
}
```

And the test:

```go title=examples/guide/08-testing/job_test.go
package testingguide

import (
	"testing"

	"github.com/GareArc/converge/convergetest"
)

func TestSyncTenantsChecksEveryTenant(t *testing.T) {
	h := convergetest.New(t)
	store := &Store{}
	rt := h.Build(t)
	if err := Register(rt, store); err != nil {
		t.Fatal(err)
	}

	h.Drain(t)
	store.Synced = nil

	h.RunPass(t, "sync-tenants")

	h.AssertReconciled(t, "sync-tenants", "acme")
	h.AssertReconciled(t, "sync-tenants", "globex")
	if len(store.Synced) != 2 {
		t.Fatalf("synced %v, want two tenants", store.Synced)
	}
}
```

`convergetest.New(t)` builds the harness: in-memory storage, an in-memory
[lease](../glossary.md#lease), and a clock you can move by hand instead of
waiting on your machine's real one. `h.Build(t)` builds the runtime those pieces back — the
same `*converge.Runtime` `converge.New` returns everywhere else in this
guide — but does not start it, so `Register(rt, store)` has a window to
register `sync-tenants` before anything can run it. The harness has a
second way to reach the runtime, `h.Runtime(t)`, which starts it
immediately if it isn't already running; reach for that one when you need
the runtime before you've decided what will start it, not when you still
have registration left to do first.

`sync-tenants` carries a schedule, and chapter 1 already showed what that
means the moment a runtime starts running it: converge does not wait out
`Every(time.Hour)` the first time — it runs the job immediately, for every
ID the trigger names. `h.Drain(t)` is the call in this test that starts the
runtime, and it does not return until nothing is left to do, so by the time
it returns, that first automatic pass has already reconciled both `acme`
and `globex` once. `store.Synced = nil` clears what it recorded, so what
the rest of the test checks is only the pass the next line asks for on
purpose.

`h.RunPass(t, "sync-tenants")` forces one complete pass over the job's
whole list right now, without moving the clock and without waiting for
`Every(time.Hour)` to elapse. `h.AssertReconciled` then polls the harness's
own recorded events for a completed, error-free run of each ID — first
`"acme"`, then `"globex"` — and the `len(store.Synced)` check after them
confirms the forced pass covered both tenants and nothing else.

## Run it

```sh
cd examples && go test ./guide/08-testing/ -v
```

```
=== RUN   TestSyncTenantsChecksEveryTenant
--- PASS: TestSyncTenantsChecksEveryTenant (0.02s)
PASS
ok  	github.com/GareArc/converge/examples/guide/08-testing	0.252s
```

## What happened

1. `go test` printed `--- PASS` in about two hundredths of a second — not
   the hour `Every(time.Hour)` names, and not even the couple of seconds
   earlier chapters' `go run` examples spent waiting out a
   `context.WithTimeout`. Nothing in this test touched a real timer; the
   only clock `sync-tenants` ever saw was the one `convergetest.New` built.
2. Nothing printed between `=== RUN` and `--- PASS`: this program has no
   `fmt.Println` of its own, the way every earlier chapter's did. The two
   `AssertReconciled` calls and the `len` check are what stood in for
   reading output by eye.
3. `ok  ...  0.252s` closed the run and `go test` exited 0. By the time
   that line printed, `store.Synced` already held exactly `"acme"` and
   `"globex"` — the assertions ran against a pass that had already
   finished, not one still catching up behind them.

## The principle

Everything `TestSyncTenantsChecksEveryTenant` drives — registration, the
schedule that discovers `"acme"` and `"globex"`, the pass that walks them,
the events `AssertReconciled` polls for — is the same code a production
`rt.Run` drives, unchanged. What the harness supplies in its place is where
that code writes its own bookkeeping and what it measures time against:
`convergetest.New` builds both in memory and hands you the clock as
`h.Clock`, so nothing here waits out a real hour, or a real second, to
prove the schedule works. A broken reconciler in this test breaks the real
reconcile loop — the same one every earlier chapter's `go run` exercised —
not a hand-written stand-in for it, which is why a passing
`TestSyncTenantsChecksEveryTenant` means the same thing a passing
production job does.

## Other shapes

`AssertReconciled` is one of three assertion helpers built the same way:
`AssertParked` polls for an ID converge gave up on, and `AssertEnqueued`
polls for a worker message a handler sent through `worker.ProducerFrom` —
each one fails the test with what it actually saw if the condition it's
waiting for never shows up. A worker job under test looks like a reconcile
job under test: `h.Build(t)`, `worker.Handle` in place of
`reconcile.Register`, and `h.Drain(t)` to run everything due before you
assert on it.

## Try breaking it

Change the reconciler to fail instead of syncing:

```go
Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
	return errors.New("sync failed")
}),
```

(and add `"errors"` to the import block). Run the test again:

```
=== RUN   TestSyncTenantsChecksEveryTenant
    job_test.go:22: convergetest: AssertReconciled("sync-tenants", "acme"): not observed; saw 3 event(s): [{Job:sync-tenants Acquired:true} {Job:sync-tenants Surface:reconcile ID:acme Attempt:1 Duration:0s Err:sync failed} {Job:sync-tenants Surface:reconcile ID:globex Attempt:1 Duration:0s Err:sync failed}]
--- FAIL: TestSyncTenantsChecksEveryTenant (2.01s)
FAIL
FAIL	github.com/GareArc/converge/examples/guide/08-testing	2.217s
FAIL
```

`AssertReconciled` polled for up to two seconds, found no error-free run of
`"acme"`, and failed with the events it actually recorded instead of a bare
timeout: one lease transition for the runtime taking charge, then one
completed run per tenant, each carrying the `"sync failed"` error the
handler returned. That is the same event stream a production `Observer`
would have seen; the harness just handed it to `t.Fatalf` instead of a
metrics pipeline.

## A caveat

Nothing in this test called `time.Sleep`, and nothing should. `sync-tenants`
has an hourly schedule; a test that waited for the schedule's own next tick
by sleeping would either sleep an hour for real or race a guess about how
long is "long enough" — passing on a fast machine, timing out on a loaded
one, for a reason that has nothing to do with whether the job works.
`h.Clock.Advance` is the harness's answer: it moves the fake clock forward
by however much simulated time you ask for, and the schedule fires the
moment that crosses a boundary it's due — deterministically, in the same
couple of hundredths of a second this chapter's own test ran in. A converge
test that sleeps instead of advancing `h.Clock` is a flaky test wearing a
passing one's clothes.

Next: [9. Running it in production](09-operations.md) — looking at a
running job from outside the process, pausing it, and working the
[dead-letter](../glossary.md#dead-letter-dlq) queue.
