# Scenario E: A custom trigger

> Assumes [chapter 03, reacting to events](../guide/03-triggers.md).

Any [wake](../glossary.md#wake) source is ~20 lines: `pubsubTrigger` below
turns Redis pub/sub cache invalidation into wakes. Wired into a job
alongside a periodic [schedule](../glossary.md#schedule) — the trigger makes
convergence fast, the schedule makes it correct regardless of whether the
trigger is even reachable:

```go title=examples/cookbook/scenario-e/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

type pubsubTrigger struct {
	rdb     *redis.Client
	channel string
}

func (t *pubsubTrigger) Run(ctx context.Context, wake func(reconcile.ID)) error {
	sub := t.rdb.Subscribe(ctx, t.channel)
	defer sub.Close()
	for {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		wake(reconcile.ID(msg.Payload))
	}
}

func cacheIDs(ctx context.Context) ([]string, error) {
	return []string{"pricing", "catalog"}, nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "refresh-cache",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			fmt.Println("refreshing cache for", id)
			return nil
		}),
		Triggers: []reconcile.Trigger{
			&pubsubTrigger{rdb: rdb, channel: "cache-invalidate"},
			reconcile.Schedule(reconcile.StringIDs(cacheIDs), reconcile.Every(2*time.Second)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

If `sub.ReceiveMessage` returns an error — pub/sub disconnects, Redis is
unreachable — `Run` returns it, and the engine backs off and restarts `Run`
on its own; nothing here needs to handle that itself. No log line and no
metric mark that restart — the same quiet-failure shape
[chapter 6](../guide/06-production.md) shows for a dead backend — so the
schedule above is what keeps this job correct, not visible, while the
trigger is down.

`msg.Payload` is the published ID string; converting it straight to
`reconcile.ID` is safe because an ID is just the name of one thing to look
at, the way [chapter 2](../guide/02-ids.md) introduces it. `wake` itself is
non-blocking, cheap, and bounded: under overload a wake is dropped, but
counted, and the schedule above covers the gap; an empty ID is rejected and
counted the same way, never forwarded silently.

Which replicas run it follows the job's
[run mode](../glossary.md#run-mode) — the trigger never needs to know. If
the source dies, the job gets slower, never wrong: with no live Redis at
all, `refresh-cache`'s schedule alone still checks `pricing` and `catalog`
every two seconds, exactly as it does when pub/sub is healthy.
