package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func replica(runs *atomic.Int64, lease converge.Lease, kv converge.KV, mode converge.RunMode) *converge.Runtime {
	rt, err := converge.New(converge.Options{Lease: lease, KV: kv})
	if err != nil {
		log.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "refresh-price-cache",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			runs.Add(1)
			return nil
		}),
		RunMode:  mode,
		Triggers: []reconcile.Trigger{reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Second))},
	})
	if err != nil {
		log.Fatal(err)
	}
	return rt
}

func runTwoCopies(mode converge.RunMode) (int64, int64) {
	lease, kv := inmem.NewLease(), inmem.NewKV()
	var first, second atomic.Int64
	runtimes := []*converge.Runtime{
		replica(&first, lease, kv, mode),
		replica(&second, lease, kv, mode),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for _, rt := range runtimes {
		wg.Add(1)
		go func(rt *converge.Runtime) {
			defer wg.Done()
			if err := rt.Run(ctx); err != nil {
				log.Println(err)
			}
		}(rt)
	}
	wg.Wait()
	return first.Load(), second.Load()
}

func main() {
	a, b := runTwoCopies(converge.OnOneReplica)
	fmt.Printf("OnOneReplica: %d runs in total, %d of them on a single copy\n", a+b, max(a, b))

	a, b = runTwoCopies(converge.OnAllReplicas)
	fmt.Printf("OnAllReplicas: %d runs in total, %d on each copy\n", a+b, a)
}
