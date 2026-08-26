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

	tracker := reconcile.NewTracker(kv, "apply-price-change")

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "apply-price-change",
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
				fmt.Println("refused: the price changed while we were applying it")
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
		if err := rt.Poke("apply-price-change", "SKU-1001"); err != nil {
			log.Println("poke:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
