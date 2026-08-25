package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func main() {
	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	attempts := 0
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "wait-for-cluster",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			attempts++
			fmt.Printf("checking %s (attempt %d)\n", id, attempts)
			if attempts < 3 {
				return reconcile.CheckAgain{In: 500 * time.Millisecond}
			}
			fmt.Println("cluster is ready")
			return nil
		}),
		AllowUnscheduled: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		<-rt.Ready()
		if err := rt.Poke("wait-for-cluster", "cluster-1"); err != nil {
			log.Println("poke:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
