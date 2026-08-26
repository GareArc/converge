package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/debughttp"
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

	err = reconcile.Periodic(rt, "sync-inventory", reconcile.Every(time.Second), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
	server := &http.Server{Addr: "localhost:6060", Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
	if err := server.Close(); err != nil {
		log.Println(err)
	}

	for _, s := range rt.Stats() {
		fmt.Printf("%+v\n", s)
	}
}
