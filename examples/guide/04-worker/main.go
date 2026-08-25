package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

type ChargeOrder struct {
	OrderID string `json:"order_id"`
}

func main() {
	rt, err := converge.New(converge.Options{
		MQ:    inmem.NewMQ(),
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	chargeOrder := worker.NewTask[ChargeOrder]("charge-order", worker.TaskOpts{Queue: "payments"})

	err = worker.Handle(rt, chargeOrder, func(ctx context.Context, p ChargeOrder) error {
		fmt.Println("charging order", p.OrderID)
		return nil
	}, worker.HandleOpts{Concurrency: 1})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := worker.ProducerFrom(rt)
	if err != nil {
		log.Fatal(err)
	}
	for _, id := range []string{"ORD-4417", "ORD-4418"} {
		if err := chargeOrder.Enqueue(context.Background(), producer, ChargeOrder{OrderID: id}, worker.EnqueueOpts{}); err != nil {
			log.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
