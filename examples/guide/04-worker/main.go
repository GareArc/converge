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

type Welcome struct {
	Email string `json:"email"`
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

	sendWelcome := worker.NewTask[Welcome]("send-welcome", worker.TaskOpts{Queue: "email"})

	err = worker.Handle(rt, sendWelcome, func(ctx context.Context, p Welcome) error {
		fmt.Println("sending welcome email to", p.Email)
		return nil
	}, worker.HandleOpts{Concurrency: 1})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := worker.ProducerFrom(rt)
	if err != nil {
		log.Fatal(err)
	}
	for _, addr := range []string{"ada@example.com", "grace@example.com"} {
		if err := sendWelcome.Enqueue(context.Background(), producer, Welcome{Email: addr}, worker.EnqueueOpts{}); err != nil {
			log.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
