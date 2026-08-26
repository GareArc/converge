package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/worker"
	"github.com/redis/go-redis/v9"
)

type welcomeEmail struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

var sendWelcomeEmail = worker.NewTask[welcomeEmail]("send-welcome-email", worker.TaskOpts{})

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	mq := convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{})

	rt, err := converge.New(converge.Options{
		MQ:    mq,
		Lease: convredis.NewLease(rdb),
		KV:    convredis.NewKV(rdb),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = worker.Handle(rt, sendWelcomeEmail, handleWelcomeEmail, worker.HandleOpts{
		Retry: worker.RetryPolicy{MaxAttempts: 5},
	})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := converge.NewProducer(mq, converge.ProducerOpts{})
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		payload := welcomeEmail{
			UserID: fmt.Sprintf("user-%d", i),
			Email:  fmt.Sprintf("user-%d@example.com", i),
		}
		if err := sendWelcomeEmail.Enqueue(ctx, producer, payload, worker.EnqueueOpts{}); err != nil {
			log.Fatal(err)
		}
	}

	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func handleWelcomeEmail(ctx context.Context, payload welcomeEmail) error {
	meta, _ := worker.MetaFromContext(ctx)
	log.Printf("sending welcome email to %s (user %s, attempt %d)", payload.Email, payload.UserID, meta.Attempt)
	return nil
}
