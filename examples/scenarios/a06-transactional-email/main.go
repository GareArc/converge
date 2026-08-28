package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

const (
	demoWindow = 2 * time.Second
	namespace  = "notifications"
)

var (
	errInvalidAddress = errors.New("mailer: invalid address")
	errUnsubscribed   = errors.New("mailer: unsubscribed")
)

type EmailJob struct {
	To       string `json:"to"`
	Template string `json:"template"`
}

type mailbox struct {
	unsubscribed map[string]bool

	mu   sync.Mutex
	sent []string
}

func (m *mailbox) send(_ context.Context, j EmailJob) error {
	if !strings.Contains(j.To, "@") {
		return errInvalidAddress
	}
	if m.unsubscribed[j.To] {
		return errUnsubscribed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, j.To)
	return nil
}

func (m *mailbox) delivered() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := slices.Clone(m.sent)
	slices.Sort(out)
	return out
}

var sendEmail = worker.NewTask[EmailJob]("send-email", worker.TaskOpts{})

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mq := inmem.NewMQ()

	rt, err := converge.New(converge.Options{
		Namespace: namespace,
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	mailer := &mailbox{unsubscribed: map[string]bool{"quiet@example.com": true}}

	err = worker.Handle(rt, sendEmail, func(ctx context.Context, j EmailJob) error {
		switch err := mailer.send(ctx, j); {
		case errors.Is(err, errInvalidAddress):
			return worker.Shelve{Reason: "invalid address"}
		case errors.Is(err, errUnsubscribed):
			return worker.Discard{Reason: "unsubscribed"}
		default:
			return err
		}
	}, worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 5}, Timeout: 15 * time.Second})
	if err != nil {
		return err
	}

	p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: namespace})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	for _, j := range []EmailJob{
		{To: "ada@example.com", Template: "welcome"},
		{To: "not-an-address", Template: "welcome"},
		{To: "quiet@example.com", Template: "welcome"},
	} {
		if err := sendEmail.Enqueue(ctx, p, j, worker.EnqueueOpts{}); err != nil {
			return err
		}
	}

	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("delivered: %v\n", mailer.delivered())

	shelf, err := worker.ShelfFrom(rt, sendEmail.Name())
	if err != nil {
		return err
	}
	shelved, err := shelf.List(context.Background())
	if err != nil {
		return err
	}
	for _, m := range shelved {
		fmt.Printf("shelved %s after %d attempt(s): %s\n", m.MessageID, m.Attempt, m.Reason)
	}
	return nil
}
