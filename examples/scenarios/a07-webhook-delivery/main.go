package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

const (
	demoWindow    = 2 * time.Second
	namespace     = "payments"
	merchantDelay = 250 * time.Millisecond
)

type Webhook struct {
	ID    string `json:"id"`
	Event string `json:"event"`
}

type merchantEndpoint struct {
	mu        sync.Mutex
	attempts  map[string]int
	delivered []string
}

type endpointResponse struct {
	StatusCode int
	RetryAfter time.Duration
}

func newMerchantEndpoint() *merchantEndpoint {
	return &merchantEndpoint{attempts: map[string]int{}}
}

func (e *merchantEndpoint) post(_ context.Context, w Webhook) (endpointResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempts[w.ID]++
	if e.attempts[w.ID] == 1 {
		return endpointResponse{StatusCode: http.StatusTooManyRequests, RetryAfter: merchantDelay}, nil
	}
	e.delivered = append(e.delivered, w.ID)
	return endpointResponse{StatusCode: http.StatusOK}, nil
}

func (e *merchantEndpoint) report() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	lines := make([]string, 0, len(e.attempts))
	for id, n := range e.attempts {
		lines = append(lines, fmt.Sprintf("%s attempts=%d delivered=%t", id, n, slices.Contains(e.delivered, id)))
	}
	slices.Sort(lines)
	return lines
}

var deliverWebhook = worker.NewTask[Webhook]("deliver-webhook", worker.TaskOpts{})

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

	hooks := newMerchantEndpoint()

	err = worker.Handle(rt, deliverWebhook, func(ctx context.Context, w Webhook) error {
		resp, err := hooks.post(ctx, w)
		if err == nil && resp.StatusCode == http.StatusTooManyRequests {
			return worker.Snooze{In: resp.RetryAfter}
		}
		return err
	}, worker.HandleOpts{
		RateLimit: converge.Rate{Events: 50, Per: time.Second},
		Retry:     worker.RetryPolicy{MaxAge: 24 * time.Hour},
	})
	if err != nil {
		return err
	}

	p, err := deliverWebhook.NewProducer(rt.Scope())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	for _, w := range []Webhook{
		{ID: "wh-1", Event: "charge.succeeded"},
		{ID: "wh-2", Event: "charge.refunded"},
		{ID: "wh-3", Event: "payout.paid"},
	} {
		if err := p.Enqueue(ctx, w, worker.EnqueueOpts{}); err != nil {
			return err
		}
	}

	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range hooks.report() {
		fmt.Println(line)
	}
	return nil
}
