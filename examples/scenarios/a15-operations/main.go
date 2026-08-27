package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

const (
	demoWindow = 15 * time.Second
	leaseTTL   = 3 * time.Second
	pollStep   = 20 * time.Millisecond
	namespace  = "payments"
	statsPath  = "/debug/jobs"
	readyPath  = "/healthz/ready"
)

var errEndpointRejected = errors.New("hooks: endpoint rejected the payload")

type Webhook struct {
	ID    string `json:"id"`
	Event string `json:"event"`
}

type merchantEndpoint struct {
	mu        sync.Mutex
	broken    bool
	delivered []string
}

func (e *merchantEndpoint) post(_ context.Context, w Webhook) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.broken {
		return errEndpointRejected
	}
	e.delivered = append(e.delivered, w.ID)
	return nil
}

func (e *merchantEndpoint) repair() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.broken = false
}

func (e *merchantEndpoint) deliveries() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.delivered)
}

var deliverWebhook = worker.NewTask[Webhook]("deliver-webhook", worker.TaskOpts{})

func readyWhen(ready <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-ready:
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "ready")
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, "starting")
		}
	})
}

func probe(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s -> %s, %d bytes", url, resp.Status, len(body)), nil
}

func await(ctx context.Context, what string, done func() (bool, error)) error {
	for {
		ok, err := done()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up waiting for %s: %w", what, ctx.Err())
		case <-time.After(pollStep):
		}
	}
}

func countOrUnknown(n int, known bool) string {
	if !known {
		return "unknown"
	}
	return strconv.Itoa(n)
}

func describe(s converge.JobStats) string {
	return fmt.Sprintf("%s surface=%s state=%s in_flight=%d backlog=%s shelved=%s",
		s.Job, s.Surface, s.State, s.InFlight,
		countOrUnknown(s.Backlog, s.BacklogKnown),
		countOrUnknown(s.Shelved, s.ShelvedKnown))
}

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
		LeaseTTL:  leaseTTL,
	})
	if err != nil {
		return err
	}

	hooks := &merchantEndpoint{broken: true}

	err = worker.Handle(rt, deliverWebhook, func(ctx context.Context, w Webhook) error {
		err := hooks.post(ctx, w)
		if errors.Is(err, errEndpointRejected) {
			return worker.Shelve{Reason: "endpoint rejected the payload"}
		}
		return err
	}, worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 3}})
	if err != nil {
		return err
	}

	p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: namespace})
	if err != nil {
		return err
	}

	jobs := debughttp.ReadOnlyHandler(rt)
	mux := http.NewServeMux()
	mux.Handle(readyPath, readyWhen(rt.Ready()))
	mux.Handle(statsPath, jobs)
	mux.Handle(statsPath+"/", jobs)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux}
	defer server.Close()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	base := "http://" + listener.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	stopped := make(chan error, 1)
	go func() { stopped <- rt.Run(ctx) }()

	<-rt.Ready()
	line, err := probe(base + readyPath)
	if err != nil {
		return err
	}
	fmt.Println(line)

	if err := deliverWebhook.Enqueue(ctx, p, Webhook{ID: "wh-1", Event: "charge.succeeded"}, worker.EnqueueOpts{}); err != nil {
		return err
	}

	shelf, err := worker.ShelfFrom(rt, deliverWebhook.Name())
	if err != nil {
		return err
	}

	var messageID string
	err = await(ctx, "the failed delivery to reach the shelf", func() (bool, error) {
		shelved, err := shelf.List(ctx)
		if err != nil || len(shelved) == 0 {
			return false, err
		}
		messageID = shelved[0].MessageID
		return true, nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("shelved message: %s\n", messageID)

	err = await(ctx, "the shelf depth to reach the stats surface", func() (bool, error) {
		for _, s := range rt.Stats() {
			if s.Job == deliverWebhook.Name() && s.ShelvedKnown && s.Shelved > 0 {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	line, err = probe(base + statsPath)
	if err != nil {
		return err
	}
	fmt.Println(line)
	for _, s := range rt.Stats() {
		fmt.Println(describe(s))
	}

	hooks.repair()
	if err := shelf.Requeue(ctx, messageID); err != nil {
		return err
	}

	err = await(ctx, "the requeued message to be delivered", func() (bool, error) {
		return hooks.deliveries() > 0, nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("delivered after requeue: %d\n", hooks.deliveries())

	cancel()
	if err := <-stopped; err != nil {
		return err
	}
	server.Close()
	if err := <-serving; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
