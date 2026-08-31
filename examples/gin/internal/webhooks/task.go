package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/worker"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	concurrency     = 8
	maxAttempts     = 6
	maxAge          = 24 * time.Hour
	runTimeout      = 15 * time.Second
	httpTimeout     = 10 * time.Second
	defaultWait     = 5 * time.Second
	maxSnooze       = 5 * time.Minute
	rateLimitEvents = 50
	rateLimitPer    = time.Second
	maxDrainBytes   = 4 << 10
)

type Delivery struct {
	ID           string `json:"id"`
	EventID      string `json:"event_id"`
	SubscriberID string `json:"subscriber_id"`
	URL          string `json:"url"`
	Event        string `json:"event"`
}

var task = worker.NewTask[Delivery]("deliver-webhook", worker.TaskOpts{})

func Register(rt *converge.Runtime, r gin.IRouter, db *pgxpool.Pool) error {
	store := NewStore(db)
	client := &http.Client{Timeout: httpTimeout}
	err := worker.Handle(rt, task, func(ctx context.Context, d Delivery) error {
		return deliver(ctx, client, store, d)
	}, worker.HandleOpts{
		Concurrency: concurrency,
		RateLimit:   converge.Rate{Events: rateLimitEvents, Per: rateLimitPer},
		Retry:       worker.RetryPolicy{MaxAttempts: maxAttempts, MaxAge: maxAge},
		Timeout:     runTimeout,
	})
	if err != nil {
		return err
	}
	producer, err := task.NewProducer(rt.Scope())
	if err != nil {
		return err
	}
	shelf, err := worker.ShelfFrom(rt, task.Name())
	if err != nil {
		return err
	}
	h := &handlers{store: store, producer: producer, shelf: shelf}
	r.POST("/events", h.publish)
	r.GET("/webhooks/shelf", h.listShelved)
	r.POST("/webhooks/shelf/:id/requeue", h.requeue)
	return nil
}

func deliver(ctx context.Context, client *http.Client, store *Store, d Delivery) error {
	meta, _ := worker.MetaFromContext(ctx)
	body, err := json.Marshal(d)
	if err != nil {
		return shelveDelivery(ctx, store, d.ID, meta.Attempt, "payload not encodable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return shelveDelivery(ctx, store, d.ID, meta.Attempt, "unusable subscriber url")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return worker.Snooze{In: retryAfter(resp)}
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return store.Record(ctx, d.ID, StatusDelivered, meta.Attempt)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return shelveDelivery(ctx, store, d.ID, meta.Attempt, fmt.Sprintf("subscriber refused with %d", resp.StatusCode))
	default:
		return fmt.Errorf("subscriber returned %d", resp.StatusCode)
	}
}

func shelveDelivery(ctx context.Context, store *Store, id string, attempt int, reason string) error {
	if err := store.Record(ctx, id, StatusFailed, attempt); err != nil {
		slog.Default().Error("webhook delivery record failed", "id", id, "error", err)
	}
	return worker.Shelve{Reason: reason}
}

func retryAfter(resp *http.Response) time.Duration {
	secs, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || secs <= 0 {
		return defaultWait
	}
	if secs > int(maxSnooze/time.Second) {
		return maxSnooze
	}
	return time.Duration(secs) * time.Second
}
