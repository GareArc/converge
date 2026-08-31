# The Gin example

The Gin example is a small commerce API: create an order, pay for it,
publish an event to a set of webhook subscribers, and keep a document index
in sync with a documents table. Underneath, one converge `Runtime` carries
three jobs — `expire-unpaid-orders` and `index-documents` reconcile against
Postgres, `deliver-webhook` durably delivers to subscriber URLs — sharing one
Redis for the queue, lease, and KV, all inside a single Gin binary.

Gin needs no converge bridge. A bridge exists for frameworks that own
program lifecycle themselves — the
[Kratos bridge](../../docs/reference/adapters.md#convkratos) wraps a
`Runtime` as a kratos `transport.Server` so converge can join the list of
servers a kratos `App` starts and stops. Gin has no such list: a
`gin.Engine` is an `http.Handler` and nothing more, so `main.go`'s
`errgroup` and the context `signal.NotifyContext` returns are the entire
integration. The same shape works verbatim for chi, echo, fiber, or plain
`net/http` — see
[How converge is wired into Gin](#how-converge-is-wired-into-gin) below.

## Run it

```text
cd examples/gin
docker compose up --build
```

This starts Redis and Postgres, waits for both to answer their
healthchecks — the `app` service's `depends_on: condition: service_healthy`
— seeds Postgres from `schema.sql` (including three webhook subscribers
pointed at a local `httpbin`), then starts the app on `localhost:8080`.
`schema.sql` only runs against an empty Postgres data directory, so an edit
to it after the first boot needs `docker compose down -v` — which drops that
volume — before it takes effect.

```yaml title=examples/gin/compose.yml
services:
  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"]
    ports:
      - "127.0.0.1:6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 2s
      retries: 15

  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: converge
      POSTGRES_PASSWORD: converge
      POSTGRES_DB: converge
    ports:
      - "127.0.0.1:5432:5432"
    volumes:
      - ./schema.sql:/docker-entrypoint-initdb.d/schema.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U converge"]
      interval: 2s
      timeout: 2s
      retries: 15

  httpbin:
    image: mccutchen/go-httpbin:v2.15.0
    environment:
      PORT: "80"
    expose:
      - "80"

  app:
    build:
      context: ../..
      dockerfile: examples/gin/Dockerfile
    environment:
      HTTP_ADDR: ":8080"
      REDIS_ADDR: "redis:6379"
      POSTGRES_DSN: "postgres://converge:converge@postgres:5432/converge?sslmode=disable"
    ports:
      - "8080:8080"
    restart: unless-stopped
    depends_on:
      redis:
        condition: service_healthy
      postgres:
        condition: service_healthy
```

## Routes

| Method | Path                          | What it does                                          |
| ------ | ----------------------------- | ------------------------------------------------------ |
| GET    | `/healthz`                    | Liveness check                                          |
| ANY    | `/debug/*path`                | Read-only job introspection (`debughttp.ReadOnlyHandler`) |
| POST   | `/orders`                     | Create a pending order                                  |
| POST   | `/orders/:id/pay`             | Pay a pending order; notifies `expire-unpaid-orders`     |
| GET    | `/orders/:id`                 | Fetch an order                                           |
| POST   | `/events`                     | Publish an event; queues one delivery per active subscriber |
| GET    | `/webhooks/shelf`             | List shelved deliveries                                  |
| POST   | `/webhooks/shelf/:id/requeue` | Requeue a shelved delivery by its converge `message_id`  |
| PUT    | `/documents/:id`               | Upsert a document; notifies `index-documents`             |
| GET    | `/search`                      | Full-text search over the indexed documents               |

## expire-unpaid-orders (reconcile)

`expire-unpaid-orders` is a reconcile job because the truth about which
orders are still unpaid lives entirely in the `orders` table —
`store.PendingOlderThan` can always list every ID still owed a decision, so
a lost notification only costs the minute until the next sweep, never
correctness. The schedule sweeps every minute for orders unpaid more than 30
minutes and cancels them; paying an order sends a notification so its ID
gets a look sooner than the next sweep would give it, but the reconcile
function just re-reads the row — if it is no longer pending, there is
nothing to do, notification or not.

A failed notification is logged and never fails the request: `pay` has
already committed the payment by the time it calls `Notify`, so the response
reports the real outcome regardless of whether the notification lands — the
schedule sweep is the backstop either way.

```go title=examples/gin/internal/orders/job.go
package orders

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	unpaidFor  = 30 * time.Minute
	sweepEvery = time.Minute
	runTimeout = 10 * time.Second
)

var job = reconcile.NewJob("expire-unpaid-orders", reconcile.JobOpts{})

func Register(rt *converge.Runtime, r gin.IRouter, db *pgxpool.Pool) error {
	store := NewStore(db)
	err := reconcile.Register(rt, reconcile.Spec{
		Job: job,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.StringIDs(store.PendingOlderThan(unpaidFor)),
				reconcile.Every(sweepEvery)),
			reconcile.Notifications(),
		},
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			return store.CancelIfUnpaid(ctx, string(id), unpaidFor)
		},
		Timeout: runTimeout,
	})
	if err != nil {
		return err
	}
	notifier, err := job.NewProducer(rt.Scope())
	if err != nil {
		return err
	}
	h := &handlers{store: store, notifier: notifier}
	r.POST("/orders", h.create)
	r.POST("/orders/:id/pay", h.pay)
	r.GET("/orders/:id", h.get)
	return nil
}
```

```text
$ curl -s -w '\nHTTP %{http_code}\n' -XPOST localhost:8080/orders \
    -H 'content-type: application/json' -d '{"id":"o-1"}'
{"id":"o-1","status":"pending"}
HTTP 201

$ curl -s -w '\nHTTP %{http_code}\n' -XPOST localhost:8080/orders/o-1/pay
{"id":"o-1","status":"paid"}
HTTP 200

$ curl -s -w '\nHTTP %{http_code}\n' localhost:8080/orders/o-1
{"id":"o-1","status":"paid","placed_at":"2026-08-31T02:24:43.830547Z"}
HTTP 200

$ curl -s -w '\nHTTP %{http_code}\n' -XPOST localhost:8080/orders/o-does-not-exist/pay
{"error":"no such order"}
HTTP 404

$ curl -s -w '\nHTTP %{http_code}\n' -XPOST localhost:8080/orders/o-1/pay
{"error":"order is not pending"}
HTTP 409
```

Paying `o-1` a second time returns 409, not a second `paid` response — the
store's `UPDATE` matched no rows because the order was no longer pending,
and the handler tells the two failure cases apart: 404 if the order does not
exist at all, 409 if it exists but is not pending.

Re-`POST`ing `o-1` behaves the same way. `Store.Create`'s insert is
`ON CONFLICT (id) DO NOTHING`, so `create` cannot tell from the write alone
whether it made a new row or hit an existing one — it checks, the way `pay`
already does, and a second create for an ID that exists never answers a
fabricated `{"status":"pending"}` again:

```text
$ curl -s -w '\nHTTP %{http_code}\n' -XPOST localhost:8080/orders \
    -H 'content-type: application/json' -d '{"id":"o-1"}'
{"id":"o-1","status":"paid","placed_at":"2026-08-31T02:24:43.830547Z"}
HTTP 409
```

## deliver-webhook (worker)

`deliver-webhook` is a worker task because the message carries the work:
`publish` decides, once, which subscribers this event is for, and
delivering to each one is that decision, not a fact re-derivable from
Postgres afterward. The worker surface is what turns that decision into
delivery without this example hand-rolling it — the retry curve, the
attempt accounting `worker.MetaFromContext` exposes, the per-second rate
limit, and the shelf a person can inspect are `worker.Handle` and
`worker.Shelf`, not code this example wrote. A reconcile job draining the
`deliveries` table on a schedule would have to rebuild all four from
scratch.

The `deliveries` table is a record of what happened to each delivery — the
audit query above and the requeue examples below both read it — but it is
not what drives delivery. `publish` writes the row and enqueues the message
as two separate, non-transactional calls, so a crash between them is
possible, and the row can disagree with the queue about a delivery's real
status until the next attempt corrects it. A service where that gap matters
more than it does here should reach for the transactional pairing in
[outbox and inbox](../../docs/cookbook/outbox-inbox.md): write the outbox
row and the domain change in one database transaction, and let a reconcile
job drain it instead of enqueuing inline.

Delivery is at-least-once, not exactly-once: a subscriber that answers 200
right as the process crashes, or whose response is lost in transit before
`store.Record` runs, sees the same event again on retry. Every subscriber
behind a converge worker task — this example's `httpbin` stand-ins
included, though they are stateless enough not to notice — must treat a
repeated delivery as a no-op.

Publishing an event queues one delivery per active subscriber, and does not
stop at the first subscriber it fails to queue or enqueue for: `publish`
logs that subscriber's failure and moves on to the next one, so the
response always reports 202 with the deliveries that actually made it onto
the queue, never a mid-loop 500 that leaves earlier subscribers durably
queued and later ones silently dropped. `deliver` reads the converge
attempt number from `worker.MetaFromContext` before doing anything, then
maps the subscriber's response: 429 snoozes for the `Retry-After` window (or
5 seconds, capped at 5 minutes) without spending an attempt, 2xx records
`StatusDelivered`, any other 4xx shelves for a person with the status code
as the reason, and everything else — 1xx, 3xx, 5xx, a malformed status — is
a plain error the retry policy backs off and retries against, up to 6
attempts or 24 hours.

```go title=examples/gin/internal/webhooks/task.go
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
```

The seeded subscribers cover the three outcomes: `sub-ok` (200), `sub-gone`
(410), `sub-slow` (429, always).

```text
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' -XPOST localhost:8080/events \
    -H 'content-type: application/json' -d '{"event":"charge.succeeded"}'
{"event_id":"6484962e-fb04-4492-972d-e195464be841","queued":["6484962e-fb04-4492-972d-e195464be841:sub-gone","6484962e-fb04-4492-972d-e195464be841:sub-ok","6484962e-fb04-4492-972d-e195464be841:sub-slow"]}
HTTP_STATUS:202

$ sleep 20

$ curl -s localhost:8080/webhooks/shelf
{"shelved":[{"task":"deliver-webhook","queue":"shop/converge/queue/deliver-webhook","message_id":"9a276b05166f51dca81ac96be1586955","attempt":1,"reason":"subscriber refused with 410","enqueued_at":"2026-08-31T02:24:49.361573829Z","shelved_at":"2026-08-31T02:24:49.385210087Z","headers":{"converge.attempt":"0","converge.enqueued-at":"2026-08-31T02:24:49.361573829Z","converge.message-id":"9a276b05166f51dca81ac96be1586955","converge.schema-version":"1"},"payload":"<base64>"}]}
```

(`payload` above is redacted for this listing — the actual response carries
the delivery's full base64-encoded JSON body under that field, not the
literal string `<base64>`.)

```text
$ docker compose exec postgres psql -U converge -c "SELECT id, status, attempts FROM deliveries ORDER BY id"
                      id                       |  status   | attempts
-----------------------------------------------+-----------+----------
 6484962e-fb04-4492-972d-e195464be841:sub-gone | failed    |        1
 6484962e-fb04-4492-972d-e195464be841:sub-ok   | delivered |        1
 6484962e-fb04-4492-972d-e195464be841:sub-slow | queued    |        0
(3 rows)
```

`sub-slow` is still `queued` with `attempts=0` three deliveries later: the
snooze path never calls `Record`, because `deliveries.attempts` carries the
real converge attempt number and a snooze does not spend one. `sub-gone`
shelved with the response status in its reason, and `GET /webhooks/shelf`
answers with an empty array on a quiet stack rather than `null`:

```text
$ curl -s localhost:8080/webhooks/shelf
{"shelved":[]}
```

The `:id` in `POST /webhooks/shelf/:id/requeue` is the converge
`message_id` shown in that listing, not `deliveries.id` — pull it straight
out of `GET /webhooks/shelf` and requeue with it:

```text
$ curl -s localhost:8080/webhooks/shelf | jq -r '.shelved[0].message_id'
9a276b05166f51dca81ac96be1586955

$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' -XPOST \
    localhost:8080/webhooks/shelf/9a276b05166f51dca81ac96be1586955/requeue
HTTP_STATUS:204
```

Requeuing an ID with no shelved record behind it — already requeued,
already purged, or never shelved at all — answers 404 rather than 500:
`Shelf.Requeue` returns the exported `worker.ErrNotShelved`, and the
handler tells that apart from a real failure the same way `orders`' `pay`
already tells 404 from 409 apart.

```text
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' -XPOST \
    localhost:8080/webhooks/shelf/does-not-exist/requeue
{"error":"no such shelved message"}
HTTP_STATUS:404
```

## index-documents (reconcile)

`index-documents` is a reconcile job for the same reason as
`expire-unpaid-orders`: `store.NeedingIndex` can always list every document
whose `indexed_at` is missing or older than its `updated_at`, so
notifications are purely an optimization over what the sweep would find
anyway. `PUT /documents/:id` upserts the row and notifies; a failed
notification is logged the same way `pay`'s is, and the request still
succeeds. Writing straight into Postgres with no notification — the way
another system's migration or batch job might — still converges: the
30-second schedule sweep finds the row and reindexes it.

Both reconcile jobs' ID sources run this way: on a schedule, forever, over
the whole table, whether or not anything in it changed since the last
sweep. That recurring full-table cost is the one a worker task never
pays — its queue only ever holds work that actually exists — and it is why
`schema.sql` carries a partial index matching each ID source's predicate,
`orders_pending_placed_at` and `documents_needing_index`: without one, the
sweep that makes reconcile affordable forever degrades into a sequential
scan forever, and the affordability the surface promises stops being true
the moment either table outgrows a single page.

```go title=examples/gin/internal/documents/job.go
package documents

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sweepEvery = 30 * time.Second
	runTimeout = 15 * time.Second
)

var job = reconcile.NewJob("index-documents", reconcile.JobOpts{})

func Register(rt *converge.Runtime, r gin.IRouter, db *pgxpool.Pool) error {
	store := NewStore(db)
	err := reconcile.Register(rt, reconcile.Spec{
		Job: job,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.StringIDs(store.NeedingIndex),
				reconcile.Every(sweepEvery)),
			reconcile.Notifications(),
		},
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			return store.Index(ctx, string(id))
		},
		Timeout: runTimeout,
	})
	if err != nil {
		return err
	}
	notifier, err := job.NewProducer(rt.Scope())
	if err != nil {
		return err
	}
	h := &handlers{store: store, notifier: notifier}
	r.PUT("/documents/:id", h.upsert)
	r.GET("/search", h.search)
	return nil
}
```

```text
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' -XPUT localhost:8080/documents/d-1 \
    -H 'content-type: application/json' \
    -d '{"title":"Reconcile","body":"the truth lives in your store"}'
{"id":"d-1"}
HTTP_STATUS:202

$ sleep 3
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' 'localhost:8080/search?q=truth'
{"results":[{"id":"d-1","title":"Reconcile","body":"the truth lives in your store"}]}
HTTP_STATUS:200

$ docker compose exec postgres psql -U converge -c \
    "INSERT INTO documents (id, title, body) VALUES ('d-2', 'Worker', 'the message is the work')"
INSERT 0 1

$ curl -s 'localhost:8080/search?q=message'
{"results":[]}
```

`d-2` was inserted straight into Postgres with no notification, so `search`
found nothing for it until the schedule sweep ran on its own — the same
`{"results":[]}` empty-array shape `search` always returns, notified or not.

The staleness window cuts the other way too, and it is worth seeing
directly. `document_index.terms` is a snapshot from the last time a
document was reindexed; `search` always joins back to the live `documents`
row for the title and body it renders, so between an edit and the sweep
that catches it, a hit can match on a term the current text no longer
contains, right next to the current text:

```text
$ docker compose exec postgres psql -U converge -c \
    "UPDATE documents SET title = 'Reconciled', body = 'the store has already moved on', updated_at = now() WHERE id = 'd-1'"
UPDATE 1

$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' 'localhost:8080/search?q=truth'
{"results":[{"id":"d-1","title":"Reconciled","body":"the store has already moved on"}]}
HTTP_STATUS:200
```

`truth` is nowhere in that response's `title` or `body` — it is a match
against the stale index entry from before the direct edit, rendered beside
the row's current, already-edited content. It resolves itself on the next
sweep, the same way `d-2` did:

```text
$ sleep 33
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' 'localhost:8080/search?q=message'
{"results":[{"id":"d-2","title":"Worker","body":"the message is the work"}]}
HTTP_STATUS:200

$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' 'localhost:8080/search?q=truth'
{"results":[]}
HTTP_STATUS:200
```

```sql title=examples/gin/schema.sql
CREATE TABLE orders (
  id        text PRIMARY KEY,
  status    text        NOT NULL,
  placed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX orders_pending_placed_at ON orders (placed_at) WHERE status = 'pending';

CREATE TABLE subscribers (
  id     text PRIMARY KEY,
  url    text    NOT NULL,
  active boolean NOT NULL DEFAULT true
);

CREATE TABLE deliveries (
  id            text PRIMARY KEY,
  event_id      text        NOT NULL,
  subscriber_id text        NOT NULL REFERENCES subscribers (id),
  status        text        NOT NULL,
  attempts      integer     NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE documents (
  id         text PRIMARY KEY,
  title      text        NOT NULL,
  body       text        NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  indexed_at timestamptz
);

CREATE TABLE document_index (
  document_id text PRIMARY KEY REFERENCES documents (id) ON DELETE CASCADE,
  terms       tsvector    NOT NULL,
  indexed_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX document_index_terms ON document_index USING gin (terms);

CREATE INDEX documents_needing_index ON documents (id)
  WHERE indexed_at IS NULL OR indexed_at < updated_at;

INSERT INTO subscribers (id, url, active) VALUES
  ('sub-slow', 'http://httpbin:80/status/429', true),
  ('sub-ok',   'http://httpbin:80/status/200', true),
  ('sub-gone', 'http://httpbin:80/status/410', true);
```

## How converge is wired into Gin

`main.go`'s `run` builds the converge `Runtime` against the Redis-backed MQ,
Lease, and KV adapters, then starts a plain `gin.New()` engine with no
framework-level notion of a background job to hook into — the three domain
packages each register their own routes on the same `gin.IRouter` and get
out of the way. Everything that keeps the process alive is one
`errgroup.Group` built from the context `signal.NotifyContext` returns: one
goroutine runs `rt.Run(gctx)`, one runs `srv.ListenAndServe()`, a third
waits on `gctx.Done()` and calls `srv.Shutdown` with a 10-second grace
period. Cancelling the shared context — SIGINT, SIGTERM, or any of the
three goroutines returning an error — stops all three, and `g.Wait()` is
the only thing `main` waits on. Nothing here is Gin-specific: the same
three goroutines, unchanged, are the whole integration for chi, echo,
fiber, or plain `net/http`, because none of them own process lifecycle any
more than Gin does — only a framework like kratos, with its own `App` that
starts and stops a list of servers, needs the extra step a bridge like
`convkratos` provides.

```go title=examples/gin/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/examples/gin/internal/documents"
	"github.com/GareArc/converge/examples/gin/internal/orders"
	"github.com/GareArc/converge/examples/gin/internal/webhooks"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const shutdownGrace = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	rt, err := converge.New(converge.Options{
		Namespace: cfg.Namespace,
		MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
		Lease:     convredis.NewLease(rdb),
		KV:        convredis.NewKV(rdb),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	engine.Any("/debug/*path", gin.WrapH(debughttp.ReadOnlyHandler(rt)))

	for _, register := range []func(*converge.Runtime, gin.IRouter, *pgxpool.Pool) error{
		orders.Register,
		webhooks.Register,
		documents.Register,
	} {
		if err := register(rt, engine, db); err != nil {
			return err
		}
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: engine}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return rt.Run(gctx) })
	g.Go(func() error {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), shutdownGrace)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
```

`/debug/*path` mounts `debughttp.ReadOnlyHandler(rt)` through `gin.WrapH`
with no prefix stripped: the handler's own `http.ServeMux` registers full
paths like `GET /debug/jobs`, so it already expects to see `/debug/...` on
the wire, and an `http.StripPrefix` here would only break it.

```text
$ curl -s localhost:8080/debug/jobs
{"jobs":[{"job":"expire-unpaid-orders","surface":"reconcile","run_mode":"OnOneReplica","state":"active","queue":"","settings":{"concurrency":"1","schedule":"every 1m","triggers":"schedule + notifications"},"lease_held":true,"in_flight":0,"backlog":0,"backlog_known":true,"backlog_at":"2026-08-31T02:25:32.968480359Z","failing":0,"shelved":0,"shelved_known":false,"shelved_at":"","last_success":"2026-08-31T02:24:43.855596539Z","last_error":"","last_error_at":"","consecutive_fails":0},{"job":"deliver-webhook","surface":"worker","run_mode":"Competing","state":"active","queue":"shop/converge/queue/deliver-webhook","settings":{"concurrency":"8","rate-limit":"50/1s","retry":"6 attempts, backoff 1s..15m, max-age 24h","schema-version":"1","timeout":"15s"},"lease_held":false,"in_flight":0,"backlog":0,"backlog_known":true,"backlog_at":"2026-08-31T02:25:32.968531693Z","failing":0,"shelved":1,"shelved_known":true,"shelved_at":"2026-08-31T02:25:32.968612485Z","last_success":"2026-08-31T02:24:49.403805Z","last_error":"","last_error_at":"","consecutive_fails":1},{"job":"index-documents","surface":"reconcile","run_mode":"OnOneReplica","state":"active","queue":"","settings":{"concurrency":"1","schedule":"every 30s","triggers":"schedule + notifications"},"lease_held":true,"in_flight":0,"backlog":0,"backlog_known":true,"backlog_at":"2026-08-31T02:25:32.968582943Z","failing":0,"shelved":0,"shelved_known":false,"shelved_at":"","last_success":"2026-08-31T02:25:32.963460557Z","last_error":"","last_error_at":"","consecutive_fails":0}]}
```

All three jobs answer the same `/debug/jobs` listing regardless of surface —
`expire-unpaid-orders` and `index-documents` show `surface: reconcile`,
`deliver-webhook` shows `surface: worker` with its queue name and a
`shelved` count, and this is read-only: nothing under `/debug` mutates job
state.
