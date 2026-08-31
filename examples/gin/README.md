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
`errgroup` and its signal-derived context are the entire integration. The
same shape works verbatim for chi, echo, fiber, or plain `net/http` — see
[How converge is wired into Gin](#how-converge-is-wired-into-gin) below.

## Run it

```text
cd examples/gin
docker compose up --build
```

This starts Redis, Postgres (seeded from `schema.sql`, including three
webhook subscribers pointed at a local `httpbin`), and the app, then serves
on `localhost:8080`. If the app logs `converge: KV is unreachable` and
exits, Redis did not come up in time — bring the stack back up once Redis is
healthy.

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
| POST   | `/webhooks/shelf/:id/requeue` | Requeue a shelved delivery                                |
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
{"id":"o-1","status":"paid","placed_at":"2026-08-31T01:41:54.785684Z"}
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

## deliver-webhook (worker)

`deliver-webhook` is a worker task because the message is the work: an
event fires once, and no query against Postgres tells you which subscriber
URLs still need a POST for it — that has to be a durable, retried message or
it is silently gone. Publishing an event queues one delivery per active
subscriber. `deliver` reads the converge attempt number from
`worker.MetaFromContext` before doing anything, then maps the subscriber's
response: 429 snoozes for the `Retry-After` window (or 5 seconds, capped at
5 minutes) without spending an attempt, 2xx records `StatusDelivered`, any
other 4xx shelves for a person with the status code as the reason, and
everything else — 1xx, 3xx, 5xx, a malformed status — is a plain error the
retry policy backs off and retries against, up to 6 attempts or 24 hours.

```go title=examples/gin/internal/webhooks/task.go
package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	concurrency = 8
	maxAttempts = 6
	maxAge      = 24 * time.Hour
	runTimeout  = 15 * time.Second
	defaultWait = 5 * time.Second
	maxSnooze   = 5 * time.Minute
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
	client := &http.Client{Timeout: runTimeout}
	err := worker.Handle(rt, task, func(ctx context.Context, d Delivery) error {
		return deliver(ctx, client, store, d)
	}, worker.HandleOpts{
		Concurrency: concurrency,
		RateLimit:   converge.Rate{Events: 50, Per: time.Second},
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
	attempt := 0
	if meta, ok := worker.MetaFromContext(ctx); ok {
		attempt = meta.Attempt
	}
	body, err := json.Marshal(d)
	if err != nil {
		return shelveDelivery(ctx, store, d.ID, attempt, "payload not encodable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return shelveDelivery(ctx, store, d.ID, attempt, "unusable subscriber url")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return worker.Snooze{In: retryAfter(resp)}
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return store.Record(ctx, d.ID, StatusDelivered, attempt)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return shelveDelivery(ctx, store, d.ID, attempt, fmt.Sprintf("subscriber refused with %d", resp.StatusCode))
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
	if time.Duration(secs) > maxSnooze/time.Second {
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
{"event_id":"63301775-0cc3-4163-9725-3a901e411791","queued":["63301775-0cc3-4163-9725-3a901e411791:sub-gone","63301775-0cc3-4163-9725-3a901e411791:sub-ok","63301775-0cc3-4163-9725-3a901e411791:sub-slow"]}
HTTP_STATUS:202

$ sleep 20

$ curl -s localhost:8080/webhooks/shelf
{"shelved":[{"task":"deliver-webhook","queue":"shop/converge/queue/deliver-webhook","message_id":"1bf53729c9ebf385157e81e5e4bb3fe2","attempt":1,"reason":"subscriber refused with 410","enqueued_at":"2026-08-31T01:41:59.038209878Z","shelved_at":"2026-08-31T01:41:59.045314634Z","headers":{"converge.attempt":"0","converge.enqueued-at":"2026-08-31T01:41:59.038209878Z","converge.message-id":"1bf53729c9ebf385157e81e5e4bb3fe2","converge.schema-version":"1"},"payload":"<base64>"}]}

$ docker compose exec postgres psql -U converge -c "SELECT id, status, attempts FROM deliveries ORDER BY id"
                      id                       |  status   | attempts
-----------------------------------------------+-----------+----------
 63301775-0cc3-4163-9725-3a901e411791:sub-gone | failed    |        1
 63301775-0cc3-4163-9725-3a901e411791:sub-ok   | delivered |        1
 63301775-0cc3-4163-9725-3a901e411791:sub-slow | queued    |        0
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

$ sleep 33
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' 'localhost:8080/search?q=message'
{"results":[{"id":"d-2","title":"Worker","body":"the message is the work"}]}
HTTP_STATUS:200
```

`d-2` was inserted straight into Postgres with no notification, so `search`
found nothing for it until the schedule sweep ran on its own — the same
`{"results":[]}` empty-array shape `search` always returns, notified or not.

## How converge is wired into Gin

`main.go`'s `run` builds the converge `Runtime` against the Redis-backed MQ,
Lease, and KV adapters, then starts a plain `gin.New()` engine with no
framework-level notion of a background job to hook into — the three domain
packages each register their own routes on the same `gin.IRouter` and get
out of the way. Everything that keeps the process alive is one
`errgroup.Group` built from the signal-derived context that
`signal.NotifyContext` returns: one goroutine runs `rt.Run(gctx)`, one runs
`srv.ListenAndServe()`, a third waits on `gctx.Done()` and calls
`srv.Shutdown` with a 10-second grace period. Cancelling the shared context
— SIGINT, SIGTERM, or any of the three goroutines returning an error — stops
all three, and `g.Wait()` is the only thing `main` waits on. Nothing here is
Gin-specific: the same three goroutines, unchanged, are the whole
integration for chi, echo, fiber, or plain `net/http`, because none of them
own process lifecycle any more than Gin does — only a framework like kratos,
with its own `App` that starts and stops a list of servers, needs the extra
step a bridge like `convkratos` provides.

`/debug/*path` mounts `debughttp.ReadOnlyHandler(rt)` through `gin.WrapH`
with no prefix stripped: the handler's own `http.ServeMux` registers full
paths like `GET /debug/jobs`, so it already expects to see `/debug/...` on
the wire, and an `http.StripPrefix` here would only break it.

```text
$ curl -s localhost:8080/debug/jobs
{"jobs":[{"job":"expire-unpaid-orders","surface":"reconcile","run_mode":"OnOneReplica","state":"active","queue":"","settings":{"concurrency":"1","schedule":"every 1m","triggers":"schedule + notifications"},"lease_held":true,"in_flight":0,"backlog":0,"backlog_known":true,"backlog_at":"2026-08-31T01:41:40.183494447Z","failing":0,"shelved":0,"shelved_known":false,"shelved_at":"","last_success":"","last_error":"","last_error_at":"","consecutive_fails":0},{"job":"deliver-webhook","surface":"worker","run_mode":"Competing","state":"active","queue":"shop/converge/queue/deliver-webhook","settings":{"concurrency":"8","rate-limit":"50/1s","retry":"6 attempts, backoff 1s..15m, max-age 24h","schema-version":"1","timeout":"15s"},"lease_held":false,"in_flight":0,"backlog":0,"backlog_known":true,"backlog_at":"2026-08-31T01:41:40.187162451Z","failing":0,"shelved":0,"shelved_known":true,"shelved_at":"2026-08-31T01:41:40.187224368Z","last_success":"","last_error":"","last_error_at":"","consecutive_fails":0},{"job":"index-documents","surface":"reconcile","run_mode":"OnOneReplica","state":"active","queue":"","settings":{"concurrency":"1","schedule":"every 30s","triggers":"schedule + notifications"},"lease_held":true,"in_flight":0,"backlog":0,"backlog_known":true,"backlog_at":"2026-08-31T01:41:40.185609366Z","failing":0,"shelved":0,"shelved_known":false,"shelved_at":"","last_success":"","last_error":"","last_error_at":"","consecutive_fails":0}]}
```

All three jobs answer the same `/debug/jobs` listing regardless of surface —
`expire-unpaid-orders` and `index-documents` show `surface: reconcile`,
`deliver-webhook` shows `surface: worker` with its queue name and a
`shelved` count, and this is read-only: nothing under `/debug` mutates job
state.
