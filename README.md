# Converge

converge is a Go library that gives a service one model for all background
work. You register a function under a name and declare two things about it —
when it runs, and what it is about — and converge runs it correctly across
every replica of your service, bringing leasing, restart-safe scheduling,
bounded retries with backoff, and observability with it. It never owns your
data, and it is not a workflow engine, a bus abstraction, or a scheduler
service.

Every job is one of two kinds. A [reconcile](docs/glossary.md#reconcile) job
re-reads your own store and makes the world match it. A worker job does the
one thing a message asked for. One question tells them apart, and it is the
only thing you have to settle before you write any code.

## Install

```sh
go get github.com/GareArc/converge
```

The core module is stdlib-only (plus a parse-only cron dependency) — it has
no Redis, no database driver, nothing to configure to start using it against
`converge/inmem`. Backends live in their own modules:

```sh
go get github.com/GareArc/converge/adapters/redis   # convredis: MQ, Lease, KV
go get github.com/GareArc/converge/adapters/otel    # convotel: Observer over OpenTelemetry metrics
go get github.com/GareArc/converge/bridges/kratos   # convkratos: Runtime as a kratos transport.Server
```

## Two programs

Both of these are real files under `examples/scenarios/`, and both run with
no services at all. From the `examples` module:

```sh
cd examples
go run ./scenarios/a01-nightly-invoices
go run ./scenarios/a06-transactional-email
```

### One that reconciles

Invoices for every account that is due one, at 00:05 Tokyo time. The truth is
in the ledger, so nothing here is sent anywhere: the function reads what is
due and issues it. Miss a night and the next run still catches everything,
which is what makes this the reconcile side of the question.

```go title=examples/scenarios/a01-nightly-invoices/main.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const demoWindow = 2 * time.Second

type ledger struct {
	mu     sync.Mutex
	due    []string
	issued []string
}

func newLedger(due ...string) *ledger { return &ledger{due: due} }

func (l *ledger) generateDueInvoices(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.issued = append(l.issued, l.due...)
	l.due = nil
	return nil
}

func (l *ledger) invoiced() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.issued)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return err
	}

	rt, err := converge.New(converge.Options{
		Namespace: "billing",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	billing := newLedger("acct-1001", "acct-1002", "acct-1003")

	err = reconcile.Periodic(rt, "generate-invoices",
		reconcile.Cron("5 0 * * *", reconcile.CronOpts{Location: tokyo}),
		billing.generateDueInvoices,
		reconcile.PeriodicOpts{Timeout: 30 * time.Minute})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("invoices issued: %v\n", billing.invoiced())
	return nil
}
```

### One that works

An email per message. Here the message *is* the work — nothing in a database
will tell you later that a receipt was owed — so it is durable and retried,
and when the retries run out it is set aside on the
[shelf](docs/glossary.md#shelf) for a person rather than dropped. This one
also shows two of the three ways a handler stops early on purpose: it shelves
an address it can never deliver to, and discards one it should not. Note too
that the producer names the task and never a queue: converge gives every job
one [inbox](docs/glossary.md#inbox) and builds its name from the namespace
and the job's own name.

```go title=examples/scenarios/a06-transactional-email/main.go
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
```

[Chapter 1 of the guide](docs/guide/01-first-job.md) walks the first program
line by line; [chapter 4](docs/guide/04-worker.md) does the same for the
second.

## Choosing a surface

One question tells the two kinds of job apart:

> **If this message were lost, would anything be wrong?**

- **No — reconcile.** The truth lives in your store. Your function reads the
  store and makes the world match it. A message is only a
  [notification](docs/glossary.md#notification) — "look at this one sooner" —
  and the [schedule](docs/glossary.md#schedule) visits everything anyway, so
  notifications are cheap, unordered, deduplicated, and allowed to vanish.
- **Yes — worker.** The truth lives in the message. Something happened and a
  side effect must follow. The message *is* the work, so it is durable,
  retried, and moved to the shelf for a person to look at when the retries
  run out.

## Non-goals

- **No exactly-once execution.** converge retries on failure — your function
  has to be safe to run twice, not just once.
- **No workflow orchestration and no sagas.** A job is one function, not a
  sequence of steps coordinated across failures; that is what a tool like
  Temporal is for.
- **No built-in batching.** A reconcile function is called once per thing
  it's checking; group things yourself inside the call if a downstream
  service needs bulk requests.
- **No ordering guarantee between worker messages, and none per key.**
  Messages for the same job can be handled at the same time, in any order.
- **No scheduling work for a specific clock time, and no cancelling it once
  queued.** A due time belongs in your own row, checked by a reconcile job;
  a worker delay covers minutes, never days.
- **No jobs that exist for a single invocation, and no registering or
  removing jobs while the process runs.** Jobs are written in code; the only
  thing that comes and goes at runtime is an [ID](docs/glossary.md#id).
- **No pause, detach, or resume.** A job is in one of three states — not
  started, running, or finished for good — and if it is ever going to
  finish, you say so when you register it, with a
  [stop condition](docs/glossary.md#stop-condition).
- **No producer-side control of consumers.** Whoever sends a job something
  can say "look at this" and "do this", and nothing else; everything about a
  job's life is declared where the job runs.
- **No splitting one reconcile job's work across replicas.** Concurrency
  inside a single replica is the answer converge gives.
- **No per-customer rate limits.** A rate limit applies to a whole job, not
  to one customer within it.

## Documentation

Start at [the documentation index](docs/index.md), or go straight to what you
came for:

- **[The guide](docs/guide/index.md)** — seven chapters that teach the model
  in order, each one a program under `examples/scenarios/` you can run:
  [your first job](docs/guide/01-first-job.md),
  [one job, many things](docs/guide/02-ids.md),
  [telling a job to look sooner](docs/guide/03-notifications.md),
  [when the message is the work](docs/guide/04-worker.md),
  [where a job runs](docs/guide/05-run-modes.md),
  [taking it to production](docs/guide/06-production.md), and
  [testing a job](docs/guide/07-testing.md).
- **[The cookbook](docs/cookbook/index.md)** — six problems people bring to a
  background-work library, answered end to end:
  [work that takes a while](docs/cookbook/durable-work.md),
  [waiting for something to become true](docs/cookbook/event-driven.md),
  [a queue somebody else owns](docs/cookbook/foreign-queue.md),
  [jobs that end](docs/cookbook/lifecycle.md),
  [outbox and inbox](docs/cookbook/outbox-inbox.md), and
  [the safety net](docs/cookbook/safety-net.md).
- **The reference** — every exported name, in six pages:
  [kernel](docs/reference/kernel.md),
  [reconcile](docs/reference/reconcile.md),
  [worker](docs/reference/worker.md),
  [operations](docs/reference/operations.md),
  [adapters and test support](docs/reference/adapters.md), and
  [converge terms in other systems](docs/reference/prior-art.md).
- **[The glossary](docs/glossary.md)** — every converge-specific word,
  defined once, in plain language.

Contributing to converge itself starts with [`AGENT.md`](AGENT.md), which
holds the verification commands and the design rules, and
[`CONTEXT.md`](CONTEXT.md), the terminology contract every name in the
library is written against.

## License

MIT — see [LICENSE](LICENSE).
