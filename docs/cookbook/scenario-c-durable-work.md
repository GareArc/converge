# Scenario C: Durable work across services

Producer and consumer are different processes.

*"When an admin invites a member, send the invite email. The email content
exists only in the request — nothing to re-read. Exactly the worker shape."*

The contract lives in a shared package both services import. **Convention:
one leaf contract package per codebase** — `NewTask` vars, payload types,
frozen wire constants; imports `converge/worker` and nothing else:

```go
// package jobs
var SendInvite = worker.NewTask[InvitePayload]("send-invite", worker.TaskOpts{
    Version: 1, // payload schema version — carried in message headers;
})              // bump it when the payload shape changes incompatibly

type InvitePayload struct {
    Email       string `json:"email"`
    InviterID   string `json:"inviter_id"`
    WorkspaceID string `json:"workspace_id"`
}
```

Producer service (API):

```go
prod, err := worker.NewProducer(convredis.NewStreamsMQ(rdb))

err = jobs.SendInvite.Enqueue(ctx, prod, jobs.InvitePayload{
    Email: req.Email, InviterID: uid, WorkspaceID: wsID,
}, worker.EnqueueOpts{})
```

The call above is not atomic with your DB transaction: a rollback still
sends the email; a failed enqueue after commit loses it. When the enqueue
must ride the transaction, use the outbox pattern — see
[Outbox and inbox recipes](outbox-inbox.md#outbox) — and notice it's just
the two models composed.

Consumer service:

```go
// package invites
func NewWorker(rt *converge.Runtime, mailer Mailer) (*Worker, error) {
    w := &Worker{mailer: mailer}
    err := worker.Handle(rt, jobs.SendInvite, w.handle, worker.HandleOpts{
        Retry:       worker.RetryPolicy{MaxAttempts: 10}, // other fields keep defaults (zero-value rule — see the worker reference)
        Concurrency: 16,
    })
    if err != nil {
        return nil, err
    }
    return w, nil
}

func (w *Worker) handle(ctx context.Context, p jobs.InvitePayload) error {
    meta, _ := worker.MetaFromContext(ctx) // attempt, message ID, enqueued-at, headers

    if invalid, _ := w.mailer.AddressRejected(p.Email); invalid {
        return worker.Discard{Reason: "address permanently rejected"}
    }
    err := w.mailer.Send(ctx, buildInvite(p, meta.MessageID)) // MessageID as idempotency key
    if isRateLimited(err) {
        return worker.Snooze{In: 30 * time.Second} // attempt untouched; MaxAge caps total
    }
    if err != nil && meta.Attempt == meta.MaxAttempts {
        w.pageOncall(ctx, p, err) // last attempt — escalate before DLQ
    }
    return err
}
```

In a process that both produces and consumes, `worker.ProducerFrom(rt)`
resolves each queue through the registered handlers' bindings, falling back
to the default MQ for queues handled elsewhere (validated at `Run`).

See also: [Worker outcomes](../guide/worker.md), and the
[worker API reference](../reference/worker.md) for `RetryPolicy`'s defaults.
