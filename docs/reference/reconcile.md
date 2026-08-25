# `converge/reconcile` — the level-triggered model

[Reconcile](../glossary.md#reconcile) jobs keep something true: converge
calls your function once per ID on a schedule, and your function re-reads
the world and puts it right — messages only say *what* to look at, never
*what to do*.

```go
type ID string
func JoinID(parts ...string) ID
func SplitID(id ID) []string
func Split2(id ID) (a, b string, err error) // arity-checked; never panics
func ToIDs(raw ...string) []ID

type Reconciler interface {
    Reconcile(ctx context.Context, id ID) error
}
type Func func(ctx context.Context, id ID) error
func (f Func) Reconcile(ctx context.Context, id ID) error { return f(ctx, id) }

// The one-liner for single-unit periodic jobs — no Spec, no ID, no options.
// Anything needing options graduates to Register.
func Periodic(rt *converge.Runtime, name string, c Cadence, fn func(ctx context.Context) error) error

func Register(rt *converge.Runtime, spec Spec) error

type Spec struct {
    Name             string               // required, unique within the runtime
    Reconciler       Reconciler           // required
    Triggers         []Trigger            // must include a PeriodicTrigger unless AllowUnscheduled
    Concurrency      int                  // default 1
    RunMode          converge.RunMode     // default OnOneReplica
    DeadLetterAfter  int                  // 0 (default) = retry forever with capped backoff
    Versions         VersionSource        // optional: enables parked-ID revival
    RateLimit        converge.Rate        // optional; process-local token bucket
    Middleware       []converge.Middleware
    AllowUnscheduled bool
    Paused           bool
}

// outcome values — implement error; keyed fields required
// after 10 consecutive CheckAgain returns for one ID the failure backoff
// curve applies anyway, reported as BackoffFallback
type CheckAgain struct {
    In time.Duration // floored: under 250ms becomes 250-375ms
}
var ErrOutdated error

// ID sources — consulted fresh at every scheduled pass
func SingleID() IDSource
func IDs(fn func(ctx context.Context) ([]ID, error)) IDSource
func StringIDs(fn func(ctx context.Context) ([]string, error)) IDSource
func IDsByPage(fn func(ctx context.Context, cursor string) ([]ID, string, error)) IDSource
    // cursor MUST be keyset-stable: a value that is both order-stable and
    // unique across calls (e.g. an auto-incrementing ID, or a timestamp
    // paired with a tiebreaker column when it isn't already unique), not
    // a positional offset — get either property wrong and concurrent
    // inserts, deletes, or tied boundary values silently skip IDs

// triggers
type Trigger interface {
    Run(ctx context.Context, wake func(ID)) error
}
type PeriodicTrigger interface {
    Trigger
    NextAfter(t time.Time) time.Time // cron-honest; drives staleness + missed-tick
}

func Schedule(ids IDSource, c Cadence) PeriodicTrigger
func Every(d time.Duration) Cadence            // anchored to persisted epoch
func Cron(expr string, o CronOpts) Cadence     // 5-field dialect, no seconds/descriptors

type CronOpts struct {
    Location   *time.Location   // nil = UTC
    MissedTick MissedTickPolicy // Skip | RunOnce (default) | Catchup
}

func OnMessage(queue string, id IDFunc, o OnMessageOpts) Trigger
type OnMessageOpts struct {
    MQ       converge.MQ           // non-default transport
    Delivery converge.DeliveryMode // Group | Broadcast; default follows run mode;
                                   // OnAllReplicas requires Broadcast
}

// ID extraction
type IDFunc func(payload []byte) (ID, error)
func RawID() IDFunc
func IDFromJSONField(field string) IDFunc
func IDFromJSONFields(fields ...string) IDFunc

// version tracking
type Version uint64
type VersionSource interface {
    Latest(ctx context.Context, id ID) (Version, error)
}
func NewTracker(kv converge.KV, namespace string) *Tracker // non-empty, no "/"; registration
                                                           // requires it to equal Spec.Name
func (t *Tracker) MarkChanged(ctx context.Context, id ID) (Version, error)
func (t *Tracker) Latest(ctx context.Context, id ID) (Version, error)
func (t *Tracker) MarkApplied(ctx context.Context, id ID, v Version) error
func (t *Tracker) Forget(ctx context.Context, id ID) error // GC when the entity is deleted
```

See [7. Stale writes](../guide/07-versions.md) for `Tracker`'s namespace and
revival rules.
