package converge

// HeaderPrefix marks headers owned by the engine; user headers with this
// prefix are rejected at enqueue time.
const HeaderPrefix = "converge."

const (
	HeaderSchemaVersion = HeaderPrefix + "schema-version"
	HeaderEnqueuedAt    = HeaderPrefix + "enqueued-at"
)

type Message struct {
	Kind    string // task name for worker messages; "" for reconcile hints
	Headers map[string]string
	Payload []byte
}
