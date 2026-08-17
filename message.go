package converge

const HeaderPrefix = "converge."

const (
	HeaderSchemaVersion = HeaderPrefix + "schema-version"
	HeaderEnqueuedAt    = HeaderPrefix + "enqueued-at"
)

type Message struct {
	Kind    string
	Headers map[string]string
	Payload []byte
}
