package converge

const HeaderPrefix = "converge."

const (
	HeaderSchemaVersion = HeaderPrefix + "schema-version"
	HeaderEnqueuedAt    = HeaderPrefix + "enqueued-at"
	HeaderMessageID     = HeaderPrefix + "message-id"
	HeaderAttempt       = HeaderPrefix + "attempt"
	HeaderSnoozes       = HeaderPrefix + "snoozes"
)

type Message struct {
	Kind    string
	Headers map[string]string
	Payload []byte
}
