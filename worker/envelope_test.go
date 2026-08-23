package worker

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

type stubDelivery struct {
	converge.Delivery
	attempt    int
	enqueuedAt time.Time
}

func (s stubDelivery) Attempt() int          { return s.attempt }
func (s stubDelivery) EnqueuedAt() time.Time { return s.enqueuedAt }

func TestEnvelopeAttempt(t *testing.T) {
	cases := []struct {
		name     string
		header   map[string]string
		delivery int
		want     int
		wantOK   bool
	}{
		{"absent header", nil, 2, 2, true},
		{"zero base", map[string]string{converge.HeaderAttempt: "0"}, 1, 1, true},
		{"absent equals zero", map[string]string{converge.HeaderAttempt: "0"}, 2, 2, true},
		{"folded base", map[string]string{converge.HeaderAttempt: "3"}, 1, 4, true},
		{"garbage", map[string]string{converge.HeaderAttempt: "x"}, 2, 2, false},
		{"negative", map[string]string{converge.HeaderAttempt: "-1"}, 2, 2, false},
		{"overflow", map[string]string{converge.HeaderAttempt: "9223372036854775807"}, 2, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnvelope(stubDelivery{attempt: tc.delivery}, converge.Message{Headers: tc.header})
			got, ok := env.attempt()
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("attempt() = (%d, %v), want (%d, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestEnvelopeMessageID(t *testing.T) {
	cases := []struct {
		name       string
		header     map[string]string
		want       string
		wantPrefix string
	}{
		{name: "header present returned verbatim", header: map[string]string{converge.HeaderMessageID: "explicit-id"}, want: "explicit-id"},
		{name: "absent header derives anon id", wantPrefix: "anon-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnvelope(stubDelivery{}, converge.Message{Kind: "job", Headers: tc.header, Payload: []byte("payload")})
			got := env.messageID()
			if tc.want != "" && got != tc.want {
				t.Fatalf("messageID() = %q, want %q", got, tc.want)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("messageID() = %q, want prefix %q", got, tc.wantPrefix)
			}
		})
	}

	kind, payload := "job", []byte("same-payload")
	first := newEnvelope(stubDelivery{}, converge.Message{Kind: kind, Payload: payload}).messageID()
	second := newEnvelope(stubDelivery{}, converge.Message{Kind: kind, Payload: payload}).messageID()
	if first != second {
		t.Fatalf("messageID() = %q then %q, want deterministic for same kind and payload", first, second)
	}
	differentPayload := newEnvelope(stubDelivery{}, converge.Message{Kind: kind, Payload: []byte("other-payload")}).messageID()
	if first == differentPayload {
		t.Fatalf("messageID() = %q, want different id for different payload, got same as %q", differentPayload, first)
	}
}

func TestEnvelopeEnqueuedAt(t *testing.T) {
	deliveryTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	headerTime := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
	cases := []struct {
		name   string
		header map[string]string
		want   time.Time
	}{
		{"valid header wins over delivery", map[string]string{converge.HeaderEnqueuedAt: headerTime.Format(time.RFC3339Nano)}, headerTime},
		{"malformed header falls back to delivery", map[string]string{converge.HeaderEnqueuedAt: "not-a-time"}, deliveryTime},
		{"absent header falls back to delivery", nil, deliveryTime},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnvelope(stubDelivery{enqueuedAt: deliveryTime}, converge.Message{Headers: tc.header})
			if got := env.enqueuedAt(); !got.Equal(tc.want) {
				t.Fatalf("enqueuedAt() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnvelopeSnoozes(t *testing.T) {
	cases := []struct {
		name   string
		header map[string]string
		want   int
	}{
		{"absent header", nil, 0},
		{"valid count", map[string]string{converge.HeaderSnoozes: "3"}, 3},
		{"garbage", map[string]string{converge.HeaderSnoozes: "x"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnvelope(stubDelivery{}, converge.Message{Headers: tc.header})
			if got := env.snoozes(); got != tc.want {
				t.Fatalf("snoozes() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestEnvelopeFolds(t *testing.T) {
	source := map[string]string{
		converge.HeaderAttempt: "2",
		converge.HeaderSnoozes: "1",
	}
	kind := "job"
	payload := []byte("payload")

	cases := []struct {
		name        string
		fold        func(envelope) converge.Message
		wantAttempt string
		wantSnoozes string
	}{
		{"forSnooze folds attempt and increments snoozes", envelope.forSnooze, "2", "2"},
		{"forNeutral folds attempt and leaves snoozes unchanged", envelope.forNeutral, "2", "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := converge.Message{Kind: kind, Headers: maps.Clone(source), Payload: payload}
			env := newEnvelope(stubDelivery{attempt: 1}, m)

			got := tc.fold(env)

			if got.Kind != kind {
				t.Fatalf("Kind = %q, want %q", got.Kind, kind)
			}
			if string(got.Payload) != string(payload) {
				t.Fatalf("Payload = %q, want %q", got.Payload, payload)
			}
			if got.Headers[converge.HeaderAttempt] != tc.wantAttempt {
				t.Fatalf("attempt header = %q, want %q", got.Headers[converge.HeaderAttempt], tc.wantAttempt)
			}
			if got.Headers[converge.HeaderSnoozes] != tc.wantSnoozes {
				t.Fatalf("snoozes header = %q, want %q", got.Headers[converge.HeaderSnoozes], tc.wantSnoozes)
			}
			if m.Headers[converge.HeaderAttempt] != "2" || m.Headers[converge.HeaderSnoozes] != "1" {
				t.Fatalf("source message headers mutated: %+v", m.Headers)
			}
		})
	}
}

func TestSeedMessage(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{"user headers preserved", map[string]string{"x-custom": "1"}},
		{"nil user headers", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := seedMessage("job", 3, now, tc.headers, []byte("payload"))
			if err != nil {
				t.Fatalf("seedMessage: %v", err)
			}
			if m.Kind != "job" {
				t.Fatalf("Kind = %q, want %q", m.Kind, "job")
			}
			if string(m.Payload) != "payload" {
				t.Fatalf("Payload = %q, want %q", m.Payload, "payload")
			}
			if m.Headers[converge.HeaderMessageID] == "" {
				t.Fatal("message-id header is empty")
			}
			if m.Headers[converge.HeaderSchemaVersion] != "3" {
				t.Fatalf("schema-version = %q, want %q", m.Headers[converge.HeaderSchemaVersion], "3")
			}
			want := now.UTC().Format(time.RFC3339Nano)
			if m.Headers[converge.HeaderEnqueuedAt] != want {
				t.Fatalf("enqueued-at = %q, want %q", m.Headers[converge.HeaderEnqueuedAt], want)
			}
			if m.Headers[converge.HeaderAttempt] != "0" {
				t.Fatalf("attempt = %q, want %q", m.Headers[converge.HeaderAttempt], "0")
			}
			for k, v := range tc.headers {
				if m.Headers[k] != v {
					t.Fatalf("user header %q = %q, want %q", k, m.Headers[k], v)
				}
			}
		})
	}

	first, err := seedMessage("job", 1, now, nil, nil)
	if err != nil {
		t.Fatalf("seedMessage: %v", err)
	}
	second, err := seedMessage("job", 1, now, nil, nil)
	if err != nil {
		t.Fatalf("seedMessage: %v", err)
	}
	if first.Headers[converge.HeaderMessageID] == second.Headers[converge.HeaderMessageID] {
		t.Fatal("message-id not unique across seedMessage calls")
	}

	_, err = seedMessage("job", 1, now, map[string]string{converge.HeaderPrefix + "x": "1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err = %v, want mention of reserved", err)
	}
}

func TestRequeueMessage(t *testing.T) {
	headers := map[string]string{
		converge.HeaderAttempt:       "5",
		converge.HeaderSnoozes:       "2",
		converge.HeaderMessageID:     "msg-1",
		converge.HeaderSchemaVersion: "3",
	}
	rec := DeadLetter{
		Task:    "job",
		Payload: []byte("payload"),
		Headers: maps.Clone(headers),
	}
	now := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	got := requeueMessage(rec, now)

	if got.Kind != rec.Task {
		t.Fatalf("Kind = %q, want %q", got.Kind, rec.Task)
	}
	if string(got.Payload) != string(rec.Payload) {
		t.Fatalf("Payload = %q, want %q", got.Payload, rec.Payload)
	}
	if got.Headers[converge.HeaderAttempt] != "0" {
		t.Fatalf("attempt = %q, want %q", got.Headers[converge.HeaderAttempt], "0")
	}
	if _, ok := got.Headers[converge.HeaderSnoozes]; ok {
		t.Fatalf("snoozes header present = %q, want absent", got.Headers[converge.HeaderSnoozes])
	}
	if got.Headers[converge.HeaderMessageID] != "msg-1" {
		t.Fatalf("message-id = %q, want %q", got.Headers[converge.HeaderMessageID], "msg-1")
	}
	if got.Headers[converge.HeaderSchemaVersion] != "3" {
		t.Fatalf("schema-version = %q, want %q", got.Headers[converge.HeaderSchemaVersion], "3")
	}
	want := now.UTC().Format(time.RFC3339Nano)
	if got.Headers[converge.HeaderEnqueuedAt] != want {
		t.Fatalf("enqueued-at = %q, want %q", got.Headers[converge.HeaderEnqueuedAt], want)
	}
	if rec.Headers[converge.HeaderAttempt] != "5" || rec.Headers[converge.HeaderSnoozes] != "2" {
		t.Fatalf("source DeadLetter headers mutated: %+v", rec.Headers)
	}
}
