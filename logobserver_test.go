package converge

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestLogObserverNilLoggerIsSilent(t *testing.T) {
	obs := LogObserver(nil)
	obs.Observe(RunCompleted{Job: "job", Outcome: Shelved})
}

func TestLogObserverReportsShelvedAtError(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	obs := LogObserver(l)
	obs.Observe(RunCompleted{Job: "charge", ID: "o-1", Outcome: Shelved})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode log line: %v", err)
	}
	if rec["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", rec["level"])
	}
	if rec["job"] != "charge" {
		t.Fatalf("job = %v, want %q", rec["job"], "charge")
	}
}

func TestLogObserverUnknownOutcomeStillLogs(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	obs := LogObserver(l)
	obs.Observe(RunCompleted{Job: "future", Outcome: Outcome{kind: outcomeKind(99)}})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode log line: %v", err)
	}
	if rec["outcome"] != "unknown" {
		t.Fatalf("outcome = %v, want unknown", rec["outcome"])
	}
}

func TestLogObserverIgnoresUnrecognizedEventType(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	obs := LogObserver(l)
	obs.Observe(fakeEvent{})
	if buf.Len() != 0 {
		t.Fatalf("unrecognized event type must not be logged, got %q", buf.String())
	}
}

type fakeEvent struct{}

func (fakeEvent) event() {}
