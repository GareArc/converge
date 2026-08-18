package converge

import (
	"strings"
	"testing"
	"time"
)

func TestRunModeZeroValueMeansDefault(t *testing.T) {
	var m RunMode
	if !m.IsZero() {
		t.Fatal("zero RunMode must report IsZero")
	}
	for _, mode := range []RunMode{OnOneReplica, SplitAcrossReplicas, OnAllReplicas} {
		if mode.IsZero() {
			t.Fatalf("%v must not be zero", mode)
		}
	}
	if OnOneReplica == SplitAcrossReplicas {
		t.Fatal("modes must be distinct")
	}
}

func TestRunModeString(t *testing.T) {
	cases := map[string]RunMode{
		"OnOneReplica":        OnOneReplica,
		"SplitAcrossReplicas": SplitAcrossReplicas,
		"OnAllReplicas":       OnAllReplicas,
		"unset":               {},
	}
	for want, mode := range cases {
		if got := mode.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}

func TestDeliveryModeZeroFollowsRunMode(t *testing.T) {
	var d DeliveryMode
	if !d.IsZero() {
		t.Fatal("zero DeliveryMode must report IsZero")
	}
	if Group.IsZero() || Broadcast.IsZero() || Group == Broadcast {
		t.Fatal("Group and Broadcast must be distinct non-zero values")
	}
}

func TestSurfaceString(t *testing.T) {
	if SurfaceReconcile.String() != "reconcile" || SurfaceWorker.String() != "worker" {
		t.Fatalf("got %q / %q", SurfaceReconcile, SurfaceWorker)
	}
	if Surface(0).String() != "unknown" {
		t.Fatal("unknown surfaces must not fabricate a name")
	}
}

func TestReservedHeaderNames(t *testing.T) {
	for _, h := range []string{HeaderSchemaVersion, HeaderEnqueuedAt, HeaderMessageID, HeaderAttempt, HeaderSnoozes} {
		if !strings.HasPrefix(h, HeaderPrefix) {
			t.Errorf("%q must carry the reserved prefix", h)
		}
	}
}

func TestWorkerHeaderValues(t *testing.T) {
	cases := map[string]string{
		HeaderMessageID: "converge.message-id",
		HeaderAttempt:   "converge.attempt",
		HeaderSnoozes:   "converge.snoozes",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("header = %q, want %q", got, want)
		}
	}
}

func TestRateZeroIsUnlimited(t *testing.T) {
	if !(Rate{}).IsZero() {
		t.Fatal("zero Rate must mean unlimited")
	}
	if (Rate{Events: 10, Per: time.Second}).IsZero() {
		t.Fatal("a set Rate must not be zero")
	}
}

var _ = []Event{
	WakeDiscarded{},
	PassOverrun{},
	IDParked{},
	VersionZero{},
	WrongSurfaceSignal{},
	BackoffFallback{},
	MessageDiscarded{},
	MessageDeadLettered{},
	QueueDepth{},
}

func TestWakeDiscardReasonZeroIsHonest(t *testing.T) {
	if got := (WakeDiscardReason{}).String(); got != "unknown" {
		t.Fatalf("zero reason = %q, want unknown, never a fabricated name", got)
	}
	if !(WakeDiscardReason{}).IsZero() || DiscardParked.IsZero() {
		t.Fatal("IsZero semantics broken")
	}
}

func TestDeadLetterReasonZeroIsHonest(t *testing.T) {
	if got := (DeadLetterReason{}).String(); got != "unknown" {
		t.Fatalf("zero reason = %q, want unknown, never a fabricated name", got)
	}
	if !(DeadLetterReason{}).IsZero() || DeadLetterMaxAttempts.IsZero() {
		t.Fatal("IsZero semantics broken")
	}
}

func TestDeadLetterReasonString(t *testing.T) {
	cases := map[string]DeadLetterReason{
		"max-attempts":   DeadLetterMaxAttempts,
		"max-age":        DeadLetterMaxAge,
		"wrong-kind":     DeadLetterWrongKind,
		"schema-version": DeadLetterSchemaVersion,
		"undecodable":    DeadLetterUndecodable,
		"wrong-surface":  DeadLetterWrongSurface,
	}
	for want, reason := range cases {
		if got := reason.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}
