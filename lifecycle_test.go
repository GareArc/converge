package converge

import (
	"testing"
	"time"
)

var lifecycleCutover = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func TestStopConditionZeroIsHonest(t *testing.T) {
	var c StopCondition
	if !c.IsZero() {
		t.Fatal("zero StopCondition must report IsZero")
	}
	if got := c.String(); got != "none" {
		t.Fatalf("String() = %q, want %q", got, "none")
	}
	if Deadline(lifecycleCutover).IsZero() {
		t.Fatal("Deadline must not be zero")
	}
	if StopKey("migration/finished").IsZero() {
		t.Fatal("StopKey must not be zero")
	}
}

func TestStopConditionString(t *testing.T) {
	cases := map[string]StopCondition{
		"deadline 2026-09-01T00:00:00Z": Deadline(lifecycleCutover),
		"stop key migration/finished":   StopKey("migration/finished"),
		"none":                          {},
	}
	for want, c := range cases {
		if got := c.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}

func TestStopConditionAccessors(t *testing.T) {
	d := Deadline(lifecycleCutover)
	if at, ok := d.deadlineAt(); !ok || !at.Equal(lifecycleCutover) {
		t.Fatalf("deadlineAt() = %v, %v, want %v, true", at, ok, lifecycleCutover)
	}
	if _, ok := d.stopKey(); ok {
		t.Fatal("a Deadline must not report a stop key")
	}

	k := StopKey("migration/finished")
	if key, ok := k.stopKey(); !ok || key != "migration/finished" {
		t.Fatalf("stopKey() = %q, %v, want %q, true", key, ok, "migration/finished")
	}
	if _, ok := k.deadlineAt(); ok {
		t.Fatal("a StopKey must not report a deadline")
	}

	var zero StopCondition
	if _, ok := zero.deadlineAt(); ok {
		t.Fatal("zero StopCondition must not report a deadline")
	}
	if _, ok := zero.stopKey(); ok {
		t.Fatal("zero StopCondition must not report a stop key")
	}
}

func TestStateZeroIsHonest(t *testing.T) {
	var s State
	if !s.IsZero() {
		t.Fatal("zero State must report IsZero")
	}
	if got := s.String(); got != "unknown" {
		t.Fatalf("String() = %q, want %q, never a fabricated name", got, "unknown")
	}
	for _, st := range []State{NotStarted, Active, Destroyed} {
		if st.IsZero() {
			t.Fatalf("%v must not be zero", st)
		}
	}
	if NotStarted == Active || Active == Destroyed || NotStarted == Destroyed {
		t.Fatal("states must be distinct")
	}
}

func TestStateString(t *testing.T) {
	cases := map[string]State{
		"not started": NotStarted,
		"active":      Active,
		"destroyed":   Destroyed,
		"unknown":     {},
	}
	for want, s := range cases {
		if got := s.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}
