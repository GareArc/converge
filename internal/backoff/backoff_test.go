package backoff

import (
	"testing"
	"time"
)

func TestJitterRange(t *testing.T) {
	d := 10 * time.Second
	for i := 0; i < 50; i++ {
		got := Jitter(d)
		if got < d/2 || got > d {
			t.Fatalf("Jitter(%v) = %v, want within [%v, %v]", d, got, d/2, d)
		}
	}
}

func TestFloor(t *testing.T) {
	if got := Floor(time.Hour); got != time.Hour {
		t.Fatalf("above-floor delay changed: %v", got)
	}
	for i := 0; i < 50; i++ {
		got := Floor(0)
		if got < floorMin || got > floorMin+floorMin/2 {
			t.Fatalf("Floor(0) = %v, want within [%v, %v]", got, floorMin, floorMin+floorMin/2)
		}
	}
}
