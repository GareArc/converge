package backoff

import (
	"testing"
	"time"
)

func TestJitterRange(t *testing.T) {
	d := 10 * time.Second
	for i := 0; i < 50; i++ {
		got := jitter(d)
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

func TestCurveDelayGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	curves := map[string]Curve{
		"reconcile": {Min: time.Second, Max: 15 * time.Minute},
		"worker":    {Min: time.Second, Max: 15 * time.Minute},
	}
	cases := []struct {
		attempt int
		base    time.Duration
		capped  bool
	}{
		{1, time.Second, false},
		{2, 2 * time.Second, false},
		{3, 4 * time.Second, false},
		{5, 16 * time.Second, false},
		{11, 15 * time.Minute, true},
		{100, 15 * time.Minute, true},
	}
	for name, c := range curves {
		t.Run(name, func(t *testing.T) {
			var prevBase time.Duration
			for _, tc := range cases {
				if tc.base < prevBase {
					t.Fatalf("attempt %d: base %v regressed below previous %v", tc.attempt, tc.base, prevBase)
				}
				prevBase = tc.base
				if tc.capped != (tc.base == c.Max) {
					t.Fatalf("attempt %d: capped flag disagrees with base %v", tc.attempt, tc.base)
				}
				for i := 0; i < 50; i++ {
					d := c.Delay(tc.attempt)
					floor := tc.base / 2
					if d < floor {
						t.Fatalf("Delay(%d) = %v, want >= floor %v", tc.attempt, d, floor)
					}
					if d > c.Max {
						t.Fatalf("Delay(%d) = %v, want <= Max %v", tc.attempt, d, c.Max)
					}
				}
			}
		})
	}
}
