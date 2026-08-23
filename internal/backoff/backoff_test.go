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

type curveDelayCase struct {
	attempt int
	base    time.Duration
	capped  bool
}

type curveFixture struct {
	name  string
	curve Curve
	cases []curveDelayCase
}

var curveFixtures = []curveFixture{
	{
		name:  "productionPolicy",
		curve: Curve{Min: time.Second, Max: 15 * time.Minute},
		cases: []curveDelayCase{
			{1, time.Second, false},
			{2, 2 * time.Second, false},
			{3, 4 * time.Second, false},
			{5, 16 * time.Second, false},
			{11, 15 * time.Minute, true},
			{100, 15 * time.Minute, true},
		},
	},
	{
		name:  "minAtHalfMaxOddMax",
		curve: Curve{Min: 6*time.Second + 500*time.Millisecond, Max: 13*time.Second + 1},
		cases: []curveDelayCase{
			{1, 6*time.Second + 500*time.Millisecond, false},
			{2, 13*time.Second + 1, true},
			{3, 13*time.Second + 1, true},
			{100, 13*time.Second + 1, true},
		},
	},
}

func TestCurveDelayGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	const sampleCount = 50
	for _, fx := range curveFixtures {
		t.Run(fx.name, func(t *testing.T) {
			var prev curveDelayCase
			var prevMax time.Duration
			havePrev := false
			for _, tc := range fx.cases {
				if tc.capped != (tc.base == fx.curve.Max) {
					t.Fatalf("attempt %d: capped flag disagrees with base %v", tc.attempt, tc.base)
				}
				floor := tc.base / 2
				curMin := tc.base
				var curMax time.Duration
				for i := 0; i < sampleCount; i++ {
					d := fx.curve.Delay(tc.attempt)
					if d < floor {
						t.Fatalf("Delay(%d) = %v, want >= floor %v", tc.attempt, d, floor)
					}
					if d > tc.base {
						t.Fatalf("Delay(%d) = %v, want <= base %v", tc.attempt, d, tc.base)
					}
					if d > fx.curve.Max {
						t.Fatalf("Delay(%d) = %v, want <= Max %v", tc.attempt, d, fx.curve.Max)
					}
					if d < curMin {
						curMin = d
					}
					if d > curMax {
						curMax = d
					}
				}
				if havePrev && !(prev.capped && tc.capped) && prevMax > curMin {
					t.Fatalf("Delay(%d) max %v exceeds Delay(%d) min %v, want non-decreasing", prev.attempt, prevMax, tc.attempt, curMin)
				}
				prev = tc
				prevMax = curMax
				havePrev = true
			}
		})
	}
}
