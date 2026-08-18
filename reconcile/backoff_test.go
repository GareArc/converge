package reconcile

import (
	"testing"
	"time"
)

func TestBackoffAfterGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	cases := []struct {
		fails int
		base  time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{11, backoffMax},
		{100, backoffMax},
	}
	for _, c := range cases {
		for i := 0; i < 50; i++ {
			d := backoffAfter(c.fails)
			if d < c.base/2 || d > c.base {
				t.Fatalf("backoffAfter(%d) = %v, want within [%v, %v]", c.fails, d, c.base/2, c.base)
			}
		}
	}
}
