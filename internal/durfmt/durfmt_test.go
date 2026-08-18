package durfmt

import (
	"testing"
	"time"
)

func TestFormatTrimsTrailingZeroUnits(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{time.Hour, "1h"},
		{time.Second, "1s"},
		{15 * time.Minute, "15m"},
		{24 * time.Hour, "24h"},
		{0, "0s"},
	}
	for _, c := range cases {
		if got := Format(c.d); got != c.want {
			t.Fatalf("Format(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestFormatKeepsSignificantSubUnits(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{90 * time.Second, "1m30s"},
		{time.Hour + 30*time.Second, "1h0m30s"},
		{time.Hour + 30*time.Minute, "1h30m"},
	}
	for _, c := range cases {
		if got := Format(c.d); got != c.want {
			t.Fatalf("Format(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestFormatFallsBackForSubSecondDurations(t *testing.T) {
	d := 500 * time.Millisecond
	if got := Format(d); got != d.String() {
		t.Fatalf("Format(%v) = %q, want %q", d, got, d.String())
	}
}

func TestFormatHandlesNegativeDurations(t *testing.T) {
	if got := Format(-time.Hour); got != "-1h" {
		t.Fatalf("Format(-1h) = %q, want -1h", got)
	}
}
