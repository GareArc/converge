package reconcile

import (
	"testing"
	"time"
)

var _ PeriodicTrigger = (*scheduleTrigger)(nil)

func TestEveryNextIsAnchored(t *testing.T) {
	c := Every(time.Hour)
	anchor := wqStart
	cases := []struct {
		after time.Time
		want  time.Time
	}{
		{anchor, anchor.Add(time.Hour)},
		{anchor.Add(30 * time.Minute), anchor.Add(time.Hour)},
		{anchor.Add(time.Hour), anchor.Add(2 * time.Hour)},
		{anchor.Add(150 * time.Minute), anchor.Add(3 * time.Hour)},
		{anchor.Add(-time.Minute), anchor},
	}
	for _, tc := range cases {
		if got := c.next(anchor, tc.after); !got.Equal(tc.want) {
			t.Fatalf("next(%v) = %v, want %v", tc.after, got, tc.want)
		}
	}
}

func TestCronNextUTCDefault(t *testing.T) {
	c := Cron("0 3 * * *", CronOpts{})
	if c.err != nil {
		t.Fatal(c.err)
	}
	after := time.Date(2026, 8, 16, 2, 0, 0, 0, time.UTC)
	want := time.Date(2026, 8, 16, 3, 0, 0, 0, time.UTC)
	if got := c.next(time.Time{}, after); !got.Equal(want) {
		t.Fatalf("next = %v, want %v", got, want)
	}
}

func TestCronHonorsLocation(t *testing.T) {
	loc := time.FixedZone("UTC+9", 9*3600)
	c := Cron("0 3 * * *", CronOpts{Location: loc})
	if c.err != nil {
		t.Fatal(c.err)
	}
	after := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	want := time.Date(2026, 8, 17, 3, 0, 0, 0, loc)
	if got := c.next(time.Time{}, after); !got.Equal(want) {
		t.Fatalf("next = %v, want %v", got, want)
	}

	west := time.FixedZone("UTC-8", -8*3600)
	cWest := Cron("0 3 * * *", CronOpts{Location: west})
	if cWest.err != nil {
		t.Fatal(cWest.err)
	}
	afterWest := time.Date(2026, 8, 16, 5, 0, 0, 0, time.UTC)
	wantWest := time.Date(2026, 8, 16, 3, 0, 0, 0, west)
	if got := cWest.next(time.Time{}, afterWest); !got.Equal(wantWest) {
		t.Fatalf("next = %v, want %v", got, wantWest)
	}
}

func TestCronNextIsAlwaysInTheFuture(t *testing.T) {
	zones := []*time.Location{
		time.UTC,
		time.FixedZone("UTC+9", 9*3600),
		time.FixedZone("UTC-8", -8*3600),
	}
	offsets := []time.Duration{0, 5 * time.Hour, 13 * time.Hour, 22 * time.Hour, 36 * time.Hour}
	for _, zone := range zones {
		c := Cron("0 3 * * *", CronOpts{Location: zone})
		if c.err != nil {
			t.Fatal(c.err)
		}
		for _, off := range offsets {
			after := wqStart.Add(off)
			if got := c.next(time.Time{}, after); !got.After(after) {
				t.Fatalf("zone %v after %v: next = %v, want strictly after %v", zone, after, got, after)
			}
		}
	}
}

func TestCronRejectsBadExpressions(t *testing.T) {
	if Cron("@daily", CronOpts{}).err == nil {
		t.Fatal("descriptors must be rejected")
	}
	if Cron("not a cron", CronOpts{}).err == nil {
		t.Fatal("garbage must be rejected")
	}
	if Cron("0 0 0 3 * * *", CronOpts{}).err == nil {
		t.Fatal("wrong field count must be rejected")
	}
}

func TestBoundaries(t *testing.T) {
	c := Every(time.Hour)
	last := wqStart
	got := boundaries(c, last, last.Add(3*time.Hour+time.Minute))
	if len(got) != 3 {
		t.Fatalf("boundaries = %v", got)
	}
	for i, b := range got {
		if want := last.Add(time.Duration(i+1) * time.Hour); !b.Equal(want) {
			t.Fatalf("boundary %d = %v, want %v", i, b, want)
		}
	}
	if got := boundaries(c, last, last.Add(30*time.Minute)); len(got) != 0 {
		t.Fatalf("no boundary due yet, got %v", got)
	}
	if got := boundaries(Every(time.Second), last, last.Add(24*time.Hour)); len(got) != maxMissedBoundaries {
		t.Fatalf("boundary scan must cap at %d, got %d", maxMissedBoundaries, len(got))
	}
}

func TestScheduleNextAfter(t *testing.T) {
	s := Schedule(SingleID(), Every(time.Hour)).(*scheduleTrigger)
	if got := s.NextAfter(wqStart); !got.Equal(wqStart.Add(time.Hour)) {
		t.Fatalf("NextAfter = %v", got)
	}
}
