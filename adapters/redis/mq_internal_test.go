package convredis

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStreamsMQReconcilesDeepPEL(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	ctx := context.Background()
	clock := convergetest.NewClock(time.Unix(1700000000, 0))

	m := &streamsMQ{rdb: client, clock: clock, visibility: time.Minute}

	const queue = "q"
	const total = 2*pendingPageCount + 1
	for i := 0; i < total; i++ {
		if err := m.Publish(ctx, queue, converge.Message{Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.ensureGroup(ctx, queue, reservedGroup); err != nil {
		t.Fatal(err)
	}
	if _, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    reservedGroup,
		Consumer: "foreign",
		Streams:  []string{streamKey(queue), newEntriesID},
		Count:    total,
	}).Result(); err != nil {
		t.Fatal(err)
	}

	if n, err := client.ZCard(ctx, pendingKey(queue, reservedGroup)).Result(); err != nil || n != 0 {
		t.Fatalf("pending set holds %d entries (err %v), want 0 before reconcile", n, err)
	}

	if err := m.reconcilePending(ctx, queue, reservedGroup); err != nil {
		t.Fatal(err)
	}

	if n, err := client.ZCard(ctx, pendingKey(queue, reservedGroup)).Result(); err != nil || n != total {
		t.Fatalf("pending set holds %d entries (err %v), want %d after reconcile", n, err, total)
	}
}

func TestEntryIDAdvanced(t *testing.T) {
	cases := []struct {
		name        string
		next, start string
		want        bool
	}{
		{"first page from range start", "1700000000000-4", pendingRangeMin, true},
		{"ordinary increment", "1700000000000-73", "1700000000000-9", true},
		{"digit width does not confuse the comparison", "1700000000000-100", "1700000000000-99", true},
		{"ms rollover", "1700000000001-0", "1700000000000-63", true},
		{"uint64 seq overflow wraps backward", "1700000000000-0", "1700000000000-18446744073709551615", false},
		{"equal is not advanced", "1700000000000-9", "1700000000000-9", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := entryIDAdvanced(c.next, c.start); got != c.want {
				t.Fatalf("entryIDAdvanced(%q, %q) = %v, want %v", c.next, c.start, got, c.want)
			}
		})
	}
}
