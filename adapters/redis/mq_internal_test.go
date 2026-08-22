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

	mq := NewStreamsMQ(client, StreamsOpts{Clock: clock, Visibility: time.Minute})
	m, ok := mq.(*streamsMQ)
	if !ok {
		t.Fatal("NewStreamsMQ must return *streamsMQ")
	}

	const queue = "q"
	const total = 80
	for i := 0; i < total; i++ {
		if err := mq.Publish(ctx, queue, converge.Message{Payload: []byte{byte(i)}}); err != nil {
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
