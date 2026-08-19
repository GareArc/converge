package convredis_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const realAddrEnv = "CONVREDIS_TEST_ADDR"

func openMini(t *testing.T) (*redis.Client, *convergetest.Clock, func(d time.Duration)) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	clock := convergetest.NewClock(time.Unix(1700000000, 0))
	advance := func(d time.Duration) {
		mr.FastForward(d)
		clock.Advance(d)
	}
	return client, clock, advance
}

func openReal(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv(realAddrEnv)
	if addr == "" {
		t.Skipf("%s not set", realAddrEnv)
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 9})
	t.Cleanup(func() { client.Close() })
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}
