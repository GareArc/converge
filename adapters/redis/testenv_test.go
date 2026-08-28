package convredis_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

const realAddrEnv = "CONVREDIS_TEST_ADDR"

func openMini(t *testing.T) (*redis.Client, *convergetest.Clock, func(d time.Duration)) {
	t.Helper()
	_, client, clock, advance := openMiniServer(t)
	return client, clock, advance
}

func openMiniServer(t *testing.T) (*miniredis.Miniredis, *redis.Client, *convergetest.Clock, func(d time.Duration)) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	start := time.Unix(1700000000, 0)
	mr.SetTime(start)
	clock := convergetest.NewClock(start)
	advance := func(d time.Duration) {
		mr.FastForward(d)
		clock.Advance(d)
		mr.SetTime(clock.Now())
	}
	return mr, client, clock, advance
}

func failCommand(t *testing.T, mr *miniredis.Miniredis, name string) func() {
	t.Helper()
	mr.Server().SetPreHook(func(c *server.Peer, cmd string, args ...string) bool {
		if cmd != name {
			return false
		}
		c.WriteError("ERR injected failure")
		return true
	})
	return func() { mr.Server().SetPreHook(nil) }
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
