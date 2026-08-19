package convredis_test

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/convergetest/portcheck"
)

func TestKVPortMiniredis(t *testing.T) {
	var advance func(d time.Duration)
	portcheck.KV(t, func(t *testing.T) converge.KV {
		client, _, adv := openMini(t)
		advance = adv
		return convredis.NewKV(client)
	}, portcheck.KVOptions{Advance: func(d time.Duration) { advance(d) }})
}

func TestKVPortRealRedis(t *testing.T) {
	portcheck.KV(t, func(t *testing.T) converge.KV {
		return convredis.NewKV(openReal(t))
	}, portcheck.KVOptions{})
}

func TestKVCASNilOldVsEmptyOld(t *testing.T) {
	client, _, _ := openMini(t)
	k := convredis.NewKV(client)
	ctx := context.Background()
	if ok, err := k.SetCAS(ctx, "k", []byte{}, []byte("v")); err != nil || ok {
		t.Fatalf("empty-old on absent key = %v, %v; want false, nil", ok, err)
	}
	if ok, err := k.SetCAS(ctx, "k", nil, []byte("v")); err != nil || !ok {
		t.Fatalf("nil-old create = %v, %v", ok, err)
	}
}
