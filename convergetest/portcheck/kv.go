package portcheck

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

type KVOptions struct {
	Advance func(d time.Duration)
}

func KV(t *testing.T, open func(t *testing.T) converge.KV, o KVOptions) {
	ctx := context.Background()

	t.Run("get absent", func(t *testing.T) {
		kv := open(t)
		val, ok, err := kv.Get(ctx, "missing")
		if err != nil || ok || val != nil {
			t.Fatalf("Get(missing) = %q, %v, %v; want nil, false, nil", val, ok, err)
		}
	})

	t.Run("set get roundtrip", func(t *testing.T) {
		kv := open(t)
		if err := kv.Set(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		val, ok, err := kv.Get(ctx, "k")
		if err != nil || !ok || string(val) != "v" {
			t.Fatalf("Get = %q, %v, %v", val, ok, err)
		}
	})

	t.Run("cas create only if absent", func(t *testing.T) {
		kv := open(t)
		if ok, err := kv.SetCAS(ctx, "k", nil, []byte("a")); err != nil || !ok {
			t.Fatalf("create = %v, %v", ok, err)
		}
		if ok, err := kv.SetCAS(ctx, "k", nil, []byte("b")); err != nil || ok {
			t.Fatalf("second create = %v, %v; want false", ok, err)
		}
		val, _, _ := kv.Get(ctx, "k")
		if string(val) != "a" {
			t.Fatalf("value clobbered: %q", val)
		}
	})

	t.Run("cas swap", func(t *testing.T) {
		kv := open(t)
		kv.SetCAS(ctx, "k", nil, []byte("a"))
		if ok, _ := kv.SetCAS(ctx, "k", []byte("wrong"), []byte("b")); ok {
			t.Fatal("swap with wrong old must fail")
		}
		if ok, err := kv.SetCAS(ctx, "k", []byte("a"), []byte("b")); err != nil || !ok {
			t.Fatalf("swap = %v, %v", ok, err)
		}
		val, _, _ := kv.Get(ctx, "k")
		if string(val) != "b" {
			t.Fatalf("got %q", val)
		}
	})

	t.Run("cas clears ttl", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		kv := open(t)
		kv.Set(ctx, "k", []byte("a"), time.Second)
		if ok, err := kv.SetCAS(ctx, "k", []byte("a"), []byte("b")); err != nil || !ok {
			t.Fatalf("swap = %v, %v", ok, err)
		}
		o.Advance(2 * time.Second)
		val, ok, _ := kv.Get(ctx, "k")
		if !ok || string(val) != "b" {
			t.Fatalf("SetCAS must clear TTL; got %q, %v", val, ok)
		}
	})

	t.Run("delete", func(t *testing.T) {
		kv := open(t)
		if err := kv.Delete(ctx, "absent"); err != nil {
			t.Fatalf("deleting absent key must not error: %v", err)
		}
		kv.Set(ctx, "k", []byte("v"), 0)
		kv.Delete(ctx, "k")
		if _, ok, _ := kv.Get(ctx, "k"); ok {
			t.Fatal("key survived delete")
		}
	})

	t.Run("ttl expires", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		kv := open(t)
		kv.Set(ctx, "k", []byte("v"), time.Second)
		if _, ok, _ := kv.Get(ctx, "k"); !ok {
			t.Fatal("expired before its TTL")
		}
		o.Advance(2 * time.Second)
		if _, ok, _ := kv.Get(ctx, "k"); ok {
			t.Fatal("survived its TTL")
		}
	})

	t.Run("scan pages every key at least once", func(t *testing.T) {
		kv := open(t)
		const n = 250
		for i := range n {
			kv.Set(ctx, fmt.Sprintf("scan/%03d", i), []byte("x"), 0)
		}
		kv.Set(ctx, "other/key", []byte("x"), 0)
		seen := map[string]int{}
		cursor := ""
		for {
			keys, next, err := kv.Scan(ctx, "scan/", cursor)
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				seen[k]++
			}
			if next == "" {
				break
			}
			cursor = next
		}
		if len(seen) != n {
			t.Fatalf("saw %d distinct keys, want %d", len(seen), n)
		}
		if _, ok := seen["other/key"]; ok {
			t.Fatal("scan crossed its prefix")
		}
	})
}
