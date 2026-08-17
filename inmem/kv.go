// Package inmem provides stdlib-only implementations of converge's ports
// for development and tests: single process, no persistence.
package inmem

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

type KV struct {
	mu    sync.Mutex
	clock converge.Clock
	items map[string]kvItem
}

type kvItem struct {
	val []byte
	exp time.Time // zero = no expiry
}

func NewKV() *KV { return NewKVWithClock(nil) }

// NewKVWithClock: nil clock = wall clock.
func NewKVWithClock(c converge.Clock) *KV {
	if c == nil {
		c = wallClock{}
	}
	return &KV{clock: c, items: map[string]kvItem{}}
}

func (k *KV) live(key string) ([]byte, bool) {
	it, ok := k.items[key]
	if !ok {
		return nil, false
	}
	if !it.exp.IsZero() && !it.exp.After(k.clock.Now()) {
		delete(k.items, key)
		return nil, false
	}
	return it.val, true
}

func (k *KV) Get(_ context.Context, key string) ([]byte, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	val, ok := k.live(key)
	if !ok {
		return nil, false, nil
	}
	return bytes.Clone(val), true, nil
}

func (k *KV) SetCAS(_ context.Context, key string, old, new []byte) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.live(key)
	if old == nil {
		if ok {
			return false, nil
		}
	} else if !ok || !bytes.Equal(cur, old) {
		return false, nil
	}
	k.items[key] = kvItem{val: bytes.Clone(new)}
	return true, nil
}

func (k *KV) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	it := kvItem{val: bytes.Clone(val)}
	if ttl > 0 {
		it.exp = k.clock.Now().Add(ttl)
	}
	k.items[key] = it
	return nil
}

func (k *KV) Delete(_ context.Context, key string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.items, key)
	return nil
}

func (k *KV) Scan(_ context.Context, prefix, cursor string) ([]string, string, error) {
	const page = 100
	k.mu.Lock()
	defer k.mu.Unlock()
	var keys []string
	for key := range k.items {
		if strings.HasPrefix(key, prefix) && key > cursor {
			if _, ok := k.live(key); ok {
				keys = append(keys, key)
			}
		}
	}
	sort.Strings(keys)
	if len(keys) > page {
		keys = keys[:page]
	}
	var next string
	if len(keys) == page {
		next = keys[page-1]
	}
	return keys, next, nil
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
