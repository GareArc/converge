package pausegate

import (
	"context"
	"sync"
	"testing"
	"time"
)

func await(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func assertStable(t *testing.T, cond func() bool) {
	t.Helper()
	time.Sleep(20 * time.Millisecond)
	if !cond() {
		t.Fatal("state changed while it must hold")
	}
}

func TestNewSeedsInitialState(t *testing.T) {
	if g := New(false); g.Paused {
		t.Fatal("New(false) must start unpaused")
	}
	if g := New(true); !g.Paused {
		t.Fatal("New(true) must start paused")
	}
}

func TestSetPausedSameValueIsNoOp(t *testing.T) {
	g := New(false)
	changed, ch := g.SetPaused(false)
	if changed || ch != nil {
		t.Fatalf("same-value SetPaused must no-op, got changed=%v ch=%v", changed, ch)
	}
	g = New(true)
	changed, ch = g.SetPaused(true)
	if changed || ch != nil {
		t.Fatalf("same-value SetPaused must no-op, got changed=%v ch=%v", changed, ch)
	}
}

func TestSetPausedResumeWithNoWaiterReturnsNilChannel(t *testing.T) {
	g := New(true)
	changed, ch := g.SetPaused(false)
	if !changed || ch != nil {
		t.Fatalf("resume with no waiter must report changed with a nil channel, got changed=%v ch=%v", changed, ch)
	}
}

func TestSetPausedResumeWithWaiterReturnsItsChannel(t *testing.T) {
	g := New(true)
	ch1 := g.resumeChan()
	changed, ch2 := g.SetPaused(false)
	if !changed || ch2 != ch1 {
		t.Fatal("resume must hand back the exact channel a waiter is blocked on")
	}
}

func TestAwaitUnpausedReturnsImmediatelyWhenNotPaused(t *testing.T) {
	var mu sync.Mutex
	g := New(false)
	if !AwaitUnpaused(context.Background(), &mu, &g) {
		t.Fatal("AwaitUnpaused on an unpaused gate must return true immediately")
	}
}

func TestAwaitUnpausedBlocksUntilResumed(t *testing.T) {
	var mu sync.Mutex
	g := New(true)
	var resultMu sync.Mutex
	returned := false
	go func() {
		AwaitUnpaused(context.Background(), &mu, &g)
		resultMu.Lock()
		returned = true
		resultMu.Unlock()
	}()
	assertStable(t, func() bool { resultMu.Lock(); defer resultMu.Unlock(); return !returned })

	mu.Lock()
	_, ch := g.SetPaused(false)
	mu.Unlock()
	if ch != nil {
		close(ch)
	}
	await(t, func() bool { resultMu.Lock(); defer resultMu.Unlock(); return returned })
}

func TestAwaitUnpausedReturnsFalseOnCtxDoneWhileBlocked(t *testing.T) {
	var mu sync.Mutex
	g := New(true)
	ctx, cancel := context.WithCancel(context.Background())
	var resultMu sync.Mutex
	var ok, returned bool
	go func() {
		result := AwaitUnpaused(ctx, &mu, &g)
		resultMu.Lock()
		ok, returned = result, true
		resultMu.Unlock()
	}()
	assertStable(t, func() bool { resultMu.Lock(); defer resultMu.Unlock(); return !returned })
	cancel()
	await(t, func() bool { resultMu.Lock(); defer resultMu.Unlock(); return returned })
	resultMu.Lock()
	defer resultMu.Unlock()
	if ok {
		t.Fatal("AwaitUnpaused must return false once ctx is done before resume")
	}
}

func TestAwaitUnpausedReturnsFalseWhenCtxAlreadyDoneEvenIfUnpaused(t *testing.T) {
	var mu sync.Mutex
	g := New(false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if AwaitUnpaused(ctx, &mu, &g) {
		t.Fatal("AwaitUnpaused must return false once ctx is done, regardless of pause state")
	}
}

func TestAwaitUnpausedReturnsFalseWhenCtxAlreadyDoneEvenIfPausedThenResumed(t *testing.T) {
	var mu sync.Mutex
	g := New(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mu.Lock()
	_, ch := g.SetPaused(false)
	mu.Unlock()
	if ch != nil {
		close(ch)
	}
	if AwaitUnpaused(ctx, &mu, &g) {
		t.Fatal("a resume that lands after ctx is already done must not make AwaitUnpaused report true")
	}
}
