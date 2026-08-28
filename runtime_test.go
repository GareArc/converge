package converge

import (
	"testing"
	"time"
)

func TestNewAppliesDefaults(t *testing.T) {
	rt, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rt.opts.LeaseTTL != 30*time.Second || rt.opts.DrainTimeout != 30*time.Second {
		t.Fatalf("defaults not applied: %+v", rt.opts)
	}
	if rt.opts.Observer == nil || rt.opts.Clock == nil {
		t.Fatal("nil Observer/Clock must default to no-op/wall clock")
	}
}

func TestNewRejectsNegativeDurations(t *testing.T) {
	if _, err := New(Options{LeaseTTL: -time.Second}); err == nil {
		t.Fatal("negative LeaseTTL must be rejected")
	}
	if _, err := New(Options{DrainTimeout: -time.Second}); err == nil {
		t.Fatal("negative DrainTimeout must be rejected")
	}
}

func TestNewClonesMiddleware(t *testing.T) {
	mws := []Middleware{func(next Handler) Handler { return next }}
	rt, err := New(Options{Middleware: mws})
	if err != nil {
		t.Fatal(err)
	}
	mws[0] = nil
	if rt.opts.Middleware[0] == nil {
		t.Fatal("New must clone Options.Middleware")
	}
}
