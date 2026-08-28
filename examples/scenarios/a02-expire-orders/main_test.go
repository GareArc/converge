package main

import (
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/reconcile"
)

func TestUnpaidOrdersAreCancelled(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	store := newStore(h.Clock().Now)
	if err := reconcile.Register(rt, expireUnpaidOrders(store)); err != nil {
		t.Fatal(err)
	}
	store.create("o-1")
	h.Clock().Advance(31 * time.Minute)
	store.create("o-2")
	h.Drain(t)
	if got := store.status("o-1"); got != statusCancelled {
		t.Fatalf("order o-1 status = %q, want %q", got, statusCancelled)
	}
	if got := store.status("o-2"); got != statusPending {
		t.Fatalf("order o-2 status = %q, want %q", got, statusPending)
	}
}
