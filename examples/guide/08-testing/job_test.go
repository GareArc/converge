package testingguide

import (
	"testing"

	"github.com/GareArc/converge/convergetest"
)

func TestSyncInventoryChecksEverySKU(t *testing.T) {
	h := convergetest.New(t)
	store := &Store{}
	rt := h.Build(t)
	if err := Register(rt, store); err != nil {
		t.Fatal(err)
	}

	h.Drain(t)
	store.Synced = nil

	h.RunPass(t, "sync-inventory")

	h.AssertReconciled(t, "sync-inventory", "SKU-1001")
	h.AssertReconciled(t, "sync-inventory", "SKU-1002")
	if len(store.Synced) != 2 {
		t.Fatalf("synced %v, want two SKUs", store.Synced)
	}
}
