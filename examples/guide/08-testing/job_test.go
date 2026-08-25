package testingguide

import (
	"testing"

	"github.com/GareArc/converge/convergetest"
)

func TestSyncTenantsChecksEveryTenant(t *testing.T) {
	h := convergetest.New(t)
	store := &Store{}
	rt := h.Build(t)
	if err := Register(rt, store); err != nil {
		t.Fatal(err)
	}

	h.Drain(t)
	store.Synced = nil

	h.RunPass(t, "sync-tenants")

	h.AssertReconciled(t, "sync-tenants", "acme")
	h.AssertReconciled(t, "sync-tenants", "globex")
	if len(store.Synced) != 2 {
		t.Fatalf("synced %v, want two tenants", store.Synced)
	}
}
