package reconcile

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/keys"
)

func newTestParkStore(kv converge.KV, job string) *kvParkStore {
	return &kvParkStore{
		kv:        kv,
		namespace: "ns",
		job:       job,
		retry:     func(context.Context) bool { return false },
	}
}

func TestKVParkStoreReadTable(t *testing.T) {
	rows := []struct {
		name   string
		raw    string
		stored bool
		want   Version
		wantOK bool
	}{
		{name: "absent"},
		{name: "zero", raw: "0", stored: true, wantOK: true},
		{name: "value", raw: "7", stored: true, want: 7, wantOK: true},
		{name: "max", raw: "18446744073709551615", stored: true, want: Version(math.MaxUint64), wantOK: true},
		{name: "empty", raw: "", stored: true},
		{name: "corrupt", raw: "junk", stored: true},
		{name: "negative", raw: "-1", stored: true},
		{name: "overflow", raw: "18446744073709551616", stored: true},
		{name: "padded", raw: " 7", stored: true},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			ctx := context.Background()
			kv := inmem.NewKV()
			s := newTestParkStore(kv, "job")
			if r.stored {
				if err := kv.Set(ctx, keys.ReconcileParked("ns", "job", "a"), []byte(r.raw), 0); err != nil {
					t.Fatal(err)
				}
			}
			v, ok := s.read(ctx, "a")
			if v != r.want || ok != r.wantOK {
				t.Fatalf("read = (%d, %v), want (%d, %v)", v, ok, r.want, r.wantOK)
			}
		})
	}
}

func TestKVParkStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	kv := inmem.NewKV()
	s := newTestParkStore(kv, "job")
	if !s.enabled() {
		t.Fatal("a KV park store is enabled")
	}
	s.clear(ctx, "a")
	s.mark(ctx, "a", 42)
	if v, ok := s.read(ctx, "a"); !ok || v != 42 {
		t.Fatalf("read after mark = (%d, %v), want (42, true)", v, ok)
	}
	raw, ok, err := kv.Get(ctx, keys.ReconcileParked("ns", "job", "a"))
	if err != nil || !ok || string(raw) != "42" {
		t.Fatalf("stored mark = (%q, %v, %v), want (\"42\", true, nil)", raw, ok, err)
	}
	s.clear(ctx, "a")
	if v, ok := s.read(ctx, "a"); ok {
		t.Fatalf("read after clear = (%d, %v), want absent", v, ok)
	}
}

func TestKVParkStoreScanVisitsMarkedIDs(t *testing.T) {
	ctx := context.Background()
	kv := inmem.NewKV()
	s := newTestParkStore(kv, "job")
	want := map[ID]bool{}
	for i := range 150 {
		id := ID("id/" + strconv.Itoa(i))
		s.mark(ctx, id, Version(i))
		want[id] = true
	}
	newTestParkStore(kv, "other").mark(ctx, "a", 1)
	got := map[ID]bool{}
	s.scan(ctx, func(id ID) { got[id] = true })
	if len(got) != len(want) {
		t.Fatalf("scan visited %d ids, want %d", len(got), len(want))
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("scan never visited %q", id)
		}
	}
}

func TestNoopParkStoreIsInert(t *testing.T) {
	ctx := context.Background()
	var s parkStore = noopParkStore{}
	if s.enabled() {
		t.Fatal("the noop park store is not enabled")
	}
	s.mark(ctx, "a", 7)
	if v, ok := s.read(ctx, "a"); ok || v != 0 {
		t.Fatalf("read = (%d, %v), want absent", v, ok)
	}
	s.clear(ctx, "a")
	visits := 0
	s.scan(ctx, func(ID) { visits++ })
	if visits != 0 {
		t.Fatalf("scan visited %d ids, want none", visits)
	}
}
