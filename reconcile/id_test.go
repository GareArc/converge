package reconcile_test

import (
	"testing"

	"github.com/GareArc/converge/reconcile"
)

func TestJoinSplitRoundTrip(t *testing.T) {
	cases := [][]string{
		{"tenant", "app"},
		{"a/b", "c"},
		{"50%", "off"},
		{"%2F", "literal"},
		{"", "empty-first"},
		{"unicode-日本", "ok"},
		{"a", "b", "c", "d"},
	}
	for _, parts := range cases {
		id := reconcile.JoinID(parts...)
		got := reconcile.SplitID(id)
		if len(got) != len(parts) {
			t.Fatalf("SplitID(%q) has %d parts, want %d", id, len(got), len(parts))
		}
		for i := range parts {
			if got[i] != parts[i] {
				t.Fatalf("part %d of %q = %q, want %q", i, id, got[i], parts[i])
			}
		}
	}
}

func TestSplit2ChecksArity(t *testing.T) {
	a, b, err := reconcile.Split2(reconcile.JoinID("t", "x"))
	if err != nil || a != "t" || b != "x" {
		t.Fatalf("Split2 = %q %q %v", a, b, err)
	}
	if _, _, err := reconcile.Split2(reconcile.ID("only-one")); err == nil {
		t.Fatal("Split2 must error on wrong arity")
	}
	if _, _, err := reconcile.Split2(reconcile.JoinID("a", "b", "c")); err == nil {
		t.Fatal("Split2 must error on three parts")
	}
}
