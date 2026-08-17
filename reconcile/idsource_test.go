package reconcile

import (
	"context"
	"errors"
	"testing"
)

func TestSingleIDYieldsOneAnonymousID(t *testing.T) {
	s := SingleID()
	ids, next, err := s.page(context.Background(), "")
	if err != nil || next != "" || len(ids) != 1 || ids[0] != "" {
		t.Fatalf("page = %v %q %v", ids, next, err)
	}
	if !s.single || s.paged || s.IsZero() {
		t.Fatal("SingleID must be single, unpaged, non-zero")
	}
}

func TestIDsReturnsWholeListAsOnePage(t *testing.T) {
	s := IDs(func(context.Context) ([]ID, error) { return []ID{"a", "b"}, nil })
	ids, next, err := s.page(context.Background(), "ignored")
	if err != nil || next != "" || len(ids) != 2 {
		t.Fatalf("page = %v %q %v", ids, next, err)
	}
	boom := errors.New("boom")
	e := IDs(func(context.Context) ([]ID, error) { return nil, boom })
	if _, _, err := e.page(context.Background(), ""); !errors.Is(err, boom) {
		t.Fatal("source error must pass through")
	}
}

func TestStringIDsConverts(t *testing.T) {
	s := StringIDs(func(context.Context) ([]string, error) { return []string{"x"}, nil })
	ids, _, err := s.page(context.Background(), "")
	if err != nil || len(ids) != 1 || ids[0] != "x" {
		t.Fatalf("page = %v %v", ids, err)
	}
}

func TestIDsByPagePassesCursorThrough(t *testing.T) {
	s := IDsByPage(func(_ context.Context, cursor string) ([]ID, string, error) {
		if cursor == "" {
			return []ID{"p1"}, "c1", nil
		}
		return []ID{"p2"}, "", nil
	})
	if !s.paged {
		t.Fatal("IDsByPage must be paged")
	}
	ids, next, _ := s.page(context.Background(), "")
	if ids[0] != "p1" || next != "c1" {
		t.Fatalf("first page = %v %q", ids, next)
	}
	ids, next, _ = s.page(context.Background(), "c1")
	if ids[0] != "p2" || next != "" {
		t.Fatalf("second page = %v %q", ids, next)
	}
}

func TestNilFuncsAreZero(t *testing.T) {
	if !IDs(nil).IsZero() || !StringIDs(nil).IsZero() || !IDsByPage(nil).IsZero() {
		t.Fatal("nil-func sources must be zero")
	}
	if !(IDSource{}).IsZero() {
		t.Fatal("zero IDSource must report IsZero")
	}
}
