package mw_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/mw"
)

func tag(name string, log *[]string) converge.Middleware {
	return func(next converge.Handler) converge.Handler {
		return func(ctx context.Context, run converge.Run) error {
			*log = append(*log, name)
			return next(ctx, run)
		}
	}
}

func TestChainAppliesOutermostFirst(t *testing.T) {
	var log []string
	h := mw.Chain(
		[]converge.Middleware{tag("global", &log), tag("spec", &log)},
		func(ctx context.Context, run converge.Run) error {
			log = append(log, "handler")
			return nil
		},
	)
	if err := h(context.Background(), converge.Run{Job: "j"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"global", "spec", "handler"}; !reflect.DeepEqual(log, want) {
		t.Fatalf("order = %v, want %v", log, want)
	}
}

func TestChainEmptyIsFinal(t *testing.T) {
	called := false
	h := mw.Chain(nil, func(ctx context.Context, run converge.Run) error {
		called = true
		return nil
	})
	h(context.Background(), converge.Run{})
	if !called {
		t.Fatal("empty chain must call the final handler")
	}
}
