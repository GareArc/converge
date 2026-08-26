package versions_test

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/versions"
	"github.com/GareArc/converge/reconcile"
)

func TestVersionsBumpMidRunDefersRatherThanFails(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}

	v := versions.Fixed(map[string]reconcile.Version{"widget": 1})

	err = reconcile.Register(rt, reconcile.Spec{
		Name:     "priced-widgets",
		Versions: v,
		Reconcile: func(context.Context, reconcile.ID) error {
			v.Bump("widget")
			return reconcile.ErrOutdated
		},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"widget"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	h.Drain(t)

	convergetest.Await(t, func() bool {
		for _, e := range h.Events() {
			rc, ok := e.(converge.RunCompleted)
			if ok && rc.Job == "priced-widgets" && rc.ID == "widget" && rc.Outcome == converge.Deferred {
				return true
			}
		}
		return false
	})

	for _, e := range h.Events() {
		rc, ok := e.(converge.RunCompleted)
		if !ok || rc.Job != "priced-widgets" || rc.ID != "widget" {
			continue
		}
		switch rc.Outcome {
		case converge.Deferred:
			if rc.Err != nil {
				t.Fatalf("Deferred outcome must carry a nil Err: %+v", rc)
			}
		case converge.Retrying:
			t.Fatalf("a mid-run version bump must defer, not be treated as a failed run: %+v", rc)
		}
	}
}
