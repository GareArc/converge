package convergetest

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

type TaskRef interface {
	Name() string
	Queue() string
	Encode(v any) ([]byte, error)
}

func (h *Harness) pollUntil(t testing.TB, check func() bool, describe func() string) {
	t.Helper()
	deadline := time.Now().Add(awaitDeadline)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s", describe())
			return
		}
		time.Sleep(awaitPoll)
	}
}

func (h *Harness) AssertReconciled(t testing.TB, job, id string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	h.pollUntil(t,
		func() bool {
			for _, e := range h.rec.Events() {
				if rc, ok := e.(converge.RunCompleted); ok && rc.Job == job && rc.ID == id && rc.Err == nil {
					return true
				}
			}
			return false
		},
		func() string {
			events := h.rec.Events()
			return fmt.Sprintf("convergetest: AssertReconciled(%q, %q): not observed; saw %d event(s): %+v", job, id, len(events), events)
		},
	)
}

func (h *Harness) AssertParked(t testing.TB, job, id string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	h.pollUntil(t,
		func() bool {
			for _, e := range h.rec.Events() {
				if p, ok := e.(converge.IDParked); ok && p.Job == job && p.ID == id {
					return true
				}
			}
			return false
		},
		func() string {
			events := h.rec.Events()
			return fmt.Sprintf("convergetest: AssertParked(%q, %q): not observed; saw %d event(s): %+v", job, id, len(events), events)
		},
	)
}

func (h *Harness) AssertEnqueued(t testing.TB, task TaskRef, want any) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	payload, err := task.Encode(want)
	if err != nil {
		t.Fatalf("convergetest: AssertEnqueued: encode want: %v", err)
		return
	}
	queue := task.Queue()
	kind := task.Name()
	h.pollUntil(t,
		func() bool {
			for _, m := range h.MQ.Published(queue) {
				if m.Kind == kind && bytes.Equal(m.Payload, payload) {
					return true
				}
			}
			return false
		},
		func() string {
			msgs := h.MQ.Published(queue)
			return fmt.Sprintf("convergetest: AssertEnqueued(%q, queue %q): not found; saw %d message(s): %+v", kind, queue, len(msgs), msgs)
		},
	)
}
