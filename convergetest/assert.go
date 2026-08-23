package convergetest

import (
	"bytes"
	"testing"

	"github.com/GareArc/converge"
)

type TaskRef interface {
	Name() string
	Queue() string
	Encode(v any) ([]byte, error)
}

func (h *Harness) AssertReconciled(t testing.TB, job, id string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	pollUntil(t, pollSpec{
		deadline: awaitDeadline,
		step:     pollStep,
		fail: func() {
			t.Helper()
			events := h.rec.Events()
			t.Fatalf("convergetest: AssertReconciled(%q, %q): not observed; saw %d event(s): %+v", job, id, len(events), events)
		},
	}, func() bool {
		for _, e := range h.rec.Events() {
			if rc, ok := e.(converge.RunCompleted); ok && rc.Job == job && rc.ID == id && rc.Err == nil {
				return true
			}
		}
		return false
	})
}

func (h *Harness) AssertParked(t testing.TB, job, id string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	pollUntil(t, pollSpec{
		deadline: awaitDeadline,
		step:     pollStep,
		fail: func() {
			t.Helper()
			events := h.rec.Events()
			t.Fatalf("convergetest: AssertParked(%q, %q): not observed; saw %d event(s): %+v", job, id, len(events), events)
		},
	}, func() bool {
		for _, e := range h.rec.Events() {
			if p, ok := e.(converge.IDParked); ok && p.Job == job && p.ID == id {
				return true
			}
		}
		return false
	})
}

func (h *Harness) AssertEnqueued(t testing.TB, task TaskRef, want any) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	if h.MQ == nil {
		t.Fatalf("convergetest: AssertEnqueued: this harness was built with a custom MQ constructor, so the recording MQ that AssertEnqueued needs does not exist; express the condition through Await instead")
		return
	}
	payload, err := task.Encode(want)
	if err != nil {
		t.Fatalf("convergetest: AssertEnqueued: encode want: %v", err)
		return
	}
	queue := task.Queue()
	kind := task.Name()
	pollUntil(t, pollSpec{
		deadline: awaitDeadline,
		step:     pollStep,
		fail: func() {
			t.Helper()
			msgs := h.MQ.Published(queue)
			t.Fatalf("convergetest: AssertEnqueued(%q, queue %q): not found; saw %d message(s): %+v", kind, queue, len(msgs), msgs)
		},
	}, func() bool {
		for _, m := range h.MQ.Published(queue) {
			if m.Kind == kind && bytes.Equal(m.Payload, payload) {
				return true
			}
		}
		return false
	})
}
