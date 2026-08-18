package convergetest

import (
	"bytes"
	"testing"
	"time"

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
	deadline := time.Now().Add(awaitDeadline)
	for {
		events := h.rec.Events()
		for _, e := range events {
			if rc, ok := e.(converge.RunCompleted); ok && rc.Job == job && rc.ID == id && rc.Err == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: AssertReconciled(%q, %q): not observed; saw %d event(s): %+v", job, id, len(events), events)
			return
		}
		time.Sleep(awaitPoll)
	}
}

func (h *Harness) AssertParked(t testing.TB, job, id string) {
	t.Helper()
	if !h.ensureRunning(t) {
		return
	}
	deadline := time.Now().Add(awaitDeadline)
	for {
		events := h.rec.Events()
		for _, e := range events {
			if p, ok := e.(converge.IDParked); ok && p.Job == job && p.ID == id {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: AssertParked(%q, %q): not observed; saw %d event(s): %+v", job, id, len(events), events)
			return
		}
		time.Sleep(awaitPoll)
	}
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
	deadline := time.Now().Add(awaitDeadline)
	for {
		msgs := h.MQ.Published(queue)
		for _, m := range msgs {
			if m.Kind == kind && bytes.Equal(m.Payload, payload) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: AssertEnqueued(%q, queue %q): not found; saw %d message(s): %+v", kind, queue, len(msgs), msgs)
			return
		}
		time.Sleep(awaitPoll)
	}
}
