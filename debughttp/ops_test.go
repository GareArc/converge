package debughttp_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/worker"
)

type opsStubJob struct {
	name  string
	ready chan struct{}

	mu     sync.Mutex
	poked  []string
	paused []bool
}

func newOpsStubJob(name string) *opsStubJob {
	return &opsStubJob{name: name, ready: make(chan struct{})}
}

func (s *opsStubJob) Name() string { return s.name }

func (s *opsStubJob) Run(ctx context.Context, d converge.JobDeps) error {
	close(s.ready)
	<-ctx.Done()
	return nil
}

func (s *opsStubJob) Ready() <-chan struct{} { return s.ready }

func (s *opsStubJob) Poke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poked = append(s.poked, id)
	return nil
}

func (s *opsStubJob) Quiet() bool { return true }

func (s *opsStubJob) Hint(id string) error { return nil }

func (s *opsStubJob) RunPassNow(ctx context.Context) error { return nil }

func (s *opsStubJob) SetPaused(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = append(s.paused, paused)
}

func (s *opsStubJob) Stats() converge.JobStats { return converge.JobStats{Job: s.name} }

func (s *opsStubJob) Info() converge.JobInfo { return converge.JobInfo{Job: s.name} }

func registerGhostWorker(t *testing.T, rt *converge.Runtime, job string) {
	t.Helper()
	tk := worker.NewTask[string](job, worker.TaskOpts{})
	err := worker.Handle(rt, tk, func(ctx context.Context, payload string) error { return nil }, worker.HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
}

func seedDeadLetter(t *testing.T, w *convergetest.Harness, job, id string) {
	t.Helper()
	rec := struct {
		Task           string            `json:"task"`
		Queue          string            `json:"queue"`
		MessageID      string            `json:"message_id"`
		Attempt        int               `json:"attempt"`
		Reason         string            `json:"reason"`
		Error          string            `json:"error,omitempty"`
		EnqueuedAt     time.Time         `json:"enqueued_at"`
		DeadLetteredAt time.Time         `json:"dead_lettered_at"`
		Headers        map[string]string `json:"headers,omitempty"`
		Payload        []byte            `json:"payload,omitempty"`
	}{
		Task:           job,
		Queue:          job,
		MessageID:      id,
		Attempt:        1,
		Reason:         converge.DeadLetterMaxAttempts.String(),
		Error:          "boom",
		EnqueuedAt:     w.Clock.Now(),
		DeadLetteredAt: w.Clock.Now(),
		Payload:        []byte(`"hello"`),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	key := "dt/converge/worker/" + job + "/dlq/" + id
	if err := w.KV.Set(context.Background(), key, raw, 0); err != nil {
		t.Fatal(err)
	}
}

func TestOpsPokeLocalFallbackReportsReplica(t *testing.T) {
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := newOpsStubJob("a")
	if err := hook.RegisterJob(rt, s); err != nil {
		t.Fatal(err)
	}
	wiring, err := hook.OpsDeps(rt)
	if err != nil {
		t.Fatal(err)
	}

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodPost, "/debug/jobs/a/poke?id=x")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	assertJSONContentType(t, rec)

	var body map[string]any
	decodeJSON(t, rec, &body)
	assertExactKeys(t, body, []string{"op", "job", "id", "responses"})
	if body["op"] != "poke" || body["job"] != "a" || body["id"] != "x" {
		t.Fatalf("body = %+v, want op/job/id poke/a/x", body)
	}
	responses := body["responses"].([]any)
	if len(responses) != 1 {
		t.Fatalf("responses = %+v, want exactly one", responses)
	}
	resp := responses[0].(map[string]any)
	if resp["acted"] != true {
		t.Fatalf("acted = %v, want true", resp["acted"])
	}
	if resp["replica"] != wiring.Replica {
		t.Fatalf("replica = %v, want %q", resp["replica"], wiring.Replica)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.poked) != 1 || s.poked[0] != "x" {
		t.Fatalf("poked = %v, want [x]", s.poked)
	}
}

func TestOpsRunPassLocalFallbackHasNoIDKey(t *testing.T) {
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newOpsStubJob("a")); err != nil {
		t.Fatal(err)
	}

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodPost, "/debug/jobs/a/run-pass")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	assertExactKeys(t, body, []string{"op", "job", "responses"})
	if body["op"] != "run-pass" {
		t.Fatalf("op = %v, want run-pass", body["op"])
	}
}

func TestOpsVerbUnknownJob404(t *testing.T) {
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	for _, path := range []string{
		"/debug/jobs/ghost/poke?id=x",
		"/debug/jobs/ghost/run-pass",
		"/debug/jobs/ghost/pause",
		"/debug/jobs/ghost/resume",
	} {
		rec := doRequest(h, http.MethodPost, path)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", path, rec.Code)
		}
		var body map[string]any
		decodeJSON(t, rec, &body)
		if _, ok := body["error"]; !ok {
			t.Fatalf("%s: body = %+v, want an error key", path, body)
		}
	}
}

func TestOpsDispatchErrorReturns502(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerReconcileJob(t, rt, "job")
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{Timeout: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/debug/jobs/job/poke?id=x", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	if _, ok := body["error"]; !ok {
		t.Fatalf("body = %+v, want an error key", body)
	}
}

func TestOpsTimeoutReachesDispatch(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerReconcileJob(t, rt, "job")

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{Timeout: 30 * time.Millisecond})
	start := w.Clock.Now()
	rec := doOpsRequestAsync(t, w.Clock, h, http.MethodPost, "/debug/jobs/job/poke?id=x")
	elapsed := w.Clock.Now().Sub(start)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	responses, _ := body["responses"].([]any)
	if len(responses) != 0 {
		t.Fatalf("responses = %+v, want empty (no listeners)", responses)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("elapsed = %v, want well under the 2s kernel default (OpsOpts.Timeout must reach the dispatch)", elapsed)
	}
}

func TestOpsPauseStopsConsumingThenResumeRestores(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("job", worker.TaskOpts{})
	var mu sync.Mutex
	var handled []string
	err = worker.Handle(rt, tk, func(ctx context.Context, payload string) error {
		mu.Lock()
		handled = append(handled, payload)
		mu.Unlock()
		return nil
	}, worker.HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)

	p, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "one", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 1
	})

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{Timeout: 200 * time.Millisecond})
	pauseRec := doOpsRequestAsync(t, w.Clock, h, http.MethodPost, "/debug/jobs/job/pause")
	if pauseRec.Code != http.StatusOK {
		t.Fatalf("pause status = %d, body = %s", pauseRec.Code, pauseRec.Body)
	}
	var pauseBody map[string]any
	decodeJSON(t, pauseRec, &pauseBody)
	responses := pauseBody["responses"].([]any)
	if len(responses) != 1 || responses[0].(map[string]any)["acted"] != true {
		t.Fatalf("pause responses = %+v, want one acted response", responses)
	}

	if err := tk.Enqueue(context.Background(), p, "two", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 1
	})

	resumeRec := doOpsRequestAsync(t, w.Clock, h, http.MethodPost, "/debug/jobs/job/resume")
	if resumeRec.Code != http.StatusOK {
		t.Fatalf("resume status = %d, body = %s", resumeRec.Code, resumeRec.Body)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 2
	})
}

func TestOpsDLQListRealDeadLetterHidesThenShowsPayload(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("job", worker.TaskOpts{})
	err = worker.Handle(rt, tk, func(ctx context.Context, payload string) error {
		return errors.New("boom")
	}, worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)

	p, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	hide := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	var hideBody map[string]any
	convergetest.Await(t, func() bool {
		rec := doRequest(hide, http.MethodGet, "/debug/jobs/job/dlq")
		if rec.Code != http.StatusOK {
			return false
		}
		var b map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
			return false
		}
		dls, _ := b["dead_letters"].([]any)
		if len(dls) != 1 {
			return false
		}
		hideBody = b
		return true
	})
	assertJSONContentType(t, doRequest(hide, http.MethodGet, "/debug/jobs/job/dlq"))
	entry := hideBody["dead_letters"].([]any)[0].(map[string]any)
	assertExactKeys(t, entry, []string{
		"task", "queue", "message_id", "attempt", "reason", "error",
		"enqueued_at", "dead_lettered_at", "headers",
	})
	if entry["reason"] != converge.DeadLetterMaxAttempts.String() {
		t.Fatalf("reason = %v, want %q", entry["reason"], converge.DeadLetterMaxAttempts.String())
	}

	show := debughttp.OpsHandler(rt, debughttp.OpsOpts{ShowPayloads: true})
	showRec := doRequest(show, http.MethodGet, "/debug/jobs/job/dlq")
	var showBody map[string]any
	decodeJSON(t, showRec, &showBody)
	entry2 := showBody["dead_letters"].([]any)[0].(map[string]any)
	payloadB64, ok := entry2["payload"].(string)
	if !ok {
		t.Fatalf("body = %+v, want a payload key when ShowPayloads is set", entry2)
	}
	raw, err := base64.StdEncoding.DecodeString(payloadB64)
	if err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload != "hello" {
		t.Fatalf("payload = %q, want %q", payload, "hello")
	}
}

func TestOpsDLQRequeueAbsent404(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerGhostWorker(t, rt, "job")
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodPost, "/debug/jobs/job/dlq/nope/requeue")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
}

func TestOpsDLQRequeuePresentSucceedsAndRemovesRecord(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerGhostWorker(t, rt, "job")
	seedDeadLetter(t, w, "job", "id-1")

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodPost, "/debug/jobs/job/dlq/id-1/requeue")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	assertExactKeys(t, body, []string{"requeued"})
	if body["requeued"] != "id-1" {
		t.Fatalf("requeued = %v, want id-1", body["requeued"])
	}

	if _, ok, err := w.KV.Get(context.Background(), "dt/converge/worker/job/dlq/id-1"); err != nil || ok {
		t.Fatalf("record still present after requeue: ok=%v err=%v", ok, err)
	}
}

func TestOpsDLQPurgeOneAbsentIs200(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerGhostWorker(t, rt, "job")
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodDelete, "/debug/jobs/job/dlq/nope")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	if body["purged"] != "nope" {
		t.Fatalf("purged = %v, want nope", body["purged"])
	}
}

func TestOpsDLQPurgeAllReturnsCount(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerGhostWorker(t, rt, "job")
	seedDeadLetter(t, w, "job", "id-1")
	seedDeadLetter(t, w, "job", "id-2")

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodDelete, "/debug/jobs/job/dlq")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	if body["purged_count"] != float64(2) {
		t.Fatalf("purged_count = %v, want 2", body["purged_count"])
	}
}

func TestOpsDLQNonWorkerJob404NamesSurface(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerReconcileJob(t, rt, "recon")

	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodGet, "/debug/jobs/recon/dlq")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	msg, _ := body["error"].(string)
	if !strings.Contains(msg, "reconcile") {
		t.Fatalf("error = %q, want it to name the actual surface", msg)
	}
}

func TestOpsDLQUnknownJob404(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	for _, path := range []string{
		"/debug/jobs/ghost/dlq",
	} {
		rec := doRequest(h, http.MethodGet, path)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", path, rec.Code)
		}
	}
	rec := doRequest(h, http.MethodPost, "/debug/jobs/ghost/dlq/x/requeue")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("requeue: status = %d, want 404", rec.Code)
	}
	rec = doRequest(h, http.MethodDelete, "/debug/jobs/ghost/dlq/x")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("purge one: status = %d, want 404", rec.Code)
	}
	rec = doRequest(h, http.MethodDelete, "/debug/jobs/ghost/dlq")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("purge all: status = %d, want 404", rec.Code)
	}
}

func TestOpsWrongMethod405(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerReconcileJob(t, rt, "job")
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodGet, "/debug/jobs/job/poke")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestOpsUnknownPath404(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt, err := converge.New(w.Options())
	if err != nil {
		t.Fatal(err)
	}
	registerReconcileJob(t, rt, "job")
	h := debughttp.OpsHandler(rt, debughttp.OpsOpts{})
	rec := doRequest(h, http.MethodGet, "/debug/jobs/job/nonexistent")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
