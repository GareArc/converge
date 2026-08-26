package debughttp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
)

var wstart = time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

func doRequest(h http.Handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	h.ServeHTTP(rec, req)
	return rec
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}
}

func assertJSONContentType(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func assertExactKeys(t *testing.T, m map[string]any, want []string) {
	t.Helper()
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	wantSorted := slices.Clone(want)
	sort.Strings(wantSorted)
	if !slices.Equal(got, wantSorted) {
		t.Fatalf("keys = %v, want %v", got, wantSorted)
	}
}

func registerReconcileJob(t *testing.T, rt *converge.Runtime, name string) {
	t.Helper()
	if err := reconcile.Periodic(rt, name, reconcile.Every(time.Hour), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

type foreignJob struct {
	name  string
	ready chan struct{}
}

func newForeignJob(name string) *foreignJob {
	return &foreignJob{name: name, ready: make(chan struct{})}
}

func (j *foreignJob) Name() string { return j.name }

func (j *foreignJob) Run(ctx context.Context, d converge.JobDeps) error {
	close(j.ready)
	<-ctx.Done()
	return nil
}

func (j *foreignJob) Ready() <-chan struct{} { return j.ready }

func (j *foreignJob) Quiet() bool { return true }

func (j *foreignJob) Notify(id string) error { return nil }

func (j *foreignJob) RunPassNow(ctx context.Context) error { return nil }

func (j *foreignJob) Stats() converge.JobStats { return converge.JobStats{Job: j.name} }

func (j *foreignJob) Info() converge.JobInfo { return converge.JobInfo{Job: j.name} }

func TestReadOnlyListMergesReconcileAndWorkerJobs(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	registerReconcileJob(t, rt, "license-refresh")

	tk := worker.NewTask[string]("send-invite", worker.TaskOpts{})
	var mu sync.Mutex
	var handled int
	err := worker.Handle(rt, tk, func(ctx context.Context, payload string) error {
		mu.Lock()
		handled++
		mu.Unlock()
		return nil
	}, worker.HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	w.Runtime(t)

	p, err := converge.NewProducer(w.MQ, converge.ProducerOpts{Namespace: "dt", Clock: w.Clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hi", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled == 1
	})

	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodGet, "/debug/jobs")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	assertJSONContentType(t, rec)

	var body struct {
		Jobs []map[string]any `json:"jobs"`
	}
	decodeJSON(t, rec, &body)
	if len(body.Jobs) != 2 {
		t.Fatalf("jobs = %d, want 2: %+v", len(body.Jobs), body.Jobs)
	}
	byName := map[string]map[string]any{}
	for _, j := range body.Jobs {
		byName[j["job"].(string)] = j
	}

	rc, ok := byName["license-refresh"]
	if !ok {
		t.Fatalf("missing license-refresh job in %+v", byName)
	}
	if rc["surface"] != "reconcile" {
		t.Fatalf("surface = %v, want reconcile", rc["surface"])
	}
	if rc["run_mode"] != "OnOneReplica" {
		t.Fatalf("run_mode = %v, want OnOneReplica", rc["run_mode"])
	}
	if rc["queue"] != "" {
		t.Fatalf("queue = %v, want empty", rc["queue"])
	}
	if rc["last_success"] == "" {
		t.Fatal("last_success = empty, want a populated RFC3339Nano timestamp (the schedule runs an initial pass on start)")
	}
	if _, err := time.Parse(time.RFC3339Nano, rc["last_success"].(string)); err != nil {
		t.Fatalf("last_success = %v, want RFC3339Nano: %v", rc["last_success"], err)
	}
	settings, ok := rc["settings"].(map[string]any)
	if !ok {
		t.Fatalf("settings = %v, want an object", rc["settings"])
	}
	if settings["schedule"] != "every 1h" {
		t.Fatalf("settings[schedule] = %v, want %q", settings["schedule"], "every 1h")
	}

	wk, ok := byName["send-invite"]
	if !ok {
		t.Fatalf("missing send-invite job in %+v", byName)
	}
	if wk["surface"] != "worker" {
		t.Fatalf("surface = %v, want worker", wk["surface"])
	}
	if wk["queue"] != "dt/converge/inbox/send-invite" {
		t.Fatalf("queue = %v, want the namespaced inbox", wk["queue"])
	}
	if wk["last_success"] == "" {
		t.Fatal("last_success = empty, want a populated RFC3339Nano timestamp after a successful run")
	}
	if _, err := time.Parse(time.RFC3339Nano, wk["last_success"].(string)); err != nil {
		t.Fatalf("last_success = %v, want RFC3339Nano: %v", wk["last_success"], err)
	}
	if wk["consecutive_fails"] != float64(0) {
		t.Fatalf("consecutive_fails = %v, want 0", wk["consecutive_fails"])
	}
}

func TestReadOnlyJobRowJSONKeysPinned(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	registerReconcileJob(t, rt, "job")

	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodGet, "/debug/jobs")
	var body map[string]any
	decodeJSON(t, rec, &body)
	jobs := body["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	row := jobs[0].(map[string]any)
	assertExactKeys(t, row, []string{
		"job", "surface", "run_mode", "queue", "settings",
		"queue_depth", "parked", "last_success", "consecutive_fails",
	})
}

func TestReadOnlySingleJobGet(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	registerReconcileJob(t, rt, "job")

	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodGet, "/debug/jobs/job")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	assertJSONContentType(t, rec)
	var single map[string]any
	decodeJSON(t, rec, &single)
	if single["job"] != "job" {
		t.Fatalf("job = %v, want %q", single["job"], "job")
	}
	if single["last_success"] != "" {
		t.Fatalf("last_success = %v, want empty string for a job that never ran (never a fabricated epoch)", single["last_success"])
	}

	list := doRequest(h, http.MethodGet, "/debug/jobs")
	var body struct {
		Jobs []map[string]any `json:"jobs"`
	}
	decodeJSON(t, list, &body)
	if len(body.Jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(body.Jobs))
	}
	got, err := json.Marshal(single)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(body.Jobs[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("single job GET = %s, want %s (matching its list row)", got, want)
	}
}

func TestReadOnlyUnknownJob404(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodGet, "/debug/jobs/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	assertJSONContentType(t, rec)
	var body map[string]any
	decodeJSON(t, rec, &body)
	if _, ok := body["error"]; !ok {
		t.Fatalf("body = %+v, want an error key", body)
	}
}

func TestReadOnlyHasNoMutatingRoutes(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	registerReconcileJob(t, rt, "job")
	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodPost, "/debug/jobs/job/poke?id=x")
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 404 or 405 (readonly must not mount verb routes)", rec.Code)
	}
}

func TestReadOnlyWrongMethod405(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	registerReconcileJob(t, rt, "job")
	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodPost, "/debug/jobs")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestReadOnlyUnknownPath404(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodGet, "/debug/jobs/a/b/c")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestGuideMountingAliasServesBareJobsPath(t *testing.T) {
	w := convergetest.NewWith(t, convergetest.Options{Namespace: "dt"})
	rt := w.Build(t)
	registerReconcileJob(t, rt, "license-refresh")

	outer := http.NewServeMux()
	outer.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
	srv := httptest.NewServer(outer)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/debug/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the guide's own %q mounting must serve the bare path)", resp.StatusCode, "/debug/jobs/")
	}
	var body struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Jobs) != 1 || body.Jobs[0]["job"] != "license-refresh" {
		t.Fatalf("jobs = %+v, want one row for license-refresh", body.Jobs)
	}
}

func TestReadOnlyListRendersUnknownSurfaceAndRunModeSafely(t *testing.T) {
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newForeignJob("mystery")); err != nil {
		t.Fatal(err)
	}
	h := debughttp.ReadOnlyHandler(rt)
	rec := doRequest(h, http.MethodGet, "/debug/jobs/mystery")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]any
	decodeJSON(t, rec, &body)
	if body["surface"] != "unknown" {
		t.Fatalf("surface = %v, want unknown for a foreign job's zero-value Surface", body["surface"])
	}
	if body["run_mode"] != "unset" {
		t.Fatalf("run_mode = %v, want unset for a foreign job's zero-value RunMode", body["run_mode"])
	}
}

func TestReadOnlyHandlerNilRuntimePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want a panic for a nil runtime")
		}
	}()
	debughttp.ReadOnlyHandler(nil)
}
