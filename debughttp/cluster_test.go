package debughttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/ctl"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
)

type clusterWorld struct {
	clock *convergetest.Clock
	mq    *inmem.MQ
	kv    *inmem.KV
	ns    string

	rtA, rtB   *converge.Runtime
	recA, recB *convergetest.Recorder

	doneA, doneB chan error
}

func newClusterWorld(t *testing.T) *clusterWorld {
	t.Helper()
	clock := convergetest.NewClock(wstart)
	mq := inmem.NewMQWithClock(clock)
	kv := inmem.NewKVWithClock(clock)
	lease := inmem.NewLeaseWithClock(clock)
	ns := "cluster"

	recA := &convergetest.Recorder{}
	rtA, err := converge.New(converge.Options{Namespace: ns, MQ: mq, Lease: lease, KV: kv, Observer: recA, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	recB := &convergetest.Recorder{}
	rtB, err := converge.New(converge.Options{Namespace: ns, MQ: mq, Lease: lease, KV: kv, Observer: recB, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}

	return &clusterWorld{
		clock: clock, mq: mq, kv: kv, ns: ns,
		rtA: rtA, rtB: rtB, recA: recA, recB: recB,
		doneA: make(chan error, 1), doneB: make(chan error, 1),
	}
}

type notifyState struct {
	mu      sync.Mutex
	fail    bool
	handled []string
}

func (s *notifyState) handle(ctx context.Context, payload string) error {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return errors.New("notify: boom")
	}
	s.mu.Lock()
	s.handled = append(s.handled, payload)
	s.mu.Unlock()
	return nil
}

func (s *notifyState) handledCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.handled)
}

func (s *notifyState) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func (w *clusterWorld) registerJobs(t *testing.T, state *notifyState, tk worker.Task[string]) {
	t.Helper()
	for _, rt := range []*converge.Runtime{w.rtA, w.rtB} {
		if err := reconcile.Register(rt, reconcile.Spec{
			Name:       "recon",
			Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error { return nil }),
			Triggers: []reconcile.Trigger{
				reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
					return []string{"r1"}, nil
				}), reconcile.Every(time.Hour)),
			},
		}); err != nil {
			t.Fatal(err)
		}
		if err := worker.Handle(rt, tk, state.handle, worker.HandleOpts{
			Retry: worker.RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute},
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func (w *clusterWorld) run(t *testing.T) {
	t.Helper()
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelA()
		cancelB()
		w.stop(t, w.doneA)
		w.stop(t, w.doneB)
	})
	go func() { w.doneA <- w.rtA.Run(ctxA) }()
	go func() { w.doneB <- w.rtB.Run(ctxB) }()
	select {
	case <-w.rtA.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("rtA never ready")
	}
	select {
	case <-w.rtB.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("rtB never ready")
	}
}

func (w *clusterWorld) stop(t *testing.T, done chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v", err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned")
		}
		w.clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func reconCompletions(rec *convergetest.Recorder) int {
	return rec.Count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Job == "recon" && rc.ID == "r1"
	})
}

func TestClusterOpsAcrossTwoReplicas(t *testing.T) {
	w := newClusterWorld(t)
	state := &notifyState{}
	tk := worker.NewTask[string]("notify", worker.TaskOpts{})
	w.registerJobs(t, state, tk)
	w.run(t)

	opsA := debughttp.OpsHandler(w.rtA, debughttp.OpsOpts{Timeout: 200 * time.Millisecond})

	convergetest.AdvanceUntil(t, w.clock, 60*time.Millisecond, func() bool {
		return reconCompletions(w.recA)+reconCompletions(w.recB) >= 1
	})
	active := w.recA
	standby := w.recB
	if reconCompletions(w.recA) == 0 {
		active, standby = w.recB, w.recA
	}
	if reconCompletions(active) == 0 || reconCompletions(standby) != 0 {
		t.Fatalf("active recon completions = %d, standby = %d; want exactly one replica to have run the schedule's initial pass",
			reconCompletions(active), reconCompletions(standby))
	}

	t.Run("pause stops both replicas then resume restores", func(t *testing.T) {
		producer, err := worker.ProducerFrom(w.rtA)
		if err != nil {
			t.Fatal(err)
		}
		if err := tk.Enqueue(context.Background(), producer, "one", worker.EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
		convergetest.Await(t, func() bool { return state.handledCount() == 1 })

		pauseRec := doOpsRequestAsync(t, w.clock, opsA, http.MethodPost, "/debug/jobs/notify/pause")
		if pauseRec.Code != http.StatusOK {
			t.Fatalf("pause status = %d, body = %s", pauseRec.Code, pauseRec.Body)
		}
		var pauseBody map[string]any
		decodeJSON(t, pauseRec, &pauseBody)
		responses := pauseBody["responses"].([]any)
		if len(responses) != 2 {
			t.Fatalf("pause responses = %+v, want 2 (one per replica)", responses)
		}
		for _, r := range responses {
			if r.(map[string]any)["acted"] != true {
				t.Fatalf("pause responses = %+v, want every replica acted", responses)
			}
		}

		raw, ok, err := w.kv.Get(context.Background(), ctl.PausedKey(w.ns, "notify"))
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(raw) != "1" {
			t.Fatalf("paused flag = (%q, %v), want (1, true)", raw, ok)
		}

		if err := tk.Enqueue(context.Background(), producer, "two", worker.EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}
		convergetest.AssertStable(t, func() bool { return state.handledCount() == 1 })

		resumeRec := doOpsRequestAsync(t, w.clock, opsA, http.MethodPost, "/debug/jobs/notify/resume")
		if resumeRec.Code != http.StatusOK {
			t.Fatalf("resume status = %d, body = %s", resumeRec.Code, resumeRec.Body)
		}
		if _, ok, err := w.kv.Get(context.Background(), ctl.PausedKey(w.ns, "notify")); err != nil || ok {
			t.Fatalf("paused flag ok=%v err=%v, want deleted after resume", ok, err)
		}
		convergetest.Await(t, func() bool { return state.handledCount() == 2 })
	})

	t.Run("poke only the leaseholder's dispatch loop acts on", func(t *testing.T) {
		beforeActive := reconCompletions(active)
		beforeStandby := reconCompletions(standby)

		pokeRec := doOpsRequestAsync(t, w.clock, opsA, http.MethodPost, "/debug/jobs/recon/poke?id=r1")
		if pokeRec.Code != http.StatusOK {
			t.Fatalf("poke status = %d, body = %s", pokeRec.Code, pokeRec.Body)
		}
		var pokeBody map[string]any
		decodeJSON(t, pokeRec, &pokeBody)
		responses := pokeBody["responses"].([]any)
		if len(responses) == 0 {
			t.Fatalf("poke responses = %+v, want at least one", responses)
		}
		var sawActed bool
		for _, r := range responses {
			if r.(map[string]any)["acted"] == true {
				sawActed = true
			}
		}
		if !sawActed {
			t.Fatalf("poke responses = %+v, want at least one acted=true", responses)
		}

		convergetest.Await(t, func() bool { return reconCompletions(active) > beforeActive })
		convergetest.AssertStable(t, func() bool { return reconCompletions(standby) == beforeStandby })
	})

	t.Run("run-pass acts on exactly the active replica", func(t *testing.T) {
		runPassRec := doOpsRequestAsync(t, w.clock, opsA, http.MethodPost, "/debug/jobs/recon/run-pass")
		if runPassRec.Code != http.StatusOK {
			t.Fatalf("run-pass status = %d, body = %s", runPassRec.Code, runPassRec.Body)
		}
		var body map[string]any
		decodeJSON(t, runPassRec, &body)
		responses := body["responses"].([]any)
		if len(responses) == 0 || len(responses) > 2 {
			t.Fatalf("run-pass responses = %+v, want 1 or 2", responses)
		}
		actedCount := 0
		for _, r := range responses {
			if r.(map[string]any)["acted"] == true {
				actedCount++
			}
		}
		if actedCount != 1 {
			t.Fatalf("run-pass responses = %+v, want exactly one acted=true", responses)
		}
	})

	t.Run("DLQ requeue redelivers and completes", func(t *testing.T) {
		state.setFail(true)
		producer, err := worker.ProducerFrom(w.rtA)
		if err != nil {
			t.Fatal(err)
		}
		if err := tk.Enqueue(context.Background(), producer, "will-fail", worker.EnqueueOpts{}); err != nil {
			t.Fatal(err)
		}

		var dlqID string
		convergetest.Await(t, func() bool {
			rec := doRequest(opsA, http.MethodGet, "/debug/jobs/notify/dlq")
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
			entry := dls[0].(map[string]any)
			id, _ := entry["message_id"].(string)
			if id == "" {
				return false
			}
			dlqID = id
			return true
		})

		state.setFail(false)
		before := state.handledCount()

		requeueRec := doRequest(opsA, http.MethodPost, "/debug/jobs/notify/dlq/"+dlqID+"/requeue")
		if requeueRec.Code != http.StatusOK {
			t.Fatalf("requeue status = %d, body = %s", requeueRec.Code, requeueRec.Body)
		}

		dlq, err := worker.DLQFrom(w.rtA, "notify")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dlq.Get(context.Background(), dlqID); !errors.Is(err, worker.ErrDeadLetterNotFound) {
			t.Fatalf("dlq.Get after requeue = %v, want %v (record gone immediately)", err, worker.ErrDeadLetterNotFound)
		}

		convergetest.Await(t, func() bool { return state.handledCount() > before })

		listRec := doRequest(opsA, http.MethodGet, "/debug/jobs/notify/dlq")
		var listBody map[string]any
		decodeJSON(t, listRec, &listBody)
		if dls, _ := listBody["dead_letters"].([]any); len(dls) != 0 {
			t.Fatalf("dead_letters = %+v, want empty after requeue completes", dls)
		}
	})
}
