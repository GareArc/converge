package convotel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestObserver(t *testing.T) (converge.Observer, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	obs, err := convotel.NewObserver(mp.Meter("converge"))
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}
	return obs, r
}

func collect(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return rm
}

func findMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not collected", name)
	return metricdata.Metrics{}
}

func attrValue(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %q missing from %v", key, set.Encoded(attribute.DefaultEncoder()))
	}
	return v.String()
}

func sumPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	s, ok := findMetric(t, rm, name).Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is not an int64 sum", name)
	}
	return s.DataPoints
}

func TestRunCompletedRecordsDurationWithStatusAndOutcome(t *testing.T) {
	obs, r := newTestObserver(t)

	obs.Observe(converge.RunCompleted{
		Job:      "sync",
		ID:       "ws-1",
		Duration: 250 * time.Millisecond,
		Outcome:  converge.Succeeded,
	})
	obs.Observe(converge.RunCompleted{
		Job:      "sync",
		ID:       "ws-2",
		Duration: 750 * time.Millisecond,
		Outcome:  converge.Retrying,
		Err:      errors.New("boom"),
	})

	h, ok := findMetric(t, collect(t, r), "converge.run.duration").Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("converge.run.duration is not a float64 histogram")
	}
	if len(h.DataPoints) != 2 {
		t.Fatalf("data points = %d, want 2 (one per status)", len(h.DataPoints))
	}
	byStatus := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, dp := range h.DataPoints {
		byStatus[attrValue(t, dp.Attributes, "converge.status")] = dp
	}
	ok1, hasOK := byStatus["ok"]
	if !hasOK {
		t.Fatal("no data point with converge.status=ok")
	}
	if ok1.Count != 1 || ok1.Sum != 0.25 {
		t.Fatalf("ok point Count=%d Sum=%v, want 1 and 0.25", ok1.Count, ok1.Sum)
	}
	if got := attrValue(t, ok1.Attributes, "converge.outcome"); got != "succeeded" {
		t.Fatalf("converge.outcome = %q, want succeeded", got)
	}
	bad, hasErr := byStatus["error"]
	if !hasErr {
		t.Fatal("no data point with converge.status=error")
	}
	if bad.Count != 1 || bad.Sum != 0.75 {
		t.Fatalf("error point Count=%d Sum=%v, want 1 and 0.75", bad.Count, bad.Sum)
	}
	if got := attrValue(t, bad.Attributes, "converge.outcome"); got != "retrying" {
		t.Fatalf("converge.outcome = %q, want retrying", got)
	}
	if got := attrValue(t, ok1.Attributes, "converge.job"); got != "sync" {
		t.Fatalf("converge.job = %q, want sync", got)
	}
}

func TestRunCompletedDoesNotAttributeID(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.RunCompleted{Job: "sync", ID: "tenant-42", Duration: time.Second, Outcome: converge.Succeeded})

	h := findMetric(t, collect(t, r), "converge.run.duration").Data.(metricdata.Histogram[float64])
	for _, dp := range h.DataPoints {
		if _, ok := dp.Attributes.Value(attribute.Key("converge.id")); ok {
			t.Fatal("per-ID attribute exported; unbounded cardinality")
		}
		if v, ok := dp.Attributes.Value(attribute.Key("converge.job")); ok && v.String() == "tenant-42" {
			t.Fatal("ID leaked into the job attribute")
		}
	}
}

func TestShelvedOutcomeCounts(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.RunCompleted{Job: "email", ID: "m-1", Outcome: converge.Shelved, Err: errors.New("max attempts")})

	pts := sumPoints(t, collect(t, r), "converge.shelved")
	if len(pts) != 1 || pts[0].Value != 1 {
		t.Fatalf("points = %+v, want a single point of 1", pts)
	}
	if got := attrValue(t, pts[0].Attributes, "converge.job"); got != "email" {
		t.Fatalf("converge.job = %q, want email", got)
	}
	if _, ok := pts[0].Attributes.Value(attribute.Key("converge.id")); ok {
		t.Fatal("per-ID attribute exported; unbounded cardinality")
	}
}

func TestDiscardedOutcomeCounts(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.RunCompleted{Job: "sync", ID: "ws-1", Outcome: converge.Discarded})
	obs.Observe(converge.RunCompleted{Job: "email", ID: "m-1", Outcome: converge.Discarded})

	pts := sumPoints(t, collect(t, r), "converge.discarded")
	if len(pts) != 2 {
		t.Fatalf("points = %d, want 2 (one per job)", len(pts))
	}
}

func TestLeaseChangedCounts(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.LeaseChanged{Job: "sync", Held: true})
	obs.Observe(converge.LeaseChanged{Job: "sync", Held: false})

	pts := sumPoints(t, collect(t, r), "converge.lease.transitions")
	if len(pts) != 2 {
		t.Fatalf("lease points = %d, want 2 (held true and false)", len(pts))
	}
	for _, dp := range pts {
		if attrValue(t, dp.Attributes, "converge.held") == "" {
			t.Fatal("converge.held missing")
		}
	}
}

func TestNotificationDroppedCounts(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.NotificationDropped{Job: "sync", ID: "ws-1", Err: converge.ErrInboxOverflow})

	pts := sumPoints(t, collect(t, r), "converge.notifications.dropped")
	if len(pts) != 1 || pts[0].Value != 1 {
		t.Fatalf("points = %+v, want a single point of 1", pts)
	}
	if got := attrValue(t, pts[0].Attributes, "converge.job"); got != "sync" {
		t.Fatalf("converge.job = %q, want sync", got)
	}
	if _, ok := pts[0].Attributes.Value(attribute.Key("converge.id")); ok {
		t.Fatal("per-ID attribute exported; unbounded cardinality")
	}
}

func TestScheduleOverrunCounts(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.ScheduleOverrun{Job: "sync", Due: time.Unix(0, 0), Late: time.Minute})

	pts := sumPoints(t, collect(t, r), "converge.schedule.overruns")
	if len(pts) != 1 || pts[0].Value != 1 {
		t.Fatalf("points = %+v, want a single point of 1", pts)
	}
	if got := attrValue(t, pts[0].Attributes, "converge.job"); got != "sync" {
		t.Fatalf("converge.job = %q, want sync", got)
	}
}

func TestJobDestroyedCounts(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.JobDestroyed{Job: "sync", Cause: converge.Deadline(time.Unix(0, 0))})

	pts := sumPoints(t, collect(t, r), "converge.destroyed")
	if len(pts) != 1 || pts[0].Value != 1 {
		t.Fatalf("points = %+v, want a single point of 1", pts)
	}
	if got := attrValue(t, pts[0].Attributes, "converge.job"); got != "sync" {
		t.Fatalf("converge.job = %q, want sync", got)
	}
}

type fakeEvent struct{ converge.RunCompleted }

func TestObserverIgnoresUnrecognizedEventType(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(fakeEvent{})

	rm := collect(t, r)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "converge.run.duration" {
				if h, ok := m.Data.(metricdata.Histogram[float64]); ok && len(h.DataPoints) != 0 {
					t.Fatalf("unrecognized event produced data on %q", m.Name)
				}
				continue
			}
			if s, ok := m.Data.(metricdata.Sum[int64]); ok && len(s.DataPoints) != 0 {
				t.Fatalf("unrecognized event produced data on %q", m.Name)
			}
		}
	}
}

func TestRunDurationIsRecordedInSeconds(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.RunCompleted{Job: "sync", Duration: 250 * time.Millisecond, Outcome: converge.Succeeded})

	if got := findMetric(t, collect(t, r), "converge.run.duration").Unit; got != "s" {
		t.Fatalf("unit = %q, want s", got)
	}
}

func TestGauges(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	if err := reconcile.Periodic(rt, "job", reconcile.Every(time.Hour), func(context.Context) error { return nil }, reconcile.PeriodicOpts{}); err != nil {
		t.Fatalf("Periodic: %v", err)
	}
	h.Runtime(t)

	convergetest.Await(t, func() bool {
		for _, js := range rt.Stats() {
			if js.Job == "job" && js.LeaseHeld {
				return true
			}
		}
		return false
	})

	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	if err := convotel.RegisterGauges(mp.Meter("converge"), rt); err != nil {
		t.Fatalf("RegisterGauges: %v", err)
	}

	g, ok := findMetric(t, collect(t, r), "converge.lease_held").Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatal("converge.lease_held is not an int64 gauge")
	}
	var found bool
	for _, dp := range g.DataPoints {
		if attrValue(t, dp.Attributes, "converge.job") != "job" {
			continue
		}
		found = true
		if dp.Value != 1 {
			t.Fatalf("converge.lease_held value = %d, want 1", dp.Value)
		}
	}
	if !found {
		t.Fatal("converge.lease_held has no data point for job \"job\"")
	}
}

func TestBacklogGaugeSkipsUnknownBacklog(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	if err := reconcile.Periodic(rt, "job", reconcile.Every(time.Hour), func(context.Context) error { return nil }, reconcile.PeriodicOpts{}); err != nil {
		t.Fatalf("Periodic: %v", err)
	}
	h.Runtime(t)

	convergetest.Await(t, func() bool { return len(rt.Stats()) == 1 })

	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	if err := convotel.RegisterGauges(mp.Meter("converge"), rt); err != nil {
		t.Fatalf("RegisterGauges: %v", err)
	}

	assertNoGaugePoint(t, collect(t, r), "converge.backlog", "job",
		"converge.backlog emitted a point for a job with BacklogKnown=false; a missing series is honest, a zero is not")
}

func TestShelvedGaugeSkipsAJobWithNoShelf(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	if err := reconcile.Periodic(rt, "job", reconcile.Every(time.Hour), func(context.Context) error { return nil }, reconcile.PeriodicOpts{}); err != nil {
		t.Fatalf("Periodic: %v", err)
	}
	h.Runtime(t)

	convergetest.Await(t, func() bool { return len(rt.Stats()) == 1 })

	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	if err := convotel.RegisterGauges(mp.Meter("converge"), rt); err != nil {
		t.Fatalf("RegisterGauges: %v", err)
	}

	assertNoGaugePoint(t, collect(t, r), "converge.shelved.current", "job",
		"converge.shelved.current emitted a point for a reconcile job, which has no shelf at all")
}

func TestLeaseHeldGaugeSkipsAJobThatTakesNoLease(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	task := worker.NewTask[string]("competing", worker.TaskOpts{})
	if err := worker.Handle(rt, task, func(context.Context, string) error { return nil }, worker.HandleOpts{}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	h.Runtime(t)

	convergetest.Await(t, func() bool { return len(rt.Stats()) == 1 })
	if js := rt.Stats()[0]; js.RunMode != converge.Competing {
		t.Fatalf("run mode = %v, want Competing", js.RunMode)
	}

	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	if err := convotel.RegisterGauges(mp.Meter("converge"), rt); err != nil {
		t.Fatalf("RegisterGauges: %v", err)
	}

	assertNoGaugePoint(t, collect(t, r), "converge.lease_held", "competing",
		"converge.lease_held emitted a point for a Competing job, which never takes a lease; a constant 0 reads as losing the lease")
}

func assertNoGaugePoint(t *testing.T, rm metricdata.ResourceMetrics, metric, job, msg string) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metric {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s is not an int64 gauge", metric)
			}
			for _, dp := range g.DataPoints {
				if attrValue(t, dp.Attributes, "converge.job") == job {
					t.Fatal(msg)
				}
			}
		}
	}
}
