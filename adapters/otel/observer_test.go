package convotel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
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
	return v.Emit()
}

func TestRunCompletedRecordsDurationWithStatus(t *testing.T) {
	obs, r := newTestObserver(t)

	obs.Observe(converge.RunCompleted{
		Job:      "sync",
		Surface:  converge.SurfaceReconcile,
		ID:       "ws-1",
		Duration: 250 * time.Millisecond,
	})
	obs.Observe(converge.RunCompleted{
		Job:      "sync",
		Surface:  converge.SurfaceReconcile,
		ID:       "ws-2",
		Duration: 750 * time.Millisecond,
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
	bad, hasErr := byStatus["error"]
	if !hasErr {
		t.Fatal("no data point with converge.status=error")
	}
	if bad.Count != 1 || bad.Sum != 0.75 {
		t.Fatalf("error point Count=%d Sum=%v, want 1 and 0.75", bad.Count, bad.Sum)
	}
	if got := attrValue(t, ok1.Attributes, "converge.surface"); got != "reconcile" {
		t.Fatalf("converge.surface = %q, want reconcile", got)
	}
	if got := attrValue(t, ok1.Attributes, "converge.job"); got != "sync" {
		t.Fatalf("converge.job = %q, want sync", got)
	}
}

func TestRunCompletedDoesNotAttributeID(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.RunCompleted{Job: "sync", Surface: converge.SurfaceWorker, ID: "tenant-42", Duration: time.Second})

	h := findMetric(t, collect(t, r), "converge.run.duration").Data.(metricdata.Histogram[float64])
	for _, dp := range h.DataPoints {
		if _, ok := dp.Attributes.Value(attribute.Key("converge.id")); ok {
			t.Fatal("per-ID attribute exported; unbounded cardinality")
		}
		if v, ok := dp.Attributes.Value(attribute.Key("converge.job")); ok && v.Emit() == "tenant-42" {
			t.Fatal("ID leaked into the job attribute")
		}
	}
}

func sumPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	s, ok := findMetric(t, rm, name).Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is not an int64 sum", name)
	}
	return s.DataPoints
}

func TestDeadLetterCountsByReasonAndQueue(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.MessageDeadLettered{
		Job:       "email",
		Queue:     "email-q",
		MessageID: "m-1",
		Attempt:   5,
		Reason:    converge.DeadLetterMaxAttempts,
	})

	pts := sumPoints(t, collect(t, r), "converge.dead_letters")
	if len(pts) != 1 || pts[0].Value != 1 {
		t.Fatalf("points = %+v, want a single point of 1", pts)
	}
	if got := attrValue(t, pts[0].Attributes, "converge.reason"); got != "max-attempts" {
		t.Fatalf("converge.reason = %q, want max-attempts", got)
	}
	if got := attrValue(t, pts[0].Attributes, "converge.queue"); got != "email-q" {
		t.Fatalf("converge.queue = %q, want email-q", got)
	}
	if _, ok := pts[0].Attributes.Value(attribute.Key("converge.message_id")); ok {
		t.Fatal("message ID exported as an attribute; unbounded cardinality")
	}
}

func TestDiscardsShareOneMetricSplitBySurface(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.WakeDiscarded{Job: "sync", ID: "ws-1", Reason: converge.DiscardParked})
	obs.Observe(converge.MessageDiscarded{Job: "email", Queue: "email-q", MessageID: "m-1", Reason: "tenant 42 is gone"})

	pts := sumPoints(t, collect(t, r), "converge.discarded")
	if len(pts) != 2 {
		t.Fatalf("points = %d, want 2 (one per surface)", len(pts))
	}
	bySurface := map[string]metricdata.DataPoint[int64]{}
	for _, dp := range pts {
		bySurface[attrValue(t, dp.Attributes, "converge.surface")] = dp
	}
	rec, ok := bySurface["reconcile"]
	if !ok {
		t.Fatal("no reconcile data point")
	}
	if got := attrValue(t, rec.Attributes, "converge.reason"); got != "parked" {
		t.Fatalf("reconcile converge.reason = %q, want parked", got)
	}
	wrk, ok := bySurface["worker"]
	if !ok {
		t.Fatal("no worker data point")
	}
	if _, ok := wrk.Attributes.Value(attribute.Key("converge.reason")); ok {
		t.Fatal("worker discard exported its free-form reason; unbounded cardinality")
	}
	if got := attrValue(t, wrk.Attributes, "converge.queue"); got != "email-q" {
		t.Fatalf("worker converge.queue = %q, want email-q", got)
	}
}

func TestParkedAndLeaseTransitionsCount(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.IDParked{Job: "sync", ID: "ws-1", Failures: 3})
	obs.Observe(converge.LeaseTransition{Job: "sync", Acquired: true})
	obs.Observe(converge.LeaseTransition{Job: "sync", Acquired: false})

	rm := collect(t, r)
	if pts := sumPoints(t, rm, "converge.parked"); len(pts) != 1 || pts[0].Value != 1 {
		t.Fatalf("converge.parked points = %+v, want a single point of 1", pts)
	}
	pts := sumPoints(t, rm, "converge.lease.transitions")
	if len(pts) != 2 {
		t.Fatalf("lease points = %d, want 2 (acquired true and false)", len(pts))
	}
	for _, dp := range pts {
		if attrValue(t, dp.Attributes, "converge.acquired") == "" {
			t.Fatal("converge.acquired missing")
		}
	}
}

func TestEachAnomalyEventRecordsItsOwnKind(t *testing.T) {
	for _, tc := range []struct {
		kind  string
		event converge.Event
	}{
		{"version-zero", converge.VersionZero{Job: "sync", ID: "ws-1"}},
		{"wrong-surface", converge.WrongSurfaceSignal{Job: "sync", ID: "ws-1", Surface: converge.SurfaceWorker}},
		{"backoff-fallback", converge.BackoffFallback{Job: "sync", ID: "ws-1", Consecutive: 11}},
		{"pass-overrun", converge.PassOverrun{Job: "sync", Due: time.Unix(0, 0)}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			obs, r := newTestObserver(t)
			obs.Observe(tc.event)

			pts := sumPoints(t, collect(t, r), "converge.anomalies")
			if len(pts) != 1 {
				t.Fatalf("points = %d, want 1", len(pts))
			}
			if got := attrValue(t, pts[0].Attributes, "converge.kind"); got != tc.kind {
				t.Fatalf("converge.kind = %q, want %q", got, tc.kind)
			}
		})
	}
}

func TestQueueDepthIsDeliberatelyUnmapped(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.QueueDepth{Job: "email", Queue: "email-q", Depth: 17})

	rm := collect(t, r)
	for _, sm := range rm.ScopeMetrics {
		if len(sm.Metrics) != 0 {
			t.Fatalf("QueueDepth produced metrics %+v; convotel exports no gauges (M1-R2)", sm.Metrics)
		}
	}
}

func TestUnrecognisedReasonRendersAsUnknown(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.WakeDiscarded{Job: "sync", ID: "ws-1", Reason: converge.WakeDiscardReason{}})

	pts := sumPoints(t, collect(t, r), "converge.discarded")
	if len(pts) != 1 {
		t.Fatalf("points = %d, want 1", len(pts))
	}
	if got := attrValue(t, pts[0].Attributes, "converge.reason"); got != "unknown" {
		t.Fatalf("converge.reason = %q, want unknown for a zero reason", got)
	}
}

func TestRunDurationIsRecordedInSeconds(t *testing.T) {
	obs, r := newTestObserver(t)
	obs.Observe(converge.RunCompleted{Job: "sync", Surface: converge.SurfaceReconcile, Duration: 250 * time.Millisecond})

	if got := findMetric(t, collect(t, r), "converge.run.duration").Unit; got != "s" {
		t.Fatalf("unit = %q, want s", got)
	}
}
