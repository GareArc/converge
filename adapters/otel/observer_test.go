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
