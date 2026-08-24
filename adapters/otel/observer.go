package convotel

import (
	"context"

	"github.com/GareArc/converge"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	attrJob      = "converge.job"
	attrSurface  = "converge.surface"
	attrStatus   = "converge.status"
	attrQueue    = "converge.queue"
	attrReason   = "converge.reason"
	attrKind     = "converge.kind"
	attrAcquired = "converge.acquired"
)

const (
	statusOK    = "ok"
	statusError = "error"
)

type observer struct {
	runDuration metric.Float64Histogram
	parked      metric.Int64Counter
	deadLetters metric.Int64Counter
	discarded   metric.Int64Counter
	leaseMoves  metric.Int64Counter
	anomalies   metric.Int64Counter
}

func NewObserver(meter metric.Meter) (converge.Observer, error) {
	o := &observer{}
	h, err := meter.Float64Histogram("converge.run.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of reconcile and worker runs."))
	if err != nil {
		return nil, err
	}
	o.runDuration = h
	for _, c := range []struct {
		target     *metric.Int64Counter
		name, desc string
	}{
		{&o.parked, "converge.parked", "Reconcile IDs parked after repeated failure."},
		{&o.deadLetters, "converge.dead_letters", "Worker messages moved to the dead-letter store."},
		{&o.discarded, "converge.discarded", "Work items dropped without running."},
		{&o.leaseMoves, "converge.lease.transitions", "Job lease acquisitions and losses."},
		{&o.anomalies, "converge.anomalies", "Misconfiguration and guard-rail signals that should stay at zero."},
	} {
		ctr, err := meter.Int64Counter(c.name, metric.WithDescription(c.desc))
		if err != nil {
			return nil, err
		}
		*c.target = ctr
	}
	return o, nil
}

func (o *observer) Observe(e converge.Event) {
	ctx := context.Background()
	switch v := e.(type) {
	case converge.RunCompleted:
		o.runDuration.Record(ctx, v.Duration.Seconds(), metric.WithAttributes(
			attribute.String(attrJob, v.Job),
			attribute.String(attrSurface, v.Surface.String()),
			attribute.String(attrStatus, runStatus(v.Err)),
		))
	}
}

func runStatus(err error) string {
	if err != nil {
		return statusError
	}
	return statusOK
}
