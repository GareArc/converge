package convotel

import (
	"context"

	"github.com/GareArc/converge"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	attrJob     = "converge.job"
	attrStatus  = "converge.status"
	attrOutcome = "converge.outcome"
	attrHeld    = "converge.held"
)

const (
	statusOK    = "ok"
	statusError = "error"
)

type observer struct {
	runDuration      metric.Float64Histogram
	shelved          metric.Int64Counter
	discarded        metric.Int64Counter
	leaseMoves       metric.Int64Counter
	notifyDropped    metric.Int64Counter
	notifySkipped    metric.Int64Counter
	scheduleOverruns metric.Int64Counter
	destroyed        metric.Int64Counter
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
		{&o.shelved, "converge.shelved", "Runs whose outcome was Shelved."},
		{&o.discarded, "converge.discarded", "Runs whose outcome was Discarded."},
		{&o.leaseMoves, "converge.lease.transitions", "Job lease acquisitions and losses."},
		{&o.notifyDropped, "converge.notifications.dropped", "Notifications dropped before reaching a job."},
		{&o.notifySkipped, "converge.notifications.skipped", "Notifications an ID function declined as not for this job."},
		{&o.scheduleOverruns, "converge.schedule.overruns", "Scheduled passes that started later than due."},
		{&o.destroyed, "converge.destroyed", "Jobs that reached the Destroyed state."},
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
			attribute.String(attrStatus, runStatus(v.Err)),
			attribute.String(attrOutcome, v.Outcome.String()),
		))
		switch v.Outcome {
		case converge.Shelved:
			o.shelved.Add(ctx, 1, metric.WithAttributes(attribute.String(attrJob, v.Job)))
		case converge.Discarded:
			o.discarded.Add(ctx, 1, metric.WithAttributes(attribute.String(attrJob, v.Job)))
		}
	case converge.LeaseChanged:
		o.leaseMoves.Add(ctx, 1, metric.WithAttributes(
			attribute.String(attrJob, v.Job),
			attribute.Bool(attrHeld, v.Held),
		))
	case converge.NotificationDropped:
		o.notifyDropped.Add(ctx, 1, metric.WithAttributes(attribute.String(attrJob, v.Job)))
	case converge.NotificationSkipped:
		o.notifySkipped.Add(ctx, 1, metric.WithAttributes(attribute.String(attrJob, v.Job)))
	case converge.ScheduleOverrun:
		o.scheduleOverruns.Add(ctx, 1, metric.WithAttributes(attribute.String(attrJob, v.Job)))
	case converge.JobDestroyed:
		o.destroyed.Add(ctx, 1, metric.WithAttributes(attribute.String(attrJob, v.Job)))
	}
}

func runStatus(err error) string {
	if err != nil {
		return statusError
	}
	return statusOK
}

func RegisterGauges(meter metric.Meter, rt *converge.Runtime) error {
	backlog, err := meter.Int64ObservableGauge("converge.backlog",
		metric.WithDescription("Cluster-wide inbox depth as of this replica's last periodic poll (stale by up to one lease heartbeat); omitted when not known."))
	if err != nil {
		return err
	}
	failing, err := meter.Int64ObservableGauge("converge.failing",
		metric.WithDescription("IDs or messages on this replica currently serving out backoff or awaiting retry."))
	if err != nil {
		return err
	}
	shelvedCurrent, err := meter.Int64ObservableGauge("converge.shelved.current",
		metric.WithDescription("Messages currently on the job's shelf as of this replica's last periodic poll; omitted when not known."))
	if err != nil {
		return err
	}
	leaseHeld, err := meter.Int64ObservableGauge("converge.lease_held",
		metric.WithDescription("1 if this replica currently holds the job's lease, 0 otherwise; omitted for jobs whose run mode takes no lease."))
	if err != nil {
		return err
	}
	inFlight, err := meter.Int64ObservableGauge("converge.in_flight",
		metric.WithDescription("IDs or deliveries currently mid-run on this replica."))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, js := range rt.Stats() {
			attrs := metric.WithAttributes(attribute.String(attrJob, js.Job))
			if js.BacklogKnown {
				o.ObserveInt64(backlog, int64(js.Backlog), attrs)
			}
			o.ObserveInt64(failing, int64(js.Failing), attrs)
			if js.ShelvedKnown {
				o.ObserveInt64(shelvedCurrent, int64(js.Shelved), attrs)
			}
			if js.RunMode == converge.OnOneReplica {
				o.ObserveInt64(leaseHeld, boolToInt64(js.LeaseHeld), attrs)
			}
			o.ObserveInt64(inFlight, int64(js.InFlight), attrs)
		}
		return nil
	}, backlog, failing, shelvedCurrent, leaseHeld, inFlight)
	return err
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
