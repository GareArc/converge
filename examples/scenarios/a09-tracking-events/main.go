package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/worker"
)

const (
	demoWindow = 2 * time.Second
	namespace  = "logistics"
)

type TrackingEvent struct {
	Shipment string    `json:"shipment"`
	Status   string    `json:"status"`
	Seen     time.Time `json:"seen"`
}

type shipmentLedger struct {
	mu     sync.Mutex
	latest map[string]TrackingEvent
	events int
}

func newShipmentLedger() *shipmentLedger {
	return &shipmentLedger{latest: map[string]TrackingEvent{}}
}

func (l *shipmentLedger) applyEvent(_ context.Context, e TrackingEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events++
	if known, ok := l.latest[e.Shipment]; ok && known.Seen.After(e.Seen) {
		return nil
	}
	l.latest[e.Shipment] = e
	return nil
}

func (l *shipmentLedger) report() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	lines := make([]string, 0, len(l.latest))
	for shipment, e := range l.latest {
		lines = append(lines, fmt.Sprintf("%s %s", shipment, e.Status))
	}
	slices.Sort(lines)
	return append(lines, fmt.Sprintf("events applied: %d", l.events))
}

var trackingEvent = worker.NewTask[TrackingEvent]("tracking-event", worker.TaskOpts{Version: 2})

func carrierFeed(at time.Time) []TrackingEvent {
	stages := []string{"accepted", "in_transit", "out_for_delivery", "delivered"}
	shipments := []string{"sh-7001", "sh-7002", "sh-7003"}
	feed := make([]TrackingEvent, 0, len(stages)*len(shipments))
	for i, status := range stages {
		for _, shipment := range shipments {
			feed = append(feed, TrackingEvent{
				Shipment: shipment,
				Status:   status,
				Seen:     at.Add(time.Duration(i) * time.Hour),
			})
		}
	}
	return feed
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mq := inmem.NewMQ()

	rt, err := converge.New(converge.Options{
		Namespace: namespace,
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	shipments := newShipmentLedger()

	err = worker.Handle(rt, trackingEvent, shipments.applyEvent, worker.HandleOpts{Concurrency: 32})
	if err != nil {
		return err
	}

	p, err := trackingEvent.NewProducer(rt.Scope())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	for _, e := range carrierFeed(time.Now()) {
		if err := p.Enqueue(ctx, e, worker.EnqueueOpts{}); err != nil {
			return err
		}
	}

	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range shipments.report() {
		fmt.Println(line)
	}
	return nil
}
