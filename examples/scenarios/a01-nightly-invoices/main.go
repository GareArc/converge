package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const demoWindow = 2 * time.Second

type ledger struct {
	mu     sync.Mutex
	due    []string
	issued []string
}

func newLedger(due ...string) *ledger { return &ledger{due: due} }

func (l *ledger) generateDueInvoices(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.issued = append(l.issued, l.due...)
	l.due = nil
	return nil
}

func (l *ledger) invoiced() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.issued)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return err
	}

	rt, err := converge.New(converge.Options{
		Namespace: "billing",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	billing := newLedger("acct-1001", "acct-1002", "acct-1003")

	err = reconcile.Periodic(rt, "generate-invoices",
		reconcile.Cron("5 0 * * *", reconcile.CronOpts{Location: tokyo}),
		billing.generateDueInvoices,
		reconcile.PeriodicOpts{Timeout: 30 * time.Minute})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("invoices issued: %v\n", billing.invoiced())
	return nil
}
