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
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow      = 2 * time.Second
	retentionWindow = 30 * 24 * time.Hour
)

type personalData struct {
	email     string
	deletedAt time.Time
}

type vault struct {
	now func() time.Time

	mu      sync.Mutex
	records map[string]personalData
	purged  []string
}

func newVault(now func() time.Time, records map[string]personalData) *vault {
	return &vault{now: now, records: records}
}

func (v *vault) purgeExpired(_ context.Context) error {
	cutoff := v.now().Add(-retentionWindow)
	v.mu.Lock()
	defer v.mu.Unlock()
	for id, rec := range v.records {
		if rec.deletedAt.IsZero() || rec.deletedAt.After(cutoff) {
			continue
		}
		delete(v.records, id)
		v.purged = append(v.purged, id)
	}
	return nil
}

func (v *vault) report() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	purged := slices.Clone(v.purged)
	slices.Sort(purged)
	kept := make([]string, 0, len(v.records))
	for id, rec := range v.records {
		kept = append(kept, id+" <"+rec.email+">")
	}
	slices.Sort(kept)
	return []string{
		fmt.Sprintf("purged: %v", purged),
		fmt.Sprintf("retained: %v", kept),
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	rt, err := converge.New(converge.Options{
		Namespace: "compliance",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	now := time.Now()
	privacy := newVault(time.Now, map[string]personalData{
		"u-2001": {email: "ada@example.com", deletedAt: now.Add(-45 * 24 * time.Hour)},
		"u-2002": {email: "grace@example.com", deletedAt: now.Add(-10 * 24 * time.Hour)},
		"u-2003": {email: "alan@example.com"},
	})

	err = reconcile.Periodic(rt, "purge-deleted-accounts",
		reconcile.Cron("0 3 * * *", reconcile.CronOpts{}),
		privacy.purgeExpired,
		reconcile.PeriodicOpts{Timeout: 2 * time.Hour})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range privacy.report() {
		fmt.Println(line)
	}
	return nil
}
