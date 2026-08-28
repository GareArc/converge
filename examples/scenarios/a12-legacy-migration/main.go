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
	demoWindow = 10 * time.Second
	leaseTTL   = 3 * time.Second
	cutoverIn  = 2 * time.Second

	targetAlgorithm = "argon2id"
)

type credentialTable struct {
	mu       sync.Mutex
	legacy   map[reconcile.ID]string
	migrated []string
}

func newCredentialTable(legacy map[reconcile.ID]string) *credentialTable {
	return &credentialTable{legacy: legacy}
}

func (t *credentialTable) unmigrated(_ context.Context, _ string) ([]reconcile.ID, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pending := make([]reconcile.ID, 0, len(t.legacy))
	for id := range t.legacy {
		pending = append(pending, id)
	}
	slices.Sort(pending)
	return pending, "", nil
}

func (t *credentialTable) migrateOne(_ context.Context, id reconcile.ID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	algorithm, ok := t.legacy[id]
	if !ok {
		return nil
	}
	delete(t.legacy, id)
	t.migrated = append(t.migrated, fmt.Sprintf("%s %s->%s", id, algorithm, targetAlgorithm))
	return nil
}

func (t *credentialTable) report() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	migrated := slices.Clone(t.migrated)
	slices.Sort(migrated)
	return []string{
		fmt.Sprintf("migrated: %v", migrated),
		fmt.Sprintf("still legacy: %d", len(t.legacy)),
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
		Namespace: "identity",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
		LeaseTTL:  leaseTTL,
	})
	if err != nil {
		return err
	}

	creds := newCredentialTable(map[reconcile.ID]string{
		"cred-3001": "sha1",
		"cred-3002": "sha1",
		"cred-3003": "md5",
	})
	cutover := time.Now().Add(cutoverIn)

	err = reconcile.Register(rt, reconcile.Spec{
		Name:      "legacy-credential-migration",
		Reconcile: creds.migrateOne,
		Triggers:  []reconcile.Trigger{reconcile.Schedule(reconcile.IDsByPage(creds.unmigrated), reconcile.Every(time.Minute))},
		Until:     converge.Deadline(cutover),
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range creds.report() {
		fmt.Println(line)
	}
	for _, s := range rt.Stats() {
		fmt.Printf("%s state=%s\n", s.Job, s.State)
	}
	fmt.Printf("cutover: %s\n", cutover.Format(time.RFC3339))
	return nil
}
