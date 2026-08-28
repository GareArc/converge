package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow      = 2 * time.Second
	accountPageSize = 2
	seatsPerUpgrade = 8
	proSeats        = 10
	tierStarter     = "starter"
	tierPro         = "pro"
)

func tierFor(seats int) string {
	if seats >= proSeats {
		return tierPro
	}
	return tierStarter
}

type accountBook struct {
	ids []reconcile.ID

	mu         sync.Mutex
	generation map[reconcile.ID]reconcile.Version
	seats      map[reconcile.ID]int
	upgrades   map[reconcile.ID]int
	tier       map[reconcile.ID]string
	runs       map[reconcile.ID]int
}

func newAccountBook(seats, upgrades map[reconcile.ID]int) *accountBook {
	ids := make([]reconcile.ID, 0, len(seats))
	for id := range seats {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	generation := make(map[reconcile.ID]reconcile.Version, len(ids))
	for _, id := range ids {
		generation[id] = 1
	}
	return &accountBook{
		ids:        ids,
		generation: generation,
		seats:      seats,
		upgrades:   upgrades,
		tier:       map[reconcile.ID]string{},
		runs:       map[reconcile.ID]int{},
	}
}

func (b *accountBook) Latest(_ context.Context, id reconcile.ID) (reconcile.Version, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	version, ok := b.generation[id]
	if !ok {
		return 0, fmt.Errorf("account %q has no recorded generation", id)
	}
	return version, nil
}

func (b *accountBook) page(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("account page cursor %q: %w", cursor, err)
		}
		start = at
	}
	end := min(start+accountPageSize, len(b.ids))
	next := ""
	if end < len(b.ids) {
		next = strconv.Itoa(end)
	}
	return b.ids[start:end], next, nil
}

func (b *accountBook) computePlanTier(_ context.Context, id reconcile.ID) error {
	seats := b.readSeats(id)
	b.applyPendingUpgrade(id)
	b.recordTier(id, tierFor(seats))
	return nil
}

func (b *accountBook) readSeats(id reconcile.ID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runs[id]++
	return b.seats[id]
}

func (b *accountBook) applyPendingUpgrade(id reconcile.ID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.upgrades[id] == 0 {
		return
	}
	b.upgrades[id]--
	b.seats[id] += seatsPerUpgrade
	b.generation[id]++
}

func (b *accountBook) recordTier(id reconcile.ID, tier string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tier[id] = tier
}

func (b *accountBook) report() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	lines := make([]string, 0, len(b.ids))
	for _, id := range b.ids {
		lines = append(lines, fmt.Sprintf("%s seats=%d tier=%s runs=%d", id, b.seats[id], b.tier[id], b.runs[id]))
	}
	return lines
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	rt, err := converge.New(converge.Options{
		Namespace: "accounts",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	accounts := newAccountBook(
		map[reconcile.ID]int{"a-1001": 4, "a-1002": 20, "a-1003": 2},
		map[reconcile.ID]int{"a-1001": 1},
	)

	err = reconcile.Register(rt, reconcile.Spec{
		Name:      "account-plan-tier",
		Reconcile: accounts.computePlanTier,
		Triggers:  []reconcile.Trigger{reconcile.Schedule(reconcile.IDsByPage(accounts.page), reconcile.Every(time.Hour))},
		Versions:  accounts,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range accounts.report() {
		fmt.Println(line)
	}
	return nil
}
