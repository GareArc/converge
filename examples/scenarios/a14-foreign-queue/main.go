package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

const (
	demoWindow        = 3 * time.Second
	pollStep          = 5 * time.Millisecond
	pingTimeout       = time.Second
	redisAddrEnv      = "REDIS_ADDR"
	defaultRedisAddr  = "127.0.0.1:6379"
	namespace         = "enterprise"
	foreignQueue      = "enterprise:workspace:sync:queue"
	workspacePageSize = 2
	newWorkspace      = reconcile.ID("ws-9004")
)

type workspaceRegistry struct {
	mu     sync.Mutex
	ids    []reconcile.ID
	listed bool
	synced map[reconcile.ID]int
}

func newWorkspaceRegistry(ids ...string) *workspaceRegistry {
	return &workspaceRegistry{ids: reconcile.ToIDs(ids...), synced: map[reconcile.ID]int{}}
}

func (r *workspaceRegistry) page(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("workspace page cursor %q: %w", cursor, err)
		}
		start = at
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	end := min(start+workspacePageSize, len(r.ids))
	next := ""
	if end < len(r.ids) {
		next = strconv.Itoa(end)
	} else {
		r.listed = true
	}
	return slices.Clone(r.ids[start:end]), next, nil
}

func (r *workspaceRegistry) add(id reconcile.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *workspaceRegistry) fullyListed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listed
}

func (r *workspaceRegistry) syncCredentials(_ context.Context, id reconcile.ID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.synced[id]++
	return nil
}

func (r *workspaceRegistry) report() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	lines := make([]string, 0, len(r.synced))
	for id, n := range r.synced {
		lines = append(lines, fmt.Sprintf("%s synced %d time(s)", id, n))
	}
	slices.Sort(lines)
	return lines
}

func redisAddr() string {
	if addr := os.Getenv(redisAddrEnv); addr != "" {
		return addr
	}
	return defaultRedisAddr
}

func clearNamespace(ctx context.Context, kv converge.KV, prefix string) error {
	cursor := ""
	for {
		found, next, err := kv.Scan(ctx, prefix, cursor)
		if err != nil {
			return err
		}
		for _, key := range found {
			if err := kv.Delete(ctx, key); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}

func awaitFirstSweep(ctx context.Context, rt *converge.Runtime, workspaces *workspaceRegistry) error {
	select {
	case <-rt.Ready():
	case <-ctx.Done():
		return ctx.Err()
	}
	for !workspaces.fullyListed() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollStep):
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := redisAddr()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	pingCtx, cancelPing := context.WithTimeout(context.Background(), pingTimeout)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		fmt.Printf("no Redis at %s (%v)\n", addr, err)
		fmt.Printf("start one and re-run; set %s to point elsewhere\n", redisAddrEnv)
		return nil
	}

	kv := convredis.NewKV(rdb)

	rt, err := converge.New(converge.Options{
		Namespace: namespace,
		MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{Retention: 7 * 24 * time.Hour}),
		Lease:     convredis.NewLease(rdb),
		KV:        kv,
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	workspaces := newWorkspaceRegistry("ws-9001", "ws-9002", "ws-9003")

	err = reconcile.Register(rt, reconcile.Spec{
		Name:      "workspace-credentials",
		Reconcile: workspaces.syncCredentials,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDsByPage(workspaces.page), reconcile.Every(5*time.Minute)),
			reconcile.NotificationsFrom(foreignQueue, reconcile.NotificationsOpts{
				ID: reconcile.IDFromJSON("workspace_id"),
				MQ: convredis.NewListMQ(rdb),
			}),
		},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	if err := clearNamespace(ctx, kv, namespace+"/"); err != nil {
		return err
	}
	if err := rdb.Del(ctx, foreignQueue).Err(); err != nil {
		return err
	}

	pushed, err := json.Marshal(map[string]string{"workspace_id": string(newWorkspace), "reason": "rotated"})
	if err != nil {
		return err
	}

	provisioned := make(chan error, 1)
	go func() {
		if err := awaitFirstSweep(ctx, rt, workspaces); err != nil {
			provisioned <- err
			return
		}
		workspaces.add(newWorkspace)
		provisioned <- rdb.LPush(ctx, foreignQueue, pushed).Err()
	}()

	if err := rt.Run(ctx); err != nil {
		return err
	}
	if err := <-provisioned; err != nil {
		return err
	}

	for _, line := range workspaces.report() {
		fmt.Println(line)
	}
	fmt.Printf("%s was added after the sweep had listed the registry, so its run came from the foreign queue\n", newWorkspace)
	return nil
}
