package convredis_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

const (
	envChaosChild = "CONVREDIS_CHAOS_CHILD"
	envChaosNS    = "CONVREDIS_CHAOS_NS"

	chaosDB       = 9
	chaosJobName  = "chaos-leader"
	chaosLeaseTTL = 5 * time.Second
	chaosSleep    = 30 * time.Second
	chaosStep     = 100 * time.Millisecond

	chaosPollInterval  = 100 * time.Millisecond
	chaosChildReadyCap = 30 * time.Second
	chaosSuccessorCap  = 30 * time.Second
	chaosCleanupWait   = 5 * time.Second
)

func chaosPassStartKey(ns string) string  { return "convredis:chaos:" + ns + ":pass-start" }
func chaosCompletionKey(ns string) string { return "convredis:chaos:" + ns + ":completion" }

func chaosSleepCtx(ctx context.Context, total time.Duration) error {
	deadline := time.Now().Add(total)
	for !time.Now().After(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(chaosStep):
		}
	}
	return nil
}

func chaosHandler(kv converge.KV, key string, sleep time.Duration) func(context.Context, reconcile.ID) error {
	return func(ctx context.Context, _ reconcile.ID) error {
		if err := kv.Set(ctx, key, []byte("1"), 0); err != nil {
			return err
		}
		if sleep <= 0 {
			return nil
		}
		return chaosSleepCtx(ctx, sleep)
	}
}

type chaosElectionObserver struct {
	job string
	p   *converge.Producer
}

func (o *chaosElectionObserver) Observe(e converge.Event) {
	lt, ok := e.(converge.LeaseChanged)
	if !ok || !lt.Held || lt.Job != o.job {
		return
	}
	o.p.Notify(context.Background(), o.job, "")
}

func newChaosRuntime(t testing.TB, mq converge.MQ, lease converge.Lease, kv converge.KV, ns string, fn func(context.Context, reconcile.ID) error) *converge.Runtime {
	t.Helper()
	p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: ns})
	if err != nil {
		t.Fatalf("convredis: chaos producer: %v", err)
	}
	obs := &chaosElectionObserver{job: chaosJobName, p: p}
	rt, err := converge.New(converge.Options{
		Namespace: ns,
		MQ:        mq,
		Lease:     lease,
		KV:        kv,
		Observer:  obs,
		LeaseTTL:  chaosLeaseTTL,
	})
	if err != nil {
		t.Fatalf("convredis: chaos runtime: %v", err)
	}
	if err := reconcile.Register(rt, reconcile.Spec{
		Job:       reconcile.NewJob(chaosJobName, reconcile.JobOpts{}),
		Reconcile: fn,
		RunMode:   converge.OnOneReplica,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Hour)),
			reconcile.Notifications(),
		},
	}); err != nil {
		t.Fatalf("convredis: chaos job register: %v", err)
	}
	return rt
}

func awaitKV(t *testing.T, kv converge.KV, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, ok, err := kv.Get(context.Background(), key)
		if err == nil && ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(chaosPollInterval)
	}
}

func awaitKVOrChildExit(t *testing.T, kv converge.KV, key string, timeout time.Duration, childDone <-chan error) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, ok, err := kv.Get(context.Background(), key)
		if err == nil && ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case werr := <-childDone:
			t.Fatalf("convredis: chaos child exited before writing its pass-start marker: %v", werr)
			return false
		case <-time.After(chaosPollInterval):
		}
	}
}

func TestChaosChild(t *testing.T) {
	if os.Getenv(envChaosChild) != "1" {
		t.Skip("convredis: not invoked as a chaos child process")
	}
	addr := os.Getenv(realAddrEnv)
	if addr == "" {
		t.Fatalf("convredis: %s not set for chaos child", realAddrEnv)
	}
	ns := os.Getenv(envChaosNS)
	if ns == "" {
		t.Fatal("convredis: CONVREDIS_CHAOS_NS not set for chaos child")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: chaosDB})
	defer client.Close()
	kv := convredis.NewKV(client)
	lease := convredis.NewLease(client)
	mq := convredis.NewStreamsMQ(client, convredis.StreamsOpts{})
	rt := newChaosRuntime(t, mq, lease, kv, ns, chaosHandler(kv, chaosPassStartKey(ns), chaosSleep))
	if err := rt.Run(context.Background()); err != nil {
		t.Fatalf("convredis: chaos child runtime: %v", err)
	}
}

func TestChaosKillLeaderHandoff(t *testing.T) {
	addr := os.Getenv(realAddrEnv)
	if addr == "" {
		t.Skipf("%s not set", realAddrEnv)
	}

	client := openReal(t)
	kv := convredis.NewKV(client)
	lease := convredis.NewLease(client)
	mq := convredis.NewStreamsMQ(client, convredis.StreamsOpts{})
	ns := fmt.Sprintf("chaos-%d", time.Now().UnixNano())

	childCmd := exec.Command(os.Args[0], "-test.run=TestChaosChild$")
	childCmd.Env = append(os.Environ(), envChaosChild+"=1", envChaosNS+"="+ns)
	childCmd.Stdout = os.Stderr
	childCmd.Stderr = os.Stderr
	if err := childCmd.Start(); err != nil {
		t.Fatalf("convredis: start chaos child: %v", err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- childCmd.Wait() }()
	t.Cleanup(func() {
		if childCmd.Process != nil {
			childCmd.Process.Kill()
		}
		select {
		case <-childDone:
		case <-time.After(chaosCleanupWait):
		}
	})

	passStartKey := chaosPassStartKey(ns)
	if !awaitKVOrChildExit(t, kv, passStartKey, chaosChildReadyCap, childDone) {
		t.Fatalf("convredis: chaos child never wrote its pass-start marker within %s", chaosChildReadyCap)
	}

	killTime := time.Now()
	if err := childCmd.Process.Kill(); err != nil {
		t.Fatalf("convredis: kill chaos child: %v", err)
	}
	<-childDone

	rt := newChaosRuntime(t, mq, lease, kv, ns, chaosHandler(kv, chaosCompletionKey(ns), 0))
	rtCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(rtCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(chaosCleanupWait):
		}
	})

	if !awaitKV(t, kv, chaosCompletionKey(ns), chaosSuccessorCap) {
		t.Fatalf("convredis: successor never completed its pass within %s of the kill", chaosSuccessorCap)
	}
	t.Logf("convredis: chaos hand-off completed %s after kill", time.Since(killTime))
}
