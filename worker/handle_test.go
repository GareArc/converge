package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
)

func okTaskInfo() taskInfo {
	return taskInfo{name: "job", queue: "job", version: 1}
}

func okRun() runFunc {
	return func(ctx context.Context, payload []byte) error { return nil }
}

func noopHandler(context.Context, string) error { return nil }

func mustHandleRuntime(t *testing.T, o converge.Options) *converge.Runtime {
	t.Helper()
	rt, err := converge.New(o)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

type publishConsumeMQ struct{ mq *inmem.MQ }

func (m publishConsumeMQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	return m.mq.Publish(ctx, queue, msg)
}

func (m publishConsumeMQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return m.mq.Consume(ctx, queue, deliver)
}

type groupOnlyMQ struct{ mq *inmem.MQ }

func (m groupOnlyMQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	return m.mq.Publish(ctx, queue, msg)
}

func (m groupOnlyMQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return m.mq.Consume(ctx, queue, deliver)
}

func (m groupOnlyMQ) ConsumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	return m.mq.ConsumeGroup(ctx, queue, group, deliver)
}

func TestNewEngineValidationMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*HandleOpts)
		wantErr string
	}{
		{"valid", func(o *HandleOpts) {}, ""},
		{"negative concurrency", func(o *HandleOpts) { o.Concurrency = -1 }, "Concurrency"},
		{"negative visibility", func(o *HandleOpts) { o.Visibility = -time.Second }, "Visibility"},
		{"negative retry max attempts", func(o *HandleOpts) { o.Retry.MaxAttempts = -1 }, "Retry"},
		{"negative retry min backoff", func(o *HandleOpts) { o.Retry.MinBackoff = -time.Second }, "Retry"},
		{"negative retry max backoff", func(o *HandleOpts) { o.Retry.MaxBackoff = -time.Second }, "Retry"},
		{"negative retry max age", func(o *HandleOpts) { o.Retry.MaxAge = -time.Second }, "Retry"},
		{"negative rate", func(o *HandleOpts) { o.RateLimit = converge.Rate{Events: -1, Per: time.Second} }, "RateLimit"},
		{"half rate events only", func(o *HandleOpts) { o.RateLimit = converge.Rate{Events: 5} }, "RateLimit"},
		{"half rate per only", func(o *HandleOpts) { o.RateLimit = converge.Rate{Per: time.Second} }, "RateLimit"},
		{"min backoff exceeds max after default", func(o *HandleOpts) { o.Retry.MinBackoff = 20 * time.Minute }, "MinBackoff"},
		{"all replicas with retry", func(o *HandleOpts) {
			o.RunMode = converge.OnAllReplicas
			o.Retry = RetryPolicy{MaxAttempts: 3}
		}, "Retry"},
		{"all replicas without retry", func(o *HandleOpts) { o.RunMode = converge.OnAllReplicas }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := HandleOpts{}
			c.mutate(&o)
			_, err := newEngine(okTaskInfo(), okRun(), o)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error %v, want it to mention %q", err, c.wantErr)
			}
		})
	}
}

func TestNewEngineAppliesDefaults(t *testing.T) {
	e, err := newEngine(okTaskInfo(), okRun(), HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.concurrency != DefaultConcurrency {
		t.Fatalf("concurrency = %d, want %d", e.cfg.concurrency, DefaultConcurrency)
	}
	if e.cfg.visibility != DefaultVisibility {
		t.Fatalf("visibility = %s, want %s", e.cfg.visibility, DefaultVisibility)
	}
	wantRetry := RetryPolicy{
		MaxAttempts: DefaultMaxAttempts,
		MinBackoff:  DefaultMinBackoff,
		MaxBackoff:  DefaultMaxBackoff,
		MaxAge:      DefaultMaxAge,
	}
	if e.cfg.retry != wantRetry {
		t.Fatalf("retry = %+v, want %+v", e.cfg.retry, wantRetry)
	}
	if e.cfg.runMode != converge.SplitAcrossReplicas {
		t.Fatalf("runMode = %v, want %v", e.cfg.runMode, converge.SplitAcrossReplicas)
	}
}

func TestHandleMisconstructedTask(t *testing.T) {
	rt := mustHandleRuntime(t, converge.Options{})
	tk := NewTask[string]("", TaskOpts{})
	err := Handle(rt, tk, noopHandler, HandleOpts{})
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("err = %v, want mention of construction failure", err)
	}
}

func TestHandleNilFn(t *testing.T) {
	rt := mustHandleRuntime(t, converge.Options{})
	tk := NewTask[string]("job", TaskOpts{})
	err := Handle[string](rt, tk, nil, HandleOpts{})
	if err == nil || !strings.Contains(err.Error(), "fn is required") {
		t.Fatalf("err = %v, want mention of missing fn", err)
	}
}

func TestHandleDuplicateTaskName(t *testing.T) {
	rt := mustHandleRuntime(t, converge.Options{})
	tk1 := NewTask[string]("job", TaskOpts{Queue: "q1"})
	tk2 := NewTask[string]("job", TaskOpts{Queue: "q2"})
	if err := Handle(rt, tk1, noopHandler, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := Handle(rt, tk2, noopHandler, HandleOpts{}); err == nil {
		t.Fatal("duplicate task name must be rejected")
	}
}

func TestHandleDuplicateQueue(t *testing.T) {
	rt := mustHandleRuntime(t, converge.Options{})
	tk1 := NewTask[string]("job-a", TaskOpts{Queue: "shared"})
	tk2 := NewTask[string]("job-b", TaskOpts{Queue: "shared"})
	if err := Handle(rt, tk1, noopHandler, HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	err := Handle(rt, tk2, noopHandler, HandleOpts{})
	if err == nil || !strings.Contains(err.Error(), "shared") {
		t.Fatalf("err = %v, want mention of queue %q", err, "shared")
	}
}

func TestBindFailures(t *testing.T) {
	cases := []struct {
		name    string
		opts    converge.Options
		handle  HandleOpts
		wantErr string
	}{
		{
			name:    "no mq anywhere",
			opts:    converge.Options{},
			handle:  HandleOpts{},
			wantErr: "needs an MQ",
		},
		{
			name:    "split without group consumer",
			opts:    converge.Options{},
			handle:  HandleOpts{MQ: publishConsumeMQ{inmem.NewMQ()}, RunMode: converge.SplitAcrossReplicas},
			wantErr: "GroupConsumer",
		},
		{
			name:    "all replicas without broadcast consumer",
			opts:    converge.Options{},
			handle:  HandleOpts{MQ: publishConsumeMQ{inmem.NewMQ()}, RunMode: converge.OnAllReplicas},
			wantErr: "BroadcastConsumer",
		},
		{
			name:    "one replica without lease",
			opts:    converge.Options{},
			handle:  HandleOpts{MQ: inmem.NewMQ(), RunMode: converge.OnOneReplica},
			wantErr: "Options.Lease",
		},
		{
			name:    "group mode without kv",
			opts:    converge.Options{},
			handle:  HandleOpts{MQ: inmem.NewMQ(), RunMode: converge.SplitAcrossReplicas},
			wantErr: "Options.KV",
		},
		{
			name:    "group mode mq without delayed publisher",
			opts:    converge.Options{KV: inmem.NewKV()},
			handle:  HandleOpts{MQ: groupOnlyMQ{inmem.NewMQ()}, RunMode: converge.SplitAcrossReplicas},
			wantErr: "DelayedPublisher",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rt := mustHandleRuntime(t, c.opts)
			tk := NewTask[string]("job", TaskOpts{})
			if err := Handle(rt, tk, noopHandler, c.handle); err != nil {
				t.Fatal(err)
			}
			err := rt.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want it to mention %q", err, c.wantErr)
			}
		})
	}
}
