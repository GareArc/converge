package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type ChargeOrder struct {
	OrderID string `json:"order_id"`
}

var chargeOrder = worker.NewTask[ChargeOrder]("charge-order", worker.TaskOpts{Queue: "payments"})

func logRuns(next converge.Handler) converge.Handler {
	return func(ctx context.Context, run converge.Run) error {
		start := time.Now()
		err := next(ctx, run)
		log.Printf("%s/%s id=%q took=%s err=%v",
			run.Surface, run.Job, run.ID, time.Since(start).Round(time.Millisecond), err)
		return err
	}
}

func newObserver() (converge.Observer, func(), error) {
	exporter, err := stdoutmetric.New()
	if err != nil {
		return nil, nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Minute))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	observer, err := convotel.NewObserver(provider.Meter("shop"))
	if err != nil {
		return nil, nil, err
	}
	return observer, func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			log.Println("metrics shutdown:", err)
		}
	}, nil
}

func registerJobs(rt *converge.Runtime, rdb *redis.Client, cfg Config) error {
	skus := func(ctx context.Context) ([]string, error) {
		return []string{"SKU-1001", "SKU-1002"}, nil
	}

	err := reconcile.Register(rt, reconcile.Spec{
		Name: "sync-inventory",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			inbound, err := rdb.Get(ctx, "warehouse:"+string(id)+":inbound").Int()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if inbound > 0 {
				return rdb.Decr(ctx, "warehouse:"+string(id)+":inbound").Err()
			}
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.OnMessage("stock-events", reconcile.IDFromJSONField("sku"), reconcile.OnMessageOpts{}),
			reconcile.Schedule(reconcile.StringIDs(skus), reconcile.Every(cfg.SyncEvery)),
		},
	})
	if err != nil {
		return err
	}

	return worker.Handle(rt, chargeOrder, func(ctx context.Context, p ChargeOrder) error {
		return nil
	}, worker.HandleOpts{
		Concurrency: 4,
		Retry:       worker.RetryPolicy{MaxAttempts: 5},
	})
}

func main() {
	cfg := configFromEnv()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	observer, shutdownMetrics, err := newObserver()
	if err != nil {
		log.Fatal(err)
	}
	defer shutdownMetrics()

	rt, err := converge.New(converge.Options{
		Namespace:    cfg.Namespace,
		MQ:           convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
		Lease:        convredis.NewLease(rdb),
		KV:           convredis.NewKV(rdb),
		Observer:     observer,
		Middleware:   []converge.Middleware{logRuns},
		DrainTimeout: cfg.DrainTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := registerJobs(rt, rdb, cfg); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
	debug := &http.Server{Addr: cfg.DebugAddr, Handler: mux}
	go func() {
		if err := debug.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println("debug server:", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("converge starting: namespace=%q redis=%s debug=%s", cfg.Namespace, cfg.RedisAddr, cfg.DebugAddr)
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
	log.Println("converge stopped cleanly")

	if err := debug.Shutdown(context.Background()); err != nil {
		log.Println("debug shutdown:", err)
	}
}
