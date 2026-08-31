package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/debughttp"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const shutdownGrace = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	rt, err := converge.New(converge.Options{
		Namespace: cfg.Namespace,
		MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
		Lease:     convredis.NewLease(rdb),
		KV:        convredis.NewKV(rdb),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	engine.Any("/debug/*path", gin.WrapH(debughttp.ReadOnlyHandler(rt)))

	for _, register := range []func(*converge.Runtime, gin.IRouter, *pgxpool.Pool) error{} {
		if err := register(rt, engine, db); err != nil {
			return err
		}
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: engine}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return rt.Run(gctx) })
	g.Go(func() error {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), shutdownGrace)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
