package orders

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	unpaidFor  = 30 * time.Minute
	sweepEvery = time.Minute
	runTimeout = 10 * time.Second
)

var job = reconcile.NewJob("expire-unpaid-orders", reconcile.JobOpts{})

func Register(rt *converge.Runtime, r gin.IRouter, db *pgxpool.Pool) error {
	store := NewStore(db)
	err := reconcile.Register(rt, reconcile.Spec{
		Job: job,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.StringIDs(store.PendingOlderThan(unpaidFor)),
				reconcile.Every(sweepEvery)),
			reconcile.Notifications(),
		},
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			return store.CancelIfUnpaid(ctx, string(id), unpaidFor)
		},
		Timeout: runTimeout,
	})
	if err != nil {
		return err
	}
	notifier, err := job.NewProducer(rt.Scope())
	if err != nil {
		return err
	}
	h := &handlers{store: store, notifier: notifier}
	r.POST("/orders", h.create)
	r.POST("/orders/:id/pay", h.pay)
	r.GET("/orders/:id", h.get)
	return nil
}
