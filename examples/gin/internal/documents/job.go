package documents

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sweepEvery = 30 * time.Second
	runTimeout = 15 * time.Second
)

var job = reconcile.NewJob("index-documents", reconcile.JobOpts{})

func Register(rt *converge.Runtime, r gin.IRouter, db *pgxpool.Pool) error {
	store := NewStore(db)
	err := reconcile.Register(rt, reconcile.Spec{
		Job: job,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.StringIDs(store.NeedingIndex),
				reconcile.Every(sweepEvery)),
			reconcile.Notifications(),
		},
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			return store.Index(ctx, string(id))
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
	r.PUT("/documents/:id", h.upsert)
	r.GET("/search", h.search)
	return nil
}
