package credcheck

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
)

type Repo interface {
	WorkspaceIDs(ctx context.Context) ([]string, error)
	SyncCredentials(ctx context.Context, workspaceID string) error
	AppIDs(ctx context.Context) ([]string, error)
	RunApp(ctx context.Context, appID string) error
}

type workspaceReconciler struct{ repo Repo }

func (r workspaceReconciler) Reconcile(ctx context.Context, id reconcile.ID) error {
	return r.repo.SyncCredentials(ctx, string(id))
}

type appReconciler struct{ repo Repo }

func (r appReconciler) Reconcile(ctx context.Context, id reconcile.ID) error {
	return r.repo.RunApp(ctx, string(id))
}

func NewReconciler(rt *converge.Runtime, repo Repo) (struct{}, error) {
	if err := reconcile.Register(rt, reconcile.Spec{
		Name:       "workspace-credentials",
		Reconciler: workspaceReconciler{repo: repo},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(repo.WorkspaceIDs), reconcile.Every(24*time.Hour)),
		},
	}); err != nil {
		return struct{}{}, err
	}
	if err := reconcile.Register(rt, reconcile.Spec{
		Name:       "app-runner",
		Reconciler: appReconciler{repo: repo},
		RunMode:    converge.OnOneReplica,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(repo.AppIDs), reconcile.Every(time.Hour)),
		},
	}); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, nil
}
