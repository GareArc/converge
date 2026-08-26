package convkratos_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GareArc/converge"
	convkratos "github.com/GareArc/converge/bridges/kratos"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/reconcile"
	"github.com/go-kratos/kratos/v2/transport"
)

func buildRuntime(t *testing.T, register func(*converge.Runtime)) *converge.Runtime {
	t.Helper()
	h := convergetest.New(t)
	rt := h.Build(t)
	register(rt)
	return rt
}

func TestServerImplementsTransportServer(t *testing.T) {
	var _ transport.Server = convkratos.Server(nil)
}

func TestStartRunsUntilStopCancels(t *testing.T) {
	rt := buildRuntime(t, func(rt *converge.Runtime) {})
	srv := convkratos.Server(rt)

	started := make(chan error, 1)
	go func() { started <- srv.Start(context.Background()) }()

	select {
	case <-rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never became ready")
	}

	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("Start = %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

func TestStopBeforeStartReturnsWithoutRunning(t *testing.T) {
	rt := buildRuntime(t, func(rt *converge.Runtime) {})
	srv := convkratos.Server(rt)

	stopped := make(chan error, 1)
	go func() { stopped <- srv.Stop(context.Background()) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop before Start = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop before Start blocked; kratos passes an undeadlined context by default")
	}

	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start after Stop = %v, want nil", err)
	}
	select {
	case <-rt.Ready():
		t.Fatal("Start after Stop launched the runtime")
	default:
	}
}

func TestStartReturnsTheRuntimeErrorUnchanged(t *testing.T) {
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcile.Register(rt, reconcile.Spec{
		Name:             "unreachable",
		Reconciler:       reconcile.Func(func(context.Context, reconcile.ID) error { return nil }),
		AllowUnscheduled: true,
	}); err != nil {
		t.Fatal(err)
	}

	srv := convkratos.Server(rt)
	err = srv.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "OnOneReplica needs Options.Lease") {
		t.Fatalf("Start = %v, want the runtime's own error unchanged", err)
	}
}

func TestSecondStartIsRejectedAndTheFirstStillDrains(t *testing.T) {
	rt := buildRuntime(t, func(rt *converge.Runtime) {})
	srv := convkratos.Server(rt)

	started := make(chan error, 1)
	go func() { started <- srv.Start(context.Background()) }()

	select {
	case <-rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never became ready")
	}

	if err := srv.Start(context.Background()); !errors.Is(err, convkratos.ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}

	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("first Start = %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel the first runtime")
	}
}
