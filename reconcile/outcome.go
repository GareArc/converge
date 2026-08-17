package reconcile

import (
	"fmt"
	"time"

	"github.com/GareArc/converge"
)

type CheckAgain struct {
	_  struct{}
	In time.Duration
}

func (c CheckAgain) Error() string {
	return fmt.Sprintf("reconcile: check again in %s", c.In)
}

func (CheckAgain) ControlSurface() converge.Surface { return converge.SurfaceReconcile }

type outdated struct{}

func (outdated) Error() string { return "reconcile: intent moved past this run" }

func (outdated) ControlSurface() converge.Surface { return converge.SurfaceReconcile }

var ErrOutdated error = outdated{}
