package worker

import (
	"fmt"
	"time"

	"github.com/GareArc/converge"
)

type Snooze struct {
	_  struct{}
	In time.Duration
}

func (s Snooze) Error() string { return fmt.Sprintf("worker: snooze for %s", s.In) }

func (Snooze) ControlSurface() converge.Surface { return converge.SurfaceWorker }

type Discard struct {
	_      struct{}
	Reason string
}

func (d Discard) Error() string { return "worker: discard: " + d.Reason }

func (Discard) ControlSurface() converge.Surface { return converge.SurfaceWorker }
