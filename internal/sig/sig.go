package sig

import (
	"errors"

	"github.com/GareArc/converge"
)

type Signal interface {
	error
	ControlSurface() converge.Surface
}

func FromError(err error) (Signal, bool) {
	var s Signal
	if err == nil || !errors.As(err, &s) {
		return nil, false
	}
	return s, true
}
