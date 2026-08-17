// Package sig seals converge's control-flow outcomes. Only packages in this
// module can name Signal, so only converge's own outcome types are treated
// as signals; engines recognize their own concrete types and park/DLQ any
// other Signal as a wrong-surface programming error.
package sig

import (
	"errors"

	"github.com/GareArc/converge"
)

type Signal interface {
	error
	ControlSurface() converge.Surface
}

// FromError reports the outermost Signal in err's chain, if any.
func FromError(err error) (Signal, bool) {
	var s Signal
	if err == nil || !errors.As(err, &s) {
		return nil, false
	}
	return s, true
}
