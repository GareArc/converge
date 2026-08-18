package converge

import (
	"fmt"
	"time"

	"github.com/GareArc/converge/internal/durfmt"
)

type Rate struct {
	Events int
	Per    time.Duration
}

func (r Rate) IsZero() bool { return r.Events == 0 && r.Per == 0 }

func (r Rate) String() string { return fmt.Sprintf("%d/%s", r.Events, durfmt.Format(r.Per)) }
