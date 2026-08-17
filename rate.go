package converge

import "time"

type Rate struct {
	Events int
	Per    time.Duration
}

func (r Rate) IsZero() bool { return r.Events == 0 && r.Per == 0 }
