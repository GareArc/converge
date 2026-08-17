package converge

import "time"

// Rate is a process-local token bucket: Events per Per, with Events also
// serving as the burst size. The zero value means unlimited.
type Rate struct {
	Events int
	Per    time.Duration
}

func (r Rate) IsZero() bool { return r.Events == 0 && r.Per == 0 }
