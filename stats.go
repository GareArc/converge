package converge

import "time"

// JobStats is a point-in-time snapshot. Engine-filled fields are zero until
// the surface engines land (plans 02–05).
type JobStats struct {
	Job              string
	Surface          Surface
	RunMode          RunMode
	QueueDepth       int
	Parked           int
	LastSuccess      time.Time
	ConsecutiveFails int
}
