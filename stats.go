package converge

import "time"

type JobStats struct {
	Job              string
	Surface          Surface
	RunMode          RunMode
	QueueDepth       int
	Parked           int
	LastSuccess      time.Time
	ConsecutiveFails int
}
