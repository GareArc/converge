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

type JobInfo struct {
	Job      string
	Surface  Surface
	RunMode  RunMode
	Queue    string
	Settings map[string]string
}
