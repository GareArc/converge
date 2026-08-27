package converge

import "time"

type JobStats struct {
	Job              string
	Surface          Surface
	RunMode          RunMode
	State            State
	LeaseHeld        bool
	InFlight         int
	Backlog          int
	BacklogKnown     bool
	BacklogAt        time.Time
	Failing          int
	Shelved          int
	ShelvedKnown     bool
	LastSuccess      time.Time
	LastError        error
	LastErrorAt      time.Time
	ConsecutiveFails int
}

type FailingID struct {
	ID       string
	Failures int
	Err      error
	NextTry  time.Time
}

type JobInfo struct {
	Job      string
	Surface  Surface
	RunMode  RunMode
	Queue    string
	Settings map[string]string
}
