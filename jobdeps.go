package converge

import "time"

type JobDeps struct {
	MQ           MQ
	Lease        Lease
	KV           KV
	Observer     Observer
	Clock        Clock
	Namespace    string
	LeaseTTL     time.Duration
	DrainTimeout time.Duration
	Middleware   []Middleware
}
