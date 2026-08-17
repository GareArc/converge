package converge

import "time"

// JobDeps is what the runtime hands each registered engine job at Run.
// Users never construct one; the surface packages consume it.
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
