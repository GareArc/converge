package hook

import (
	"context"
	"time"
)

var RegisterJob func(rt any, job any) error

var AttachOptions func(o any, attach func(rt any)) any

var Inspect func(rt any) (any, error)

type RuntimeDeps struct {
	KV        any
	MQ        any
	Clock     any
	Namespace string
	Replica   string
}

var RuntimeDepsOf func(rt any) (RuntimeDeps, error)

var Notify func(rt any, job, id string) error

var Sweep func(rt any, ctx context.Context, job string) error

var Quiet func(rt any) bool

var FailingIDs func(rt any, job string) (any, error)

var StopConditionDeadline func(c any) (time.Time, bool)

var StopConditionKey func(c any) (string, bool)
