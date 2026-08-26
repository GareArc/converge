package hook

import (
	"context"
	"time"
)

var RegisterJob func(rt any, job any) error

type ProducerWiring struct {
	MQ    any
	Clock any
}

var ProducerDeps func(rt any) (ProducerWiring, error)

var ProducerSend func(p any, ctx context.Context, job string, m any, delay time.Duration) error

var ProducerNow func(p any) (time.Time, bool)

var AttachOptions func(o any, attach func(rt any)) any

var Inspect func(rt any) (any, error)

type OpsWiring struct {
	KV        any
	MQ        any
	Clock     any
	Namespace string
	Replica   string
}

var OpsDeps func(rt any) (OpsWiring, error)

var Hint func(rt any, job, id string) error

var RunPassNow func(rt any, ctx context.Context, job string) error

var Quiet func(rt any) bool
