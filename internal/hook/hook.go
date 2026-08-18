package hook

var RegisterJob func(rt any, job any) error

type ProducerWiring struct {
	MQ      any
	Clock   any
	QueueMQ func(queue string) any
}

var ProducerDeps func(rt any) (ProducerWiring, error)

var AttachOptions func(o any, attach func(rt any)) any

var Inspect func(rt any) (any, error)

type OpsWiring struct {
	KV        any
	MQ        any
	Clock     any
	Namespace string
	Replica   string
	QueueMQ   func(queue string) any
}

var OpsDeps func(rt any) (OpsWiring, error)
