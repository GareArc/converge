package hook

var RegisterJob func(rt any, job any) error

type ProducerWiring struct {
	MQ      any
	Clock   any
	QueueMQ func(queue string) any
}

var ProducerDeps func(rt any) (ProducerWiring, error)
