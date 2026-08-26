package wiring

import (
	"fmt"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/hook"
)

type Producer struct {
	MQ    converge.MQ
	Clock converge.Clock
}

func ProducerFor(rt *converge.Runtime) (Producer, error) {
	w, err := hook.ProducerDeps(rt)
	if err != nil {
		return Producer{}, err
	}
	return Producer{MQ: asMQ(w.MQ), Clock: asClock(w.Clock)}, nil
}

type Ops struct {
	KV        converge.KV
	MQ        converge.MQ
	Clock     converge.Clock
	Namespace string
	Replica   string
}

func OpsFor(rt *converge.Runtime) (Ops, error) {
	w, err := hook.OpsDeps(rt)
	if err != nil {
		return Ops{}, err
	}
	return Ops{
		KV:        asKV(w.KV),
		MQ:        asMQ(w.MQ),
		Clock:     asClock(w.Clock),
		Namespace: w.Namespace,
		Replica:   w.Replica,
	}, nil
}

func Jobs(rt *converge.Runtime) ([]converge.JobInfo, error) {
	raw, err := hook.Inspect(rt)
	if err != nil {
		return nil, err
	}
	infos, ok := raw.([]converge.JobInfo)
	if !ok {
		return nil, fmt.Errorf("wiring: inspect returned %T, want []converge.JobInfo", raw)
	}
	return infos, nil
}

func Attach(o converge.Options, fn func(rt any)) (converge.Options, error) {
	out := hook.AttachOptions(o, fn)
	opts, ok := out.(converge.Options)
	if !ok {
		return converge.Options{}, fmt.Errorf("wiring: attach returned %T, want converge.Options", out)
	}
	return opts, nil
}

func asMQ(v any) converge.MQ {
	if m, ok := v.(converge.MQ); ok {
		return m
	}
	return nil
}

func asKV(v any) converge.KV {
	if kv, ok := v.(converge.KV); ok {
		return kv
	}
	return nil
}

func asClock(v any) converge.Clock {
	if c, ok := v.(converge.Clock); ok {
		return c
	}
	return nil
}
