package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type TaskOpts struct {
	Queue   string
	Codec   Codec
	Version int
}

type Task[T any] struct {
	name    string
	queue   string
	codec   Codec
	version int
	err     error
}

func NewTask[T any](name string, o TaskOpts) Task[T] {
	t := Task[T]{name: name, queue: o.Queue, codec: o.Codec, version: o.Version}
	switch {
	case name == "":
		t.err = errors.New("worker: task name is required")
	case strings.Contains(name, "/"):
		t.err = fmt.Errorf("worker: task %q: name must not contain %q", name, "/")
	case o.Version < 0:
		t.err = fmt.Errorf("worker: task %q: Version must not be negative", name)
	}
	if t.queue == "" {
		t.queue = name
	}
	if t.codec == nil {
		t.codec = jsonCodec{}
	}
	if t.version == 0 {
		t.version = 1
	}
	return t
}
