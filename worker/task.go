package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/GareArc/converge/internal/keys"
)

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type TaskOpts struct {
	Codec   Codec
	Version int
	Queue   string
}

type Task[T any] struct {
	name    string
	codec   Codec
	version int
	queue   string
	err     error
}

func NewTask[T any](name string, o TaskOpts) Task[T] {
	t := Task[T]{name: name, codec: o.Codec, version: o.Version, queue: o.Queue}
	switch {
	case name == "":
		t.err = errors.New("worker: task name is required")
	case strings.Contains(name, "/"):
		t.err = fmt.Errorf("worker: task %q: name must not contain %q", name, "/")
	case o.Version < 0:
		t.err = fmt.Errorf("worker: task %q: Version must not be negative", name)
	}
	if err := keys.ValidateDeclared(o.Queue); t.err == nil && err != nil {
		t.err = fmt.Errorf("worker: task %q: Queue %w", name, err)
	}
	if t.codec == nil {
		t.codec = jsonCodec{}
	}
	if t.version == 0 {
		t.version = 1
	}
	return t
}

func (t Task[T]) Name() string { return t.name }

func (t Task[T]) Version() int { return t.version }

func (t Task[T]) QueueName(namespace string) string {
	if t.queue != "" {
		return t.queue
	}
	return keys.Queue(namespace, t.name)
}

func (t Task[T]) Encode(v any) ([]byte, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.codec.Marshal(v)
}
