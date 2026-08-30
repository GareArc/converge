package worker

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewTaskDefaults(t *testing.T) {
	tk := NewTask[string]("send-invite", TaskOpts{})
	if tk.err != nil {
		t.Fatal(tk.err)
	}
	if tk.version != 1 {
		t.Fatalf("default version = %d, want 1", tk.version)
	}
	if _, ok := tk.codec.(jsonCodec); !ok {
		t.Fatalf("default codec = %T", tk.codec)
	}
	custom := NewTask[string]("send-invite", TaskOpts{Version: 3})
	if custom.version != 3 {
		t.Fatalf("opts version = %d, want 3", custom.version)
	}
}

func TestNewTaskMisconstruction(t *testing.T) {
	cases := []struct {
		name    string
		task    string
		opts    TaskOpts
		wantErr string
	}{
		{"empty name", "", TaskOpts{}, "required"},
		{"slash name", "a/b", TaskOpts{}, "must not contain"},
		{"negative version", "ok", TaskOpts{Version: -1}, "negative"},
		{"queue with leading space", "ok", TaskOpts{Queue: " q"}, "Queue"},
		{"queue with control character", "ok", TaskOpts{Queue: "q\x00"}, "control character"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tk := NewTask[string](c.task, c.opts)
			if tk.err == nil || !strings.Contains(tk.err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want mention of %q", tk.err, c.wantErr)
			}
		})
	}
}

func TestTaskAccessors(t *testing.T) {
	tk := NewTask[string]("send-invite", TaskOpts{})
	if tk.Name() != "send-invite" {
		t.Fatalf("Name() = %q, want %q", tk.Name(), "send-invite")
	}
}

func TestTaskEncodeRoundTrip(t *testing.T) {
	type payload struct {
		Name string
		Age  int
	}
	tk := NewTask[payload]("send-invite", TaskOpts{})
	want := payload{Name: "Alice", Age: 30}
	data, err := tk.Encode(want)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	var got payload
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestTaskEncodeDeferredError(t *testing.T) {
	tk := NewTask[string]("", TaskOpts{})
	if tk.err == nil {
		t.Fatal("test setup: expected a deferred construction error")
	}
	if _, err := tk.Encode("hello"); err != tk.err {
		t.Fatalf("Encode() error = %v, want the deferred construction error %v", err, tk.err)
	}
}

func TestCodecJSONRoundTrip(t *testing.T) {
	type payload struct {
		Name string
		Age  int
	}
	codec := jsonCodec{}
	original := payload{Name: "Alice", Age: 30}
	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded payload
	err = codec.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.Name != "Alice" || decoded.Age != 30 {
		t.Fatalf("got %+v, want %+v", decoded, original)
	}
}

func TestTaskQueueNameIsDeclaredOrDerived(t *testing.T) {
	derived := NewTask[string]("send-invite", TaskOpts{})
	if got := derived.QueueName("acme"); got != "acme/converge/queue/send-invite" {
		t.Fatalf("QueueName = %q", got)
	}
	if got := derived.QueueName(""); got != "converge/queue/send-invite" {
		t.Fatalf("QueueName with no namespace = %q", got)
	}
	declared := NewTask[string]("send-invite", TaskOpts{Queue: "dify:credential:rotate"})
	if got := declared.QueueName("acme"); got != "dify:credential:rotate" {
		t.Fatalf("declared QueueName = %q, want it verbatim, not namespaced", got)
	}
	hashTag := NewTask[string]("send-invite", TaskOpts{Queue: "{dify}:rotate"})
	if hashTag.err != nil {
		t.Fatalf("a Redis Cluster hash tag must be accepted: %v", hashTag.err)
	}
	if NewTask[string]("v3", TaskOpts{Version: 3}).Version() != 3 {
		t.Fatal("Version() must report the declared version")
	}
	if NewTask[string]("v0", TaskOpts{}).Version() != 1 {
		t.Fatal("Version() must report the defaulted version")
	}
}
