package worker

import (
	"strings"
	"testing"
)

func TestNewTaskDefaults(t *testing.T) {
	tk := NewTask[string]("send-invite", TaskOpts{})
	if tk.err != nil {
		t.Fatal(tk.err)
	}
	if tk.queue != "send-invite" || tk.version != 1 {
		t.Fatalf("defaults = %q, %d", tk.queue, tk.version)
	}
	if _, ok := tk.codec.(jsonCodec); !ok {
		t.Fatalf("default codec = %T", tk.codec)
	}
	custom := NewTask[string]("send-invite", TaskOpts{Queue: "mail", Version: 3})
	if custom.queue != "mail" || custom.version != 3 {
		t.Fatalf("opts = %q, %d", custom.queue, custom.version)
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
