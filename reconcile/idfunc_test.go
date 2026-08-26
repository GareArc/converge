package reconcile_test

import (
	"testing"

	"github.com/GareArc/converge/reconcile"
)

func TestRawID(t *testing.T) {
	f := reconcile.RawID()
	id, err := f([]byte("ws_42"))
	if err != nil || id != "ws_42" {
		t.Fatalf("RawID = %q %v", id, err)
	}
	if _, err := f(nil); err == nil {
		t.Fatal("empty payload must error")
	}
}

func TestIDFromJSONField(t *testing.T) {
	f := reconcile.IDFromJSONField("workspace_id")
	cases := []struct {
		payload string
		want    reconcile.ID
		wantErr bool
	}{
		{`{"workspace_id": "ws_1"}`, "ws_1", false},
		{`{"workspace_id": "ws_1", "type": "changed"}`, "ws_1", false},
		{`{"other": "x"}`, "", true},
		{`{"workspace_id": 42}`, "", true},
		{`{"workspace_id": ""}`, "", true},
		{`not json`, "", true},
		{``, "", true},
	}
	for _, c := range cases {
		id, err := f([]byte(c.payload))
		if c.wantErr != (err != nil) || id != c.want {
			t.Fatalf("payload %q: id %q err %v", c.payload, id, err)
		}
	}
}
