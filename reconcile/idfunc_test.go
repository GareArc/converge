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

func TestIDFromJSONFieldsJoinsInOrder(t *testing.T) {
	f := reconcile.IDFromJSONFields("tenant_id", "app_id")
	id, err := f([]byte(`{"app_id": "a1", "tenant_id": "t1"}`))
	if err != nil {
		t.Fatal(err)
	}
	tenant, app, err := reconcile.Split2(id)
	if err != nil || tenant != "t1" || app != "a1" {
		t.Fatalf("Split2 = %q %q %v", tenant, app, err)
	}
	if _, err := f([]byte(`{"tenant_id": "t1"}`)); err == nil {
		t.Fatal("missing second field must error")
	}
}

func TestIDFromJSONFieldsEmpty(t *testing.T) {
	f := reconcile.IDFromJSONFields()
	if _, err := f([]byte(`{}`)); err == nil {
		t.Fatal("zero fields must error")
	}
}
