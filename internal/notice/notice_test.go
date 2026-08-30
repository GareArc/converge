package notice

import "testing"

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"plain id", "m-42"},
		{"id with punctuation", `tenant/7 "x"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload, err := Encode(c.id)
			if err != nil {
				t.Fatalf("Encode(%q): %v", c.id, err)
			}
			got, err := Decode(payload)
			if err != nil {
				t.Fatalf("Decode(%q): %v", payload, err)
			}
			if got.ID != c.id || got.All {
				t.Fatalf("Decode = %+v, want ID %q and All false", got, c.id)
			}
		})
	}
}

func TestEncodeRefusesAnEmptyID(t *testing.T) {
	if payload, err := Encode(""); err == nil {
		t.Fatalf("Encode(\"\") = %q, want an error; the whole job is EncodeAll", payload)
	}
}

func TestEncodeAllIsTheDocumentedWireForm(t *testing.T) {
	payload, err := EncodeAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"all":true}` {
		t.Fatalf("EncodeAll = %s, want {\"all\":true}", payload)
	}
	got, err := Decode(payload)
	if err != nil || !got.All || got.ID != "" {
		t.Fatalf("Decode(EncodeAll) = %+v, %v; want All true", got, err)
	}
}

func TestEncodeIDIsTheDocumentedWireForm(t *testing.T) {
	payload, err := Encode("ws-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"id":"ws-1"}` {
		t.Fatalf("Encode = %s, want {\"id\":\"ws-1\"}", payload)
	}
}

func TestDecodeRejectsUndecodablePayloads(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"garbage", []byte("{not valid json")},
		{"empty", nil},
		{"wrong shape", []byte(`[1,2,3]`)},
		{"wrong field type", []byte(`{"id":42}`)},
		{"empty id", []byte(`{"id":""}`)},
		{"no fields", []byte(`{}`)},
		{"both fields", []byte(`{"id":"ws-1","all":true}`)},
		{"all false and no id", []byte(`{"all":false}`)},
		{"json null", []byte("null")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Decode(c.payload)
			if err == nil {
				t.Fatalf("Decode(%q) = %+v, want an error", c.payload, got)
			}
			if got != (Notification{}) {
				t.Fatalf("Decode(%q) = %+v, want the zero Notification on error", c.payload, got)
			}
		})
	}
}

func TestDecodeToleratesUnknownFields(t *testing.T) {
	got, err := Decode([]byte(`{"id":"m-7","reason":"future field"}`))
	if err != nil || got.ID != "m-7" {
		t.Fatalf("Decode = %+v, %v; want ID m-7", got, err)
	}
	got, err = Decode([]byte(`{"all":true,"reason":"bulk import"}`))
	if err != nil || !got.All {
		t.Fatalf("Decode = %+v, %v; want All true", got, err)
	}
}

func TestDecodeTreatsExplicitEmptyIDWithAllAsAll(t *testing.T) {
	got, err := Decode([]byte(`{"id":"","all":true}`))
	if err != nil || !got.All {
		t.Fatalf("Decode = %+v, %v; an empty id makes no claim, All wins", got, err)
	}
}
