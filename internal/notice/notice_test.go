package notice

import "testing"

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"plain id", "m-42"},
		{"empty id addresses the whole job", ""},
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
			if got != c.id {
				t.Fatalf("Decode = %q, want %q", got, c.id)
			}
		})
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, err := Decode(c.payload)
			if err == nil {
				t.Fatalf("Decode(%q) = %q, want an error", c.payload, id)
			}
			if id != "" {
				t.Fatalf("Decode(%q) id = %q, want empty on error", c.payload, id)
			}
		})
	}
}

func TestDecodeToleratesUnknownFields(t *testing.T) {
	id, err := Decode([]byte(`{"id":"m-7","reason":"future field"}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if id != "m-7" {
		t.Fatalf("Decode = %q, want %q", id, "m-7")
	}
}
