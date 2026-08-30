package keys

import (
	"strings"
	"testing"
)

func TestLayout(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"probe", Probe("ns"), "ns/converge/probe"},
		{"probe no namespace", Probe(""), "converge/probe"},
		{"notifications", Notifications("ns", "job"), "ns/converge/notifications/job"},
		{"notifications no namespace", Notifications("", "job"), "converge/notifications/job"},
		{"queue", Queue("ns", "task"), "ns/converge/queue/task"},
		{"queue no namespace", Queue("", "task"), "converge/queue/task"},
		{"worker", Worker("ns", "job", "x"), "ns/converge/worker/job/x"},
		{"worker no namespace", Worker("", "job", "x"), "converge/worker/job/x"},
		{"worker bare", Worker("ns", "job"), "ns/converge/worker/job"},
		{"reconcile", Reconcile("ns", "job", "x"), "ns/converge/reconcile/job/x"},
		{"worker lease", WorkerLease("ns", "job"), "ns/converge/worker/job/lease"},
		{"reconcile lease", ReconcileLease("", "job"), "converge/reconcile/job/lease"},
		{"shelf prefix", WorkerShelfPrefix("ns", "job"), "ns/converge/worker/job/shelf/"},
		{"shelf", WorkerShelf("ns", "job", "m1"), "ns/converge/worker/job/shelf/m1"},
		{"tombstone", Tombstone("ns", "job"), "ns/converge/tombstone/job"},
		{"tombstone no namespace", Tombstone("", "job"), "converge/tombstone/job"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}

func TestValidateDeclared(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"empty means derived", "", ""},
		{"plain", "dify:credential:rotate", ""},
		{"redis hash tag", "{dify}:credential:rotate", ""},
		{"internal space", "credential rotate", ""},
		{"slashes and unicode", "équipe/rotation", ""},
		{"leading space", " x", "leading or trailing whitespace"},
		{"trailing tab", "x\t", "leading or trailing whitespace"},
		{"embedded newline", "a\nb", "control character at byte 1 (0x0a)"},
		{"embedded nul", "a\x00b", "control character at byte 1 (0x00)"},
		{"delete", "a\x7fb", "control character at byte 1 (0x7f)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDeclared(tc.value)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateDeclared(%q) = %v, want nil", tc.value, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateDeclared(%q) = %v, want mention of %q", tc.value, err, tc.wantErr)
			}
		})
	}
}
