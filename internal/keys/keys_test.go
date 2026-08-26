package keys

import "testing"

func TestLayout(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"probe", Probe("ns"), "ns/converge/probe"},
		{"probe no namespace", Probe(""), "converge/probe"},
		{"inbox", Inbox("ns", "job"), "ns/converge/inbox/job"},
		{"inbox no namespace", Inbox("", "job"), "converge/inbox/job"},
		{"worker", Worker("ns", "job", "x"), "ns/converge/worker/job/x"},
		{"worker no namespace", Worker("", "job", "x"), "converge/worker/job/x"},
		{"worker bare", Worker("ns", "job"), "ns/converge/worker/job"},
		{"reconcile", Reconcile("ns", "job", "x"), "ns/converge/reconcile/job/x"},
		{"worker lease", WorkerLease("ns", "job"), "ns/converge/worker/job/lease"},
		{"reconcile lease", ReconcileLease("", "job"), "converge/reconcile/job/lease"},
		{"shelf prefix", WorkerShelfPrefix("ns", "job"), "ns/converge/worker/job/shelf/"},
		{"shelf", WorkerShelf("ns", "job", "m1"), "ns/converge/worker/job/shelf/m1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}
