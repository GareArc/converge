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
		{"worker", Worker("ns", "job", "x"), "ns/converge/worker/job/x"},
		{"worker no namespace", Worker("", "job", "x"), "converge/worker/job/x"},
		{"worker bare", Worker("ns", "job"), "ns/converge/worker/job"},
		{"reconcile", Reconcile("ns", "job", "x"), "ns/converge/reconcile/job/x"},
		{"worker lease", WorkerLease("ns", "job"), "ns/converge/worker/job/lease"},
		{"reconcile lease", ReconcileLease("", "job"), "converge/reconcile/job/lease"},
		{"dlq prefix", WorkerDLQPrefix("ns", "job"), "ns/converge/worker/job/dlq/"},
		{"dlq", WorkerDLQ("ns", "job", "m1"), "ns/converge/worker/job/dlq/m1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}
