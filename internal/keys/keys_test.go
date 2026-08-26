package keys

import "testing"

func TestLayout(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"worker", Worker("ns", "job", "x"), "ns/converge/worker/job/x"},
		{"worker no namespace", Worker("", "job", "x"), "converge/worker/job/x"},
		{"worker bare", Worker("ns", "job"), "ns/converge/worker/job"},
		{"reconcile", Reconcile("ns", "job", "x"), "ns/converge/reconcile/job/x"},
		{"worker lease", WorkerLease("ns", "job"), "ns/converge/worker/job/lease"},
		{"reconcile lease", ReconcileLease("", "job"), "converge/reconcile/job/lease"},
		{"dlq prefix", WorkerDLQPrefix("ns", "job"), "ns/converge/worker/job/dlq/"},
		{"dlq", WorkerDLQ("ns", "job", "m1"), "ns/converge/worker/job/dlq/m1"},
		{"parked prefix", ReconcileParkedPrefix("ns", "job"), "ns/converge/reconcile/job/parked/"},
		{"parked", ReconcileParked("ns", "job", "id1"), "ns/converge/reconcile/job/parked/id1"},
		{"tracker", Tracker("t", "id1"), "converge/tracker/t/id1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}
