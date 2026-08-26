package keys

import "strings"

func join(ns string, fixed []string, parts []string) string {
	elems := make([]string, 0, len(fixed)+len(parts)+1)
	if ns != "" {
		elems = append(elems, ns)
	}
	elems = append(elems, fixed...)
	elems = append(elems, parts...)
	return strings.Join(elems, "/")
}

func Worker(ns, job string, parts ...string) string {
	return join(ns, []string{"converge", "worker", job}, parts)
}

func Reconcile(ns, job string, parts ...string) string {
	return join(ns, []string{"converge", "reconcile", job}, parts)
}

func WorkerLease(ns, job string) string { return Worker(ns, job, "lease") }

func ReconcileLease(ns, job string) string { return Reconcile(ns, job, "lease") }

func WorkerDLQPrefix(ns, job string) string { return Worker(ns, job, "dlq") + "/" }

func WorkerDLQ(ns, job, messageID string) string { return WorkerDLQPrefix(ns, job) + messageID }

func ReconcileParkedPrefix(ns, job string) string { return Reconcile(ns, job, "parked") + "/" }

func ReconcileParked(ns, job, id string) string { return ReconcileParkedPrefix(ns, job) + id }

func Tracker(namespace, id string) string { return "converge/tracker/" + namespace + "/" + id }
