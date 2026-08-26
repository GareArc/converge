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

func Probe(ns string) string {
	return join(ns, []string{"converge", "probe"}, nil)
}

func Inbox(ns, job string) string { return join(ns, []string{"converge", "inbox"}, []string{job}) }

func Worker(ns, job string, parts ...string) string {
	return join(ns, []string{"converge", "worker", job}, parts)
}

func Reconcile(ns, job string, parts ...string) string {
	return join(ns, []string{"converge", "reconcile", job}, parts)
}

func WorkerLease(ns, job string) string { return Worker(ns, job, "lease") }

func ReconcileLease(ns, job string) string { return Reconcile(ns, job, "lease") }

func WorkerShelfPrefix(ns, job string) string { return Worker(ns, job, "shelf") + "/" }

func WorkerShelf(ns, job, messageID string) string { return WorkerShelfPrefix(ns, job) + messageID }
