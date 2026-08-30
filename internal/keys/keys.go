package keys

import (
	"fmt"
	"strings"
	"unicode"
)

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

func Notifications(ns, job string) string {
	return join(ns, []string{"converge", "notifications"}, []string{job})
}

func Queue(ns, task string) string { return join(ns, []string{"converge", "queue"}, []string{task}) }

func ValidateDeclared(name string) error {
	if name == "" {
		return nil
	}
	if strings.TrimSpace(name) != name {
		return fmt.Errorf("%q has leading or trailing whitespace", name)
	}
	for i, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("%q has a control character at byte %d (0x%02x)", name, i, r)
		}
	}
	return nil
}

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

func Tombstone(ns, job string) string {
	return join(ns, []string{"converge", "tombstone"}, []string{job})
}
