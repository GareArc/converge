// Package converge gives services one model for all background work: a
// level-triggered reconcile surface and an edge-triggered worker surface on
// one kernel. This package is the kernel: the runtime, its ports, and the
// shared value types. See github.com/GareArc/converge/reconcile and
// github.com/GareArc/converge/worker for the two surfaces.
package converge
