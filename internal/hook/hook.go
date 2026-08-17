// Package hook is the registration seam between the kernel and the surface
// engines: package converge installs RegisterJob in init; reconcile and
// worker call it. internal/ visibility seals the seam inside this module.
package hook

var RegisterJob func(rt any, job any) error
