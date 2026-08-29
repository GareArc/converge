package reconcile

import (
	"errors"
	"fmt"
	"strings"

	"github.com/GareArc/converge/internal/keys"
)

type JobOpts struct {
	Notifications string
}

type Job struct {
	name          string
	notifications string
	err           error
}

func NewJob(name string, o JobOpts) Job {
	j := Job{name: name, notifications: o.Notifications}
	switch {
	case name == "":
		j.err = errors.New("reconcile: job name is required")
	case strings.Contains(name, "/"):
		j.err = fmt.Errorf("reconcile: job %q: name must not contain %q", name, "/")
	}
	if err := keys.ValidateDeclared(o.Notifications); j.err == nil && err != nil {
		j.err = fmt.Errorf("reconcile: job %q: Notifications %w", name, err)
	}
	return j
}

func (j Job) Name() string { return j.name }

func (j Job) NotificationsName(namespace string) string {
	if j.notifications != "" {
		return j.notifications
	}
	return keys.Notifications(namespace, j.name)
}

func (j Job) check() error {
	if j.err != nil {
		return j.err
	}
	if j.name == "" {
		return errors.New("reconcile: Spec.Job is required; build one with NewJob")
	}
	return nil
}
