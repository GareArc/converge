package reconcile

import (
	"strings"
	"testing"
)

func TestNewJobMisconstruction(t *testing.T) {
	cases := []struct {
		name    string
		job     string
		opts    JobOpts
		wantErr string
	}{
		{"empty name", "", JobOpts{}, "required"},
		{"slash name", "a/b", JobOpts{}, "must not contain"},
		{"notifications with trailing space", "ok", JobOpts{Notifications: "x "}, "Notifications"},
		{"notifications with control character", "ok", JobOpts{Notifications: "x\ny"}, "control character"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := NewJob(c.job, c.opts)
			if j.err == nil || !strings.Contains(j.err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want mention of %q", j.err, c.wantErr)
			}
		})
	}
}

func TestJobNotificationsNameIsDeclaredOrDerived(t *testing.T) {
	derived := NewJob("workspace-credentials", JobOpts{})
	if got := derived.NotificationsName("dify"); got != "dify/converge/notifications/workspace-credentials" {
		t.Fatalf("NotificationsName = %q", got)
	}
	if got := derived.NotificationsName(""); got != "converge/notifications/workspace-credentials" {
		t.Fatalf("NotificationsName with no namespace = %q", got)
	}
	declared := NewJob("workspace-credentials", JobOpts{Notifications: "dify:workspace-credentials"})
	if got := declared.NotificationsName("dify"); got != "dify:workspace-credentials" {
		t.Fatalf("declared NotificationsName = %q, want it verbatim", got)
	}
	if declared.Name() != "workspace-credentials" {
		t.Fatalf("Name = %q", declared.Name())
	}
}

func TestNewJobAcceptsAnyVisibleDeclaredName(t *testing.T) {
	for _, declared := range []string{
		"dify:workspace-credentials",
		"{tenant-7}:notify",
		"converge/notifications/workspace-credentials",
		"通知",
	} {
		j := NewJob("workspace-credentials", JobOpts{Notifications: declared})
		if j.err != nil {
			t.Fatalf("declared name %q must be accepted: %v", declared, j.err)
		}
		if got := j.NotificationsName("dify"); got != declared {
			t.Fatalf("NotificationsName = %q, want %q verbatim", got, declared)
		}
	}
}

func TestZeroJobIsRefusedBySpec(t *testing.T) {
	s := okSpec()
	s.Job = Job{}
	_, err := newEngine(s)
	if err == nil || !strings.Contains(err.Error(), "Spec.Job") {
		t.Fatalf("err = %v, want mention of Spec.Job", err)
	}
}
