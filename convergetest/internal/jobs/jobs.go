package jobs

import "github.com/GareArc/converge/worker"

type Invite struct {
	Email string `json:"email"`
}

var SendInvite = worker.NewTask[Invite]("send-invite", worker.TaskOpts{})
