package ctl

import (
	"time"

	"github.com/GareArc/converge/internal/keys"
)

const (
	OpPoke    = "poke"
	OpHint    = "hint"
	OpRunPass = "run-pass"
	OpPause   = "pause"
	OpResume  = "resume"
)

type Command struct {
	Op   string `json:"op"`
	Job  string `json:"job"`
	ID   string `json:"id,omitempty"`
	OpID string `json:"op_id"`
}

type Response struct {
	Replica string    `json:"replica"`
	Acted   bool      `json:"acted"`
	Err     string    `json:"err,omitempty"`
	At      time.Time `json:"at"`
}

type Request struct {
	Op      string
	Job     string
	ID      string
	Timeout time.Duration
}

func Queue(ns string) string { return keys.Ctl(ns) }

func ResPrefix(ns, opID string) string { return keys.Ctl(ns, "res", opID) }

func ResKey(ns, opID, replica string) string { return keys.Ctl(ns, "res", opID, replica) }

func PausedKey(ns, job string) string { return keys.Ctl(ns, "paused", job) }
