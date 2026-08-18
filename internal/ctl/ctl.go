package ctl

import (
	"strings"
	"time"
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

func key(ns string, parts ...string) string {
	elems := make([]string, 0, len(parts)+2)
	if ns != "" {
		elems = append(elems, ns)
	}
	elems = append(elems, "converge", "ctl")
	elems = append(elems, parts...)
	return strings.Join(elems, "/")
}

func Queue(ns string) string { return key(ns) }

func ResPrefix(ns, opID string) string { return key(ns, "res", opID) }

func ResKey(ns, opID, replica string) string { return key(ns, "res", opID, replica) }

func PausedKey(ns, job string) string { return key(ns, "paused", job) }
