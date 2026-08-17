package reconcile

import (
	"fmt"
	"strings"
)

type ID string

const idSep = "/"

var (
	idEscaper   = strings.NewReplacer("%", "%25", "/", "%2F")
	idUnescaper = strings.NewReplacer("%2F", "/", "%25", "%")
)

func JoinID(parts ...string) ID {
	esc := make([]string, len(parts))
	for i, p := range parts {
		esc[i] = idEscaper.Replace(p)
	}
	return ID(strings.Join(esc, idSep))
}

func SplitID(id ID) []string {
	raw := strings.Split(string(id), idSep)
	out := make([]string, len(raw))
	for i, p := range raw {
		out[i] = idUnescaper.Replace(p)
	}
	return out
}

func Split2(id ID) (string, string, error) {
	parts := SplitID(id)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("reconcile: id %q has %d parts, want 2", string(id), len(parts))
	}
	return parts[0], parts[1], nil
}

func ToIDs(raw ...string) []ID {
	out := make([]ID, len(raw))
	for i, r := range raw {
		out[i] = ID(r)
	}
	return out
}
