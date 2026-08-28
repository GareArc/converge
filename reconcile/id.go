package reconcile

type ID string

func ToIDs(raw ...string) []ID {
	out := make([]ID, len(raw))
	for i, r := range raw {
		out[i] = ID(r)
	}
	return out
}
