package docscheck

var Stoplist = []string{
	"Job",
	"Task",
	"Pass",
	"Version",
	"Queue",
	"Surface",
	"Schedule",
	"Trigger",
	"ID",
	"Outcome",
	"Worker",
	"Message ID",
}

func stoplisted() map[string]bool {
	m := make(map[string]bool, len(Stoplist))
	for _, s := range Stoplist {
		m[NormalizeTerm(s)] = true
	}
	return m
}
