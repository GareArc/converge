package docscheck

import (
	"bufio"
	"os"
	"regexp"
	"strings"
)

var (
	contextTermRe   = regexp.MustCompile(`^\*\*([^*]+)\*\*:\s*$`)
	glossaryTermRe  = regexp.MustCompile(`^###\s+(.+?)\s*$`)
	parentheticalRe = regexp.MustCompile(`\s*\([^)]*\)`)
	parenContentRe  = regexp.MustCompile(`\(([^)]*)\)`)
	spaceRe         = regexp.MustCompile(`\s+`)
)

func ContextTerms(path string) ([]string, error) { return scanTerms(path, contextTermRe) }

func GlossaryTerms(path string) ([]string, error) { return scanTerms(path, glossaryTermRe) }

func scanTerms(path string, re *regexp.Regexp) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	inFence := false
	for sc.Scan() {
		line := sc.Text()
		if leadingFence(line) != "" {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if m := re.FindStringSubmatch(line); m != nil {
			out = append(out, strings.TrimSpace(m[1]))
		}
	}
	return out, sc.Err()
}

func NormalizeTerm(s string) string {
	s = parentheticalRe.ReplaceAllString(s, "")
	s = spaceRe.ReplaceAllString(strings.TrimSpace(s), " ")
	return strings.ToLower(s)
}

func SearchAliases(term string) []string {
	var out []string
	base := strings.TrimSpace(parentheticalRe.ReplaceAllString(term, ""))
	if base != "" {
		out = append(out, base)
	}
	for _, m := range parenContentRe.FindAllStringSubmatch(term, -1) {
		alias := strings.TrimSpace(m[1])
		if alias != "" {
			out = append(out, alias)
		}
	}
	return out
}

func MaskNonProse(src string) string {
	out := []byte(src)
	blank := func(lo, hi int) {
		for i := lo; i < hi && i < len(out); i++ {
			if out[i] != '\n' {
				out[i] = ' '
			}
		}
	}
	pos := 0
	inFence := false
	for pos < len(src) {
		eol := strings.IndexByte(src[pos:], '\n')
		if eol == -1 {
			eol = len(src) - pos
		}
		line := src[pos : pos+eol]
		trimmed := strings.TrimSpace(line)
		switch {
		case leadingFence(line) != "":
			inFence = !inFence
			blank(pos, pos+eol)
		case inFence:
			blank(pos, pos+eol)
		case strings.HasPrefix(trimmed, "# "):
			blank(pos, pos+eol)
		}
		pos += eol + 1
	}
	maskSpans(out, "`", "`")
	maskLinkTargets(out)
	return string(out)
}

func maskSpans(out []byte, open, close string) {
	s := string(out)
	i := 0
	for {
		a := strings.Index(s[i:], open)
		if a == -1 {
			return
		}
		a += i
		b := strings.Index(s[a+len(open):], close)
		if b == -1 {
			return
		}
		b = a + len(open) + b + len(close)
		for k := a; k < b; k++ {
			if out[k] != '\n' {
				out[k] = ' '
			}
		}
		i = b
	}
}

func maskLinkTargets(out []byte) {
	s := string(out)
	i := 0
	for {
		a := strings.Index(s[i:], "](")
		if a == -1 {
			return
		}
		a += i
		b := strings.IndexByte(s[a:], ')')
		if b == -1 {
			return
		}
		b = a + b + 1
		for k := a; k < b; k++ {
			if out[k] != '\n' {
				out[k] = ' '
			}
		}
		i = b
	}
}

func FirstProseUse(masked, term string) int {
	re, err := termRegexp(term)
	if err != nil {
		return -1
	}
	loc := re.FindStringIndex(masked)
	if loc == nil {
		return -1
	}
	return loc[0]
}

func FirstGloss(src, term string) int {
	q := regexp.QuoteMeta(term)
	patterns := []string{
		`(?i)^###\s+` + q + `\s*$`,
		`(?i)\*\*` + q + `\*\*`,
		`(?i)\[[^\]]*\b` + q + `\b[^\]]*\]\([^)]*glossary\.md[^)]*\)`,
	}
	best := -1
	for _, p := range patterns {
		re, err := regexp.Compile(`(?m)` + p)
		if err != nil {
			continue
		}
		if loc := re.FindStringIndex(src); loc != nil {
			if best == -1 || loc[0] < best {
				best = loc[0]
			}
		}
	}
	return best
}

func termRegexp(term string) (*regexp.Regexp, error) {
	return regexp.Compile(`(?i)\b` + regexp.QuoteMeta(term) + `\b`)
}
