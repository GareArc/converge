package docscheck

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func mustRoot(t *testing.T) string {
	t.Helper()
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

const uncheckedGoBlockBudget = 71

func TestTaggedGoBlocksMatchTheirSourceFiles(t *testing.T) {
	root := mustRoot(t)
	files, err := MarkdownFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	unchecked := 0
	for _, f := range files {
		blocks, err := ParseBlocks(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		for _, b := range blocks {
			if b.Lang != "go" {
				continue
			}
			title, ok := b.Attrs["title"]
			if !ok {
				unchecked++
				continue
			}
			where := fmt.Sprintf("%s:%d", relTo(root, f), b.Line)
			want, err := os.ReadFile(filepath.Join(root, title))
			if err != nil {
				t.Errorf("%s: title=%s: %v", where, title, err)
				continue
			}
			if b.Body != string(want) {
				t.Errorf("%s: block does not match %s\n%s", where, title, firstDiff(b.Body, string(want)))
			}
		}
	}
	if unchecked > uncheckedGoBlockBudget {
		t.Errorf("unchecked go blocks: %d, budget %d; tag the new block title=<path> or raise uncheckedGoBlockBudget deliberately",
			unchecked, uncheckedGoBlockBudget)
	}
	t.Logf("unchecked go blocks: %d (budget %d)", unchecked, uncheckedGoBlockBudget)
}

func relTo(root, path string) string {
	r, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(r)
}

func firstDiff(got, want string) string {
	g := strings.Split(got, "\n")
	w := strings.Split(want, "\n")
	n := len(g)
	if len(w) > n {
		n = len(w)
	}
	for i := 0; i < n; i++ {
		var a, b string
		if i < len(g) {
			a = g[i]
		}
		if i < len(w) {
			b = w[i]
		}
		if a != b {
			return fmt.Sprintf("  first difference at line %d\n  in doc:  %q\n  in file: %q", i+1, a, b)
		}
	}
	return "  (no line difference; check trailing newline)"
}

func TestGlossaryCoversContext(t *testing.T) {
	root := mustRoot(t)
	ctx, err := ContextTerms(filepath.Join(root, "CONTEXT.md"))
	if err != nil {
		t.Fatal(err)
	}
	glos, err := GlossaryTerms(filepath.Join(root, "docs", "glossary.md"))
	if err != nil {
		t.Fatal(err)
	}
	inCtx := map[string]bool{}
	for _, s := range ctx {
		inCtx[NormalizeTerm(s)] = true
	}
	inGlos := map[string]bool{}
	for _, s := range glos {
		inGlos[NormalizeTerm(s)] = true
	}
	for _, s := range ctx {
		if !inGlos[NormalizeTerm(s)] {
			t.Errorf("CONTEXT.md defines %q; docs/glossary.md does not", s)
		}
	}
	for _, s := range glos {
		if !inCtx[NormalizeTerm(s)] {
			t.Errorf("docs/glossary.md defines %q; CONTEXT.md does not", s)
		}
	}
}

func TestNoTermUsedCold(t *testing.T) {
	root := mustRoot(t)
	terms, err := ContextTerms(filepath.Join(root, "CONTEXT.md"))
	if err != nil {
		t.Fatal(err)
	}
	skip := stoplisted()
	pages, err := BarPages(root)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, term := range terms {
		if skip[NormalizeTerm(term)] {
			continue
		}
		checked++
	}
	for _, page := range pages {
		raw, err := os.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		src := string(raw)
		masked := MaskNonProse(src)
		for _, term := range terms {
			if skip[NormalizeTerm(term)] {
				continue
			}
			for _, alias := range SearchAliases(term) {
				use := FirstProseUse(masked, alias)
				if use == -1 {
					continue
				}
				gloss := FirstGloss(src, alias)
				if gloss == -1 {
					t.Errorf("%s: uses %q with no definition and no glossary link", relTo(root, page), alias)
					continue
				}
				if gloss > use {
					t.Errorf("%s: uses %q at offset %d before its definition at %d", relTo(root, page), alias, use, gloss)
				}
			}
		}
	}
	t.Logf("terms checked: %d; stoplisted: %v", checked, Stoplist)
}

var atxHeadingRe = regexp.MustCompile(`^(#{1,6})\s+(.+)$`)

var errUnterminatedFence = errors.New("markdown file ends with an unterminated code fence")

func forEachProseLine(src string, fn func(line string)) error {
	fence := ""
	for _, line := range strings.Split(src, "\n") {
		if fence == "" {
			if f := leadingFence(line); f != "" {
				fence = f
				continue
			}
			fn(line)
			continue
		}
		if isClosingFence(line, fence) {
			fence = ""
		}
	}
	if fence != "" {
		return errUnterminatedFence
	}
	return nil
}

func headingSlugs(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	seen := map[string]int{}
	var out []string
	err = forEachProseLine(string(raw), func(line string) {
		m := atxHeadingRe.FindStringSubmatch(line)
		if m == nil {
			return
		}
		text := strings.TrimRight(strings.TrimSpace(m[2]), "# ")
		out = append(out, uniqueSlug(slugify(text), seen))
	})
	return out, err
}

func uniqueSlug(s string, seen map[string]int) string {
	if s != "" && s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	n, ok := seen[s]
	seen[s] = n + 1
	if ok {
		return fmt.Sprintf("%s-%d", s, n)
	}
	return s
}

var slugSeparatorRun = regexp.MustCompile("[\\s~`!@#$%^&*()_+=\\[\\]{}|\\\\;:\"',.<>/?“”‘’-]+")

func slugify(heading string) string {
	s := slugSeparatorRun.ReplaceAllString(strings.TrimSpace(heading), "-")
	s = strings.ToLower(s)
	return strings.Trim(s, "-")
}

func closestSlug(slugs []string, want string) string {
	best := ""
	bestDist := -1
	for _, s := range slugs {
		d := levenshtein(s, want)
		if bestDist == -1 || d < bestDist {
			bestDist = d
			best = s
		}
	}
	return best
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	la, lb := len(ra), len(rb)
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func stripFencedCode(src string) (string, error) {
	var b strings.Builder
	err := forEachProseLine(src, func(line string) {
		b.WriteString(line)
		b.WriteByte('\n')
	})
	return b.String(), err
}

func stripInlineCode(src string) string {
	out := []byte(src)
	pos := 0
	for pos < len(out) {
		if out[pos] != '`' {
			pos++
			continue
		}
		start := pos
		for pos < len(out) && out[pos] == '`' {
			pos++
		}
		runLen := pos - start
		end := -1
		scan := pos
		for scan < len(out) && out[scan] != '\n' {
			if out[scan] != '`' {
				scan++
				continue
			}
			closeStart := scan
			for scan < len(out) && out[scan] == '`' {
				scan++
			}
			if scan-closeStart == runLen {
				end = scan
				break
			}
		}
		if end == -1 {
			continue
		}
		for k := start; k < end; k++ {
			if out[k] != '\n' {
				out[k] = ' '
			}
		}
		pos = end
	}
	return string(out)
}

var (
	linkRe   = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)
	refUseRe = regexp.MustCompile(`\[([^\]]+)\]\[([^\]]*)\]`)
	refDefRe = regexp.MustCompile(`(?m)^ {0,3}\[([^\]]+)\]:\s*(\S+)`)
)

func normalizeLabel(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func refTargets(prose string) (targets []string, undefinedLabels []string) {
	defs := map[string]string{}
	for _, m := range refDefRe.FindAllStringSubmatch(prose, -1) {
		defs[normalizeLabel(m[1])] = m[2]
	}
	for _, m := range refUseRe.FindAllStringSubmatch(prose, -1) {
		label := m[2]
		if label == "" {
			label = m[1]
		}
		target, ok := defs[normalizeLabel(label)]
		if !ok {
			undefinedLabels = append(undefinedLabels, label)
			continue
		}
		targets = append(targets, target)
	}
	return targets, undefinedLabels
}

func checkLinkTarget(t *testing.T, root, f, target string, slugCache map[string][]string) {
	t.Helper()
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
		return
	}
	display := target
	fragment := ""
	if i := strings.IndexByte(target, '#'); i >= 0 {
		fragment = target[i+1:]
		target = target[:i]
	}
	if target == "" {
		return
	}
	resolved := filepath.Join(filepath.Dir(f), target)
	if _, err := os.Stat(resolved); err != nil {
		t.Errorf("%s: broken link %q", relTo(root, f), display)
		return
	}
	if fragment == "" || !strings.HasSuffix(resolved, ".md") {
		return
	}
	slugs, ok := slugCache[resolved]
	if !ok {
		var err error
		slugs, err = headingSlugs(resolved)
		if err != nil {
			t.Errorf("%s: %v", relTo(root, resolved), err)
		}
		slugCache[resolved] = slugs
	}
	for _, s := range slugs {
		if s == fragment {
			return
		}
	}
	t.Errorf("%s: link %q has broken fragment %q; closest heading slug is %q",
		relTo(root, f), display, fragment, closestSlug(slugs, fragment))
}

func TestInternalMarkdownLinksResolve(t *testing.T) {
	root := mustRoot(t)
	files, err := MarkdownFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	slugCache := map[string][]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		prose, ferr := stripFencedCode(string(raw))
		if ferr != nil {
			t.Errorf("%s: %v", relTo(root, f), ferr)
			continue
		}
		prose = stripInlineCode(prose)
		for _, m := range linkRe.FindAllStringSubmatch(prose, -1) {
			checkLinkTarget(t, root, f, m[1], slugCache)
		}
		targets, undefinedLabels := refTargets(prose)
		for _, label := range undefinedLabels {
			t.Errorf("%s: reference-style link has no definition for label %q", relTo(root, f), label)
		}
		for _, target := range targets {
			checkLinkTarget(t, root, f, target, slugCache)
		}
	}
}

func TestStripFencedCodeFlagsUnterminatedFence(t *testing.T) {
	src := "before\n```go\ncode that never closes\n[real link](docs/glossary.md)\n"
	if _, err := stripFencedCode(src); err == nil {
		t.Error("expected an error for an unterminated fence")
	}
}

func TestStripFencedCodeStripsBalancedFences(t *testing.T) {
	src := "before\n```go\nfoo[X](bar)\n```\nafter\n"
	out, err := stripFencedCode(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "foo[X](bar)") {
		t.Error("fenced content was not stripped")
	}
	if !strings.Contains(out, "before") || !strings.Contains(out, "after") {
		t.Error("prose outside the fence was stripped")
	}
}

func TestHeadingSlugsFlagsUnterminatedFence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.md")
	src := "# Title\n\n```go\nunterminated\n"
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := headingSlugs(path); err == nil {
		t.Error("expected an error for an unterminated fence")
	}
}

func TestReferenceStyleLinksResolveAgainstDefinitions(t *testing.T) {
	src := "See [Reference][apiref] and the shortcut [apiref][], but not " +
		"[bad][missing].\n\n[apiref]: docs/reference/kernel.md\n"
	targets, undefined := refTargets(src)
	if len(targets) != 2 || targets[0] != "docs/reference/kernel.md" || targets[1] != "docs/reference/kernel.md" {
		t.Errorf("targets = %v, want two docs/reference/kernel.md entries", targets)
	}
	if len(undefined) != 1 || undefined[0] != "missing" {
		t.Errorf("undefined = %v, want [missing]", undefined)
	}
}

func TestStripInlineCodeHidesBracketPairsInSingleBacktickSpans(t *testing.T) {
	src := "`KV.Get` returns `map[string][]byte` values."
	out := stripInlineCode(src)
	if strings.Contains(out, "map[string][]byte") {
		t.Error("single-backtick code span was not stripped")
	}
}

func TestStripInlineCodeHandlesMultiBacktickSpans(t *testing.T) {
	src := "See ``[X][Y]`` here, not ``` [P][Q] ``` either."
	out := stripInlineCode(src)
	if strings.Contains(out, "[X][Y]") {
		t.Error("double-backtick code span was not stripped")
	}
	if strings.Contains(out, "[P][Q]") {
		t.Error("triple-backtick inline span was not stripped")
	}
}

func TestUnpairedBacktickDoesNotMaskALaterLine(t *testing.T) {
	src := "Prices are quoted in ` per unit.\n\n" +
		"See [the missing page](does-not-exist.md) for details.\n\n" +
		"The symbol ` closes nothing in particular.\n"
	out := stripInlineCode(src)
	matches := linkRe.FindAllStringSubmatch(out, -1)
	if len(matches) != 1 || matches[0][1] != "does-not-exist.md" {
		t.Errorf("matches = %v, want one link to does-not-exist.md", matches)
	}
}

func TestStripInlineCodeLeavesRealMarkdownLinksAlone(t *testing.T) {
	src := "See `map[string][]byte` and [glossary](docs/glossary.md)."
	out := stripInlineCode(src)
	matches := linkRe.FindAllStringSubmatch(out, -1)
	if len(matches) != 1 || matches[0][1] != "docs/glossary.md" {
		t.Errorf("matches = %v, want one link to docs/glossary.md", matches)
	}
}

func TestReferenceStyleLinksIgnoreCodeSpanBracketPairs(t *testing.T) {
	src := "`KV.Get` returns `map[string][]byte` values.\n"
	_, undefined := refTargets(stripInlineCode(src))
	if len(undefined) != 0 {
		t.Errorf("undefined = %v, want none — a code span's bracket pairs must not read as a reference link", undefined)
	}
}

func TestReferenceStyleLinkStillResolvesNextToCodeSpan(t *testing.T) {
	src := "See `map[string][]byte` in [the reference][apiref].\n\n" +
		"[apiref]: docs/reference/kernel.md\n"
	targets, undefined := refTargets(stripInlineCode(src))
	if len(undefined) != 0 {
		t.Errorf("undefined = %v, want none", undefined)
	}
	if len(targets) != 1 || targets[0] != "docs/reference/kernel.md" {
		t.Errorf("targets = %v, want [docs/reference/kernel.md]", targets)
	}
}
