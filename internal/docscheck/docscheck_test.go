package docscheck

import (
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
	t.Logf("unchecked go blocks: %d", unchecked)
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

func headingSlugs(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	inFence := false
	for _, line := range strings.Split(string(raw), "\n") {
		if leadingFence(line) != "" {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if m := atxHeadingRe.FindStringSubmatch(line); m != nil {
			text := strings.TrimRight(strings.TrimSpace(m[2]), "# ")
			out = append(out, slugify(text))
		}
	}
	return out, nil
}

func slugify(heading string) string {
	lower := strings.ToLower(heading)
	var b strings.Builder
	for _, r := range lower {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == ' ', r == '-':
			b.WriteRune(r)
		}
	}
	return strings.ReplaceAll(b.String(), " ", "-")
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

func stripFencedCode(src string) string {
	lines := strings.Split(src, "\n")
	inFence := false
	for i, line := range lines {
		if leadingFence(line) != "" {
			inFence = !inFence
			lines[i] = ""
			continue
		}
		if inFence {
			lines[i] = ""
		}
	}
	return strings.Join(lines, "\n")
}

func TestInternalMarkdownLinksResolve(t *testing.T) {
	root := mustRoot(t)
	files, err := MarkdownFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	linkRe := regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)
	slugCache := map[string][]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		prose := stripFencedCode(string(raw))
		for _, m := range linkRe.FindAllStringSubmatch(prose, -1) {
			target := m[1]
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			fragment := ""
			if i := strings.IndexByte(target, '#'); i >= 0 {
				fragment = target[i+1:]
				target = target[:i]
			}
			if target == "" {
				continue
			}
			resolved := filepath.Join(filepath.Dir(f), target)
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s: broken link %q", relTo(root, f), m[1])
				continue
			}
			if fragment == "" || !strings.HasSuffix(resolved, ".md") {
				continue
			}
			slugs, ok := slugCache[resolved]
			if !ok {
				slugs, err = headingSlugs(resolved)
				if err != nil {
					t.Fatal(err)
				}
				slugCache[resolved] = slugs
			}
			found := false
			for _, s := range slugs {
				if s == fragment {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: link %q has broken fragment %q; closest heading slug is %q",
					relTo(root, f), m[1], fragment, closestSlug(slugs, fragment))
			}
		}
	}
}
