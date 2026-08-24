package docscheck

import (
	"fmt"
	"os"
	"path/filepath"
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
	if os.Getenv("DOCSCHECK_STRICT") == "" {
		t.Skip("set DOCSCHECK_STRICT=1 to enforce; enabled permanently in task 17")
	}
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
	if os.Getenv("DOCSCHECK_STRICT") == "" {
		t.Skip("set DOCSCHECK_STRICT=1 to enforce; enabled permanently in task 17")
	}
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
