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
