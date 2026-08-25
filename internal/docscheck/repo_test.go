package docscheck

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMarkdownFilesSkipsDotDirectories(t *testing.T) {
	root := t.TempDir()
	dotDir := filepath.Join(root, ".superpowers")
	if err := os.MkdirAll(dotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dotDir, "hidden.md"), []byte("# hidden\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "visible.md"), []byte("# visible\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, err := MarkdownFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || filepath.Base(files[0]) != "visible.md" {
		t.Errorf("MarkdownFiles(%q) = %v, want only visible.md", root, files)
	}
}

func TestMarkdownFilesWalksARootWhoseOwnNameIsSkippable(t *testing.T) {
	for _, name := range []string{".hidden", "website", "node_modules", "testdata", "superpowers"} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), name)
			if err := os.MkdirAll(filepath.Join(root, "docs"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "docs", "page.md"), []byte("# page\n"), 0o644); err != nil {
				t.Fatal(err)
			}

			files, err := MarkdownFiles(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 {
				t.Errorf("MarkdownFiles(%q) = %v, want the one page below it", root, files)
			}
		})
	}
}
