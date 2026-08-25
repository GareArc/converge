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
