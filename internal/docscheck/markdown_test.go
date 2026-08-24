package docscheck

import (
	"path/filepath"
	"testing"
)

func TestParseBlocksReadsLangAttrsAndBody(t *testing.T) {
	blocks, err := ParseBlocks(filepath.Join("testdata", "sample.md"))
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 {
		t.Fatalf("got %d blocks, want 3", len(blocks))
	}
	if blocks[0].Lang != "go" {
		t.Errorf("block 0 lang = %q, want go", blocks[0].Lang)
	}
	if got := blocks[0].Attrs["title"]; got != "examples/guide/01-first-job/main.go" {
		t.Errorf("block 0 title = %q", got)
	}
	if blocks[0].Body != "package main\n" {
		t.Errorf("block 0 body = %q", blocks[0].Body)
	}
	if len(blocks[1].Attrs) != 0 {
		t.Errorf("block 1 attrs = %v, want empty", blocks[1].Attrs)
	}
	if blocks[2].Lang != "sh" {
		t.Errorf("block 2 lang = %q, want sh", blocks[2].Lang)
	}
	if blocks[0].Line >= blocks[1].Line {
		t.Errorf("block lines not increasing: %d, %d", blocks[0].Line, blocks[1].Line)
	}
}

func TestParseBlocksIgnoresIndentedFences(t *testing.T) {
	blocks, err := ParseBlocks(filepath.Join("testdata", "sample.md"))
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 {
		t.Fatalf("got %d blocks, want 3 (indented fence must not be parsed)", len(blocks))
	}
	for _, b := range blocks {
		if b.Attrs["title"] == "examples/guide/00-illustrative/main.go" {
			t.Errorf("indented fence was parsed as a block: %+v", b)
		}
		if b.Body == "package illustrative\n" {
			t.Errorf("indented fence body leaked into a parsed block: %+v", b)
		}
	}
}
