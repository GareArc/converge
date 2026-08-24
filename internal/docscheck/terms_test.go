package docscheck

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeTermStripsParentheticalAndCase(t *testing.T) {
	cases := map[string]string{
		"Dead-letter (DLQ)": "dead-letter",
		"Run mode":          "run mode",
		"ID":                "id",
	}
	for in, want := range cases {
		if got := NormalizeTerm(in); got != want {
			t.Errorf("NormalizeTerm(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMaskNonProseHidesCodeAndTitle(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "warm.md"))
	if err != nil {
		t.Fatal(err)
	}
	masked := MaskNonProse(string(src))
	if len(masked) != len(string(src)) {
		t.Fatalf("mask changed length: %d vs %d", len(masked), len(string(src)))
	}
	if FirstProseUse(masked, "warm") != -1 {
		t.Error("H1 title was not masked")
	}
	if got := FirstProseUse(masked, "parked"); got == -1 {
		t.Error("prose use of parked was masked away")
	}
}

func TestFirstGlossFindsBoldDefinition(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "warm.md"))
	if err != nil {
		t.Fatal(err)
	}
	gloss := FirstGloss(string(src), "parked")
	if gloss == -1 {
		t.Fatal("bold definition not found")
	}
	use := FirstProseUse(MaskNonProse(string(src)), "parked")
	if gloss > use {
		t.Errorf("gloss at %d comes after first use at %d", gloss, use)
	}
}

func TestColdPageHasNoGloss(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "cold.md"))
	if err != nil {
		t.Fatal(err)
	}
	if FirstGloss(string(src), "parked") != -1 {
		t.Error("found a gloss on a page that has none")
	}
	if FirstProseUse(MaskNonProse(string(src)), "parks") == -1 {
		t.Error("expected a prose use on the cold page")
	}
}
