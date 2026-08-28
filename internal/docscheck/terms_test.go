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
	if got := FirstProseUse(masked, "shelved"); got == -1 {
		t.Error("prose use of shelved was masked away")
	}
}

func TestFirstGlossFindsBoldDefinition(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "warm.md"))
	if err != nil {
		t.Fatal(err)
	}
	gloss := FirstGloss(string(src), "shelved")
	if gloss == -1 {
		t.Fatal("bold definition not found")
	}
	use := FirstProseUse(MaskNonProse(string(src)), "shelved")
	if gloss > use {
		t.Errorf("gloss at %d comes after first use at %d", gloss, use)
	}
}

func TestColdPageHasNoGloss(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("testdata", "cold.md"))
	if err != nil {
		t.Fatal(err)
	}
	if FirstGloss(string(src), "shelved") != -1 {
		t.Error("found a gloss on a page that has none")
	}
	if FirstProseUse(MaskNonProse(string(src)), "shelves") == -1 {
		t.Error("expected a prose use on the cold page")
	}
}

func TestMaskNonProseUnpairedBacktickDoesNotMaskALaterLine(t *testing.T) {
	src := "Prices are quoted in ` per unit.\n\n" +
		"The job shelves the message after too many failures.\n\n" +
		"The symbol ` closes nothing in particular.\n"
	masked := MaskNonProse(src)
	if FirstProseUse(masked, "shelves") == -1 {
		t.Error("a cold term one line below an unpaired backtick was masked away")
	}
}

func TestMaskNonProseStillHidesASingleLineCodeSpan(t *testing.T) {
	masked := MaskNonProse("The `shelves` identifier is code, not prose.\n")
	if FirstProseUse(masked, "shelves") != -1 {
		t.Error("a single-line code span was left unmasked")
	}
}

func TestSearchAliasesSplitsParentheticalAndPlain(t *testing.T) {
	cases := map[string][]string{
		"Dead-letter (DLQ)": {"Dead-letter", "DLQ"},
		"Run mode":          {"Run mode"},
	}
	for in, want := range cases {
		got := SearchAliases(in)
		if len(got) != len(want) {
			t.Fatalf("SearchAliases(%q) = %v, want %v", in, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("SearchAliases(%q)[%d] = %q, want %q", in, i, got[i], want[i])
			}
		}
	}
}

func TestFirstGlossGlossaryLinkRequiresWordBoundary(t *testing.T) {
	falsePositive := "See the [Release process](docs/glossary.md) for details."
	if FirstGloss(falsePositive, "Lease") != -1 {
		t.Error("glossary link pattern matched \"Lease\" inside \"Release\"")
	}
	real := "See the [Lease](docs/glossary.md) definition."
	if FirstGloss(real, "Lease") == -1 {
		t.Error("glossary link pattern did not match a real Lease link")
	}
}

func TestUnclosedLinkTargetDoesNotMaskALaterLine(t *testing.T) {
	src := "See [the reference](../reference/kernel.md for details.\n\n" +
		"The job shelves the message after too many failures.\n\n" +
		"Rate limiting (process-local) is configured separately.\n"
	masked := MaskNonProse(src)
	if FirstProseUse(masked, "shelves") == -1 {
		t.Error("a cold term below a link missing its closing paren was masked away")
	}
}

func TestMaskNonProseStillHidesALinkTargetOnItsOwnLine(t *testing.T) {
	masked := MaskNonProse("See [the reference](../reference/shelves.md) for details.\n")
	if FirstProseUse(masked, "shelves") != -1 {
		t.Error("a link target on a single line was left unmasked")
	}
}
