package docscheck

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var skipDirs = map[string]bool{
	"node_modules": true,
	"superpowers":  true,
	"testdata":     true,
	"website":      true,
}

func skipDir(name string) bool {
	return strings.HasPrefix(name, ".") || skipDirs[name]
}

func RepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("docscheck: go.work not found in any parent directory")
		}
		dir = parent
	}
}

func MarkdownFiles(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && skipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".md") {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func BarPages(root string) ([]string, error) {
	all, err := MarkdownFiles(root)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, p := range all {
		r, err := filepath.Rel(root, p)
		if err != nil {
			return nil, err
		}
		r = filepath.ToSlash(r)
		if r == "README.md" || strings.HasPrefix(r, "docs/guide/") || strings.HasPrefix(r, "docs/cookbook/") {
			out = append(out, p)
		}
	}
	return out, nil
}
