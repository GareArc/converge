package docscheck

import (
	"bufio"
	"os"
	"strings"
)

type Block struct {
	File  string
	Line  int
	Lang  string
	Attrs map[string]string
	Body  string
}

func ParseBlocks(path string) ([]Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var blocks []Block
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var open *Block
	var fence string
	var body []string
	lineNo := 0

	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if open == nil {
			f := leadingFence(line)
			if f == "" {
				continue
			}
			lang, attrs := parseInfo(strings.TrimSpace(line[len(f):]))
			if lang == "" {
				continue
			}
			open = &Block{File: path, Line: lineNo, Lang: lang, Attrs: attrs}
			fence = f
			body = nil
			continue
		}
		if isClosingFence(line, fence) {
			if len(body) > 0 {
				open.Body = strings.Join(body, "\n") + "\n"
			}
			blocks = append(blocks, *open)
			open = nil
			continue
		}
		body = append(body, line)
	}
	return blocks, sc.Err()
}

func leadingFence(line string) string {
	n := 0
	for n < len(line) && line[n] == '`' {
		n++
	}
	if n < 3 {
		return ""
	}
	return line[:n]
}

func isClosingFence(line, fence string) bool {
	t := strings.TrimSpace(line)
	return len(t) >= len(fence) && strings.Trim(t, "`") == ""
}

func parseInfo(info string) (string, map[string]string) {
	attrs := map[string]string{}
	fields := strings.Fields(info)
	if len(fields) == 0 {
		return "", attrs
	}
	for _, f := range fields[1:] {
		if k, v, ok := strings.Cut(f, "="); ok {
			attrs[k] = v
		}
	}
	return fields[0], attrs
}
