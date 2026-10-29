package main

import (
	"fmt"
	"regexp"
	"strings"
)

// filter retient les noms qui correspondent à l'un de ses motifs : liste
// séparée par des virgules, jokers « * » (toute suite de caractères, « / »
// compris) et « ? » (un caractère).
type filter []*regexp.Regexp

func parseFilter(s string) (filter, error) {
	var f filter
	for _, p := range splitList(s) {
		re := regexp.QuoteMeta(p)
		re = strings.NewReplacer(`\*`, `.*`, `\?`, `.`).Replace(re)
		f = append(f, regexp.MustCompile(`^(?s:`+re+`)$`))
	}
	if len(f) == 0 {
		return nil, fmt.Errorf("empty filter %q", s)
	}
	return f, nil
}

// exact construit un filtre qui ne retient que les noms donnés.
func exact(names ...string) filter {
	var f filter
	for _, n := range names {
		f = append(f, regexp.MustCompile(`^`+regexp.QuoteMeta(n)+`$`))
	}
	return f
}

func (f filter) match(name string) bool {
	for _, re := range f {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// splitList découpe une liste séparée par des virgules, sans les blancs ni
// les éléments vides.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
