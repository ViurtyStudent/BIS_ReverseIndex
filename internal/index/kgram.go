package index

import (
	"regexp"
	"strings"
)

const defaultKGramSize = 3

func buildTermKGrams(term string, k int) []string {
	if term == "" || k <= 0 {
		return nil
	}

	padded := "$" + term + "$"
	if len(padded) < k {
		return []string{padded}
	}

	grams := make([]string, 0, len(padded)-k+1)
	for i := 0; i <= len(padded)-k; i++ {
		grams = append(grams, padded[i:i+k])
	}

	return grams
}

func buildPatternKGrams(pattern string, k int) []string {
	if pattern == "" || k <= 0 {
		return nil
	}

	parts := strings.Split(pattern, "*")
	grams := make([]string, 0)
	seen := make(map[string]struct{})

	for i, part := range parts {
		if part == "" {
			continue
		}

		fragment := part
		if i == 0 {
			fragment = "$" + fragment
		}
		if i == len(parts)-1 {
			fragment += "$"
		}

		if len(fragment) < k {
			continue
		}

		for j := 0; j <= len(fragment)-k; j++ {
			gram := fragment[j : j+k]
			if _, ok := seen[gram]; ok {
				continue
			}
			seen[gram] = struct{}{}
			grams = append(grams, gram)
		}
	}

	return grams
}

func wildcardToRegexp(pattern string) (*regexp.Regexp, error) {
	escaped := regexp.QuoteMeta(pattern)
	regexPattern := "^" + strings.ReplaceAll(escaped, "\\*", ".*") + "$"
	return regexp.Compile(regexPattern)
}
