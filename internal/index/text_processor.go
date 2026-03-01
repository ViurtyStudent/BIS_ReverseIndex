package index

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/kljensen/snowball"
)

type TextProcessor struct {
	stopWords map[string]struct{}
	language  string
}

func NewTextProcessor(language string) *TextProcessor {
	tp := &TextProcessor{
		stopWords: make(map[string]struct{}),
		language:  language,
	}

	switch language {
	case "russian":
		tp.loadRussianStopWords()
	case "english":
		tp.loadEnglishStopWords()
	default:
		tp.loadRussianStopWords()
		tp.loadEnglishStopWords()
	}

	return tp
}

func (tp *TextProcessor) loadRussianStopWords() {
	words := []string{
		"и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то", "все", "она",
		"так", "его", "но", "да", "ты", "к", "у", "же", "вы", "за", "бы", "по", "только", "её",
		"мне", "было", "вот", "от", "меня", "еще", "нет", "о", "из", "ему", "теперь", "когда",
		"уже", "вам", "ни", "быть", "был", "им", "до", "если", "или", "ней", "для",
		"была", "были", "этот", "эта", "эти", "это", "который", "которая", "которые", "которое",
		"свой", "своя", "свои", "своё", "чтобы", "при", "между", "чем", "над", "под", "после",
	}
	for _, w := range words {
		tp.stopWords[w] = struct{}{}
	}
}

func (tp *TextProcessor) loadEnglishStopWords() {
	words := []string{
		"a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
		"by", "from", "as", "is", "was", "are", "were", "been", "be", "have", "has", "had",
		"do", "does", "did", "will", "would", "could", "should", "may", "might", "must",
		"shall", "can", "need", "dare", "ought", "used", "it", "its", "this", "that", "these",
		"those", "i", "you", "he", "she", "we", "they", "what", "which", "who", "whom",
		"when", "where", "why", "how", "all", "each", "every", "both", "few", "more", "most",
		"other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
		"too", "very", "just", "also", "now", "here", "there", "then", "once", "if", "about",
	}
	for _, w := range words {
		tp.stopWords[w] = struct{}{}
	}
}

var nonAlphaRegex = regexp.MustCompile(`[^\p{L}\p{N}]+`)

func (tp *TextProcessor) Tokenize(text string) []string {
	text = strings.ToLower(text)
	parts := nonAlphaRegex.Split(text, -1)

	tokens := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			tokens = append(tokens, p)
		}
	}
	return tokens
}

func (tp *TextProcessor) RemoveStopWords(tokens []string) []string {
	result := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if _, isStop := tp.stopWords[t]; !isStop {
			result = append(result, t)
		}
	}
	return result
}

func (tp *TextProcessor) Stem(token string) string {
	lang := tp.detectLanguage(token)
	if lang == "" {
		return token
	}

	stemmed, err := snowball.Stem(token, lang, true)
	if err != nil {
		return token
	}
	return stemmed
}

func (tp *TextProcessor) StemTokens(tokens []string) []string {
	result := make([]string, 0, len(tokens))
	for _, t := range tokens {
		result = append(result, tp.Stem(t))
	}
	return result
}

func (tp *TextProcessor) detectLanguage(token string) string {
	for _, r := range token {
		if unicode.Is(unicode.Cyrillic, r) {
			return "russian"
		}
		if unicode.Is(unicode.Latin, r) {
			return "english"
		}
	}
	return ""
}

func (tp *TextProcessor) Process(text string) []string {
	tokens := tp.Tokenize(text)
	tokens = tp.RemoveStopWords(tokens)
	tokens = tp.StemTokens(tokens)

	seen := make(map[string]struct{})
	result := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if _, ok := seen[t]; !ok && t != "" {
			seen[t] = struct{}{}
			result = append(result, t)
		}
	}
	return result
}

func (tp *TextProcessor) ProcessQuery(term string) string {
	term = strings.ToLower(strings.TrimSpace(term))
	return tp.Stem(term)
}

func (tp *TextProcessor) IsStopWord(word string) bool {
	_, ok := tp.stopWords[strings.ToLower(word)]
	return ok
}
