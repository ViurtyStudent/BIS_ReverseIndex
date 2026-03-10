package index

import (
	"fmt"
	"strings"
	"unicode"
)

type Query interface {
	Evaluate(idx *InvertedIndex) *Bitmap
	String() string
}

type TermQuery struct {
	Term string
}

func (q *TermQuery) Evaluate(idx *InvertedIndex) *Bitmap {
	return idx.SearchTerm(q.Term)
}

func (q *TermQuery) String() string {
	return q.Term
}

type PrefixQuery struct {
	Prefix string
}

func (q *PrefixQuery) Evaluate(idx *InvertedIndex) *Bitmap {
	return idx.SearchPrefix(q.Prefix)
}

func (q *PrefixQuery) String() string {
	return q.Prefix + "*"
}

type WildcardQuery struct {
	Pattern string
}

func (q *WildcardQuery) Evaluate(idx *InvertedIndex) *Bitmap {
	return idx.SearchWildcard(q.Pattern)
}

func (q *WildcardQuery) String() string {
	return q.Pattern
}

type AndQuery struct {
	Left  Query
	Right Query
}

func (q *AndQuery) Evaluate(idx *InvertedIndex) *Bitmap {
	left := q.Left.Evaluate(idx)
	right := q.Right.Evaluate(idx)
	return left.And(right)
}

func (q *AndQuery) String() string {
	return fmt.Sprintf("(%s AND %s)", q.Left, q.Right)
}

type OrQuery struct {
	Left  Query
	Right Query
}

func (q *OrQuery) Evaluate(idx *InvertedIndex) *Bitmap {
	left := q.Left.Evaluate(idx)
	right := q.Right.Evaluate(idx)
	return left.Or(right)
}

func (q *OrQuery) String() string {
	return fmt.Sprintf("(%s OR %s)", q.Left, q.Right)
}

type NotQuery struct {
	Inner Query
}

func (q *NotQuery) Evaluate(idx *InvertedIndex) *Bitmap {
	inner := q.Inner.Evaluate(idx)
	universe := idx.GetUniverse()
	return inner.Not(universe)
}

func (q *NotQuery) String() string {
	return fmt.Sprintf("NOT %s", q.Inner)
}

type QueryParser struct {
	processor *TextProcessor
	tokens    []string
	pos       int
}

func NewQueryParser(processor *TextProcessor) *QueryParser {
	return &QueryParser{processor: processor}
}

func (p *QueryParser) Parse(query string) (Query, error) {
	p.tokens = p.tokenize(query)
	p.pos = 0

	if len(p.tokens) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	q, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.pos < len(p.tokens) {
		return nil, fmt.Errorf("unexpected token at position %d: %s", p.pos, p.tokens[p.pos])
	}

	return q, nil
}

func (p *QueryParser) tokenize(query string) []string {
	var tokens []string
	var current strings.Builder

	for _, r := range query {
		if unicode.IsSpace(r) {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		} else if r == '(' || r == ')' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			tokens = append(tokens, string(r))
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

func (p *QueryParser) parseOr() (Query, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "OR" {
		p.pos++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrQuery{Left: left, Right: right}
	}

	return left, nil
}

func (p *QueryParser) parseAnd() (Query, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "AND" {
		p.pos++
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &AndQuery{Left: left, Right: right}
	}

	return left, nil
}

func (p *QueryParser) parseNot() (Query, error) {
	if p.pos < len(p.tokens) && strings.ToUpper(p.tokens[p.pos]) == "NOT" {
		p.pos++
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotQuery{Inner: inner}, nil
	}

	return p.parsePrimary()
}

func (p *QueryParser) parsePrimary() (Query, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("unexpected end of query")
	}

	token := p.tokens[p.pos]

	if token == "(" {
		p.pos++
		q, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.pos >= len(p.tokens) || p.tokens[p.pos] != ")" {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		p.pos++
		return q, nil
	}

	p.pos++

	if strings.Contains(token, "*") {
		if strings.Count(token, "*") == 1 && strings.HasSuffix(token, "*") && len(token) > 1 {
			return &PrefixQuery{Prefix: token[:len(token)-1]}, nil
		}
		return &WildcardQuery{Pattern: token}, nil
	}

	return &TermQuery{Term: token}, nil
}

func Search(idx *InvertedIndex, queryStr string) ([]*Document, error) {
	parser := NewQueryParser(idx.GetProcessor())
	query, err := parser.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	result := query.Evaluate(idx)
	docIDs := result.ToArray()
	return idx.GetDocuments(docIDs), nil
}
