package index

import (
	"strings"
	"sync"
)

type Document struct {
	ID      uint32 `json:"id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

type InvertedIndex struct {
	mu            sync.RWMutex
	terms         map[string]*Bitmap
	kGramIndex    map[string]map[string]struct{}
	termSet       map[string]struct{}
	kGramSize     int
	documents     map[uint32]*Document
	processor     *TextProcessor
	universe      *Bitmap
	nextDocID     uint32
	docIDMap      map[string]uint32
	reverseDocMap map[uint32]string
}

func NewInvertedIndex(language string) *InvertedIndex {
	return &InvertedIndex{
		terms:         make(map[string]*Bitmap),
		kGramIndex:    make(map[string]map[string]struct{}),
		termSet:       make(map[string]struct{}),
		kGramSize:     defaultKGramSize,
		documents:     make(map[uint32]*Document),
		processor:     NewTextProcessor(language),
		universe:      NewBitmap(),
		nextDocID:     1,
		docIDMap:      make(map[string]uint32),
		reverseDocMap: make(map[uint32]string),
	}
}

func (idx *InvertedIndex) AddDocument(externalID string, title, content string) uint32 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if docID, exists := idx.docIDMap[externalID]; exists {
		idx.removeDocumentLocked(docID)
	}

	docID := idx.nextDocID
	idx.nextDocID++

	doc := &Document{
		ID:      docID,
		Title:   title,
		Content: content,
	}
	idx.documents[docID] = doc
	idx.universe.Add(docID)
	idx.docIDMap[externalID] = docID
	idx.reverseDocMap[docID] = externalID

	fullText := title + " " + content
	terms := idx.processor.Process(fullText)

	for _, term := range terms {
		if _, ok := idx.terms[term]; !ok {
			idx.terms[term] = NewBitmap()
			idx.termSet[term] = struct{}{}
			idx.addTermToKGramIndexLocked(term)
		}
		idx.terms[term].Add(docID)
	}

	return docID
}

func (idx *InvertedIndex) RemoveDocument(externalID string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	docID, exists := idx.docIDMap[externalID]
	if !exists {
		return false
	}

	idx.removeDocumentLocked(docID)
	delete(idx.docIDMap, externalID)
	delete(idx.reverseDocMap, docID)
	return true
}

func (idx *InvertedIndex) removeDocumentLocked(docID uint32) {
	idx.universe.Remove(docID)

	for term, bitmap := range idx.terms {
		bitmap.Remove(docID)
		if bitmap.IsEmpty() {
			idx.removeTermFromKGramIndexLocked(term)
			delete(idx.termSet, term)
			delete(idx.terms, term)
		}
	}

	delete(idx.documents, docID)
}

func (idx *InvertedIndex) GetDocument(docID uint32) (*Document, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	doc, ok := idx.documents[docID]
	return doc, ok
}

func (idx *InvertedIndex) GetDocuments(docIDs []uint32) []*Document {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	docs := make([]*Document, 0, len(docIDs))
	for _, id := range docIDs {
		if doc, ok := idx.documents[id]; ok {
			docs = append(docs, doc)
		}
	}
	return docs
}

func (idx *InvertedIndex) SearchTerm(term string) *Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	processedTerm := idx.processor.ProcessQuery(term)
	if bitmap, ok := idx.terms[processedTerm]; ok {
		return bitmap.Clone()
	}
	return NewBitmap()
}

func (idx *InvertedIndex) GetTermBitmap(term string) *Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if bitmap, ok := idx.terms[term]; ok {
		return bitmap.Clone()
	}
	return NewBitmap()
}

func (idx *InvertedIndex) SearchPrefix(prefix string) *Bitmap {
	processedPrefix := idx.processor.ProcessQuery(prefix)
	if processedPrefix == "" {
		return NewBitmap()
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := NewBitmap()
	for term, bitmap := range idx.terms {
		if strings.HasPrefix(term, processedPrefix) {
			result = result.Or(bitmap)
		}
	}

	return result
}

func (idx *InvertedIndex) SearchWildcard(pattern string) *Bitmap {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	if pattern == "" {
		return NewBitmap()
	}

	if !strings.Contains(pattern, "*") {
		return idx.SearchTerm(pattern)
	}

	re, err := wildcardToRegexp(pattern)
	if err != nil {
		return NewBitmap()
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	queryKGrams := buildPatternKGrams(pattern, idx.kGramSize)

	candidates := make(map[string]struct{})
	if len(queryKGrams) == 0 {
		for term := range idx.termSet {
			candidates[term] = struct{}{}
		}
	} else {
		initialized := false
		for _, gram := range queryKGrams {
			termsForGram, ok := idx.kGramIndex[gram]
			if !ok {
				return NewBitmap()
			}

			if !initialized {
				for term := range termsForGram {
					candidates[term] = struct{}{}
				}
				initialized = true
				continue
			}

			for term := range candidates {
				if _, ok := termsForGram[term]; !ok {
					delete(candidates, term)
				}
			}

			if len(candidates) == 0 {
				return NewBitmap()
			}
		}
	}

	result := NewBitmap()
	for term := range candidates {
		if re.MatchString(term) {
			if bitmap, ok := idx.terms[term]; ok {
				result = result.Or(bitmap)
			}
		}
	}

	return result
}

func (idx *InvertedIndex) addTermToKGramIndexLocked(term string) {
	for _, gram := range buildTermKGrams(term, idx.kGramSize) {
		if _, ok := idx.kGramIndex[gram]; !ok {
			idx.kGramIndex[gram] = make(map[string]struct{})
		}
		idx.kGramIndex[gram][term] = struct{}{}
	}
}

func (idx *InvertedIndex) removeTermFromKGramIndexLocked(term string) {
	for _, gram := range buildTermKGrams(term, idx.kGramSize) {
		if termsForGram, ok := idx.kGramIndex[gram]; ok {
			delete(termsForGram, term)
			if len(termsForGram) == 0 {
				delete(idx.kGramIndex, gram)
			}
		}
	}
}

func (idx *InvertedIndex) GetUniverse() *Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.universe.Clone()
}

func (idx *InvertedIndex) GetProcessor() *TextProcessor {
	return idx.processor
}

func (idx *InvertedIndex) DocumentCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.documents)
}

func (idx *InvertedIndex) TermCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.terms)
}
