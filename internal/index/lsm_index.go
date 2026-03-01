package index

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"ViurtyStudent/internal/lsm"
)

type LSMIndex struct {
	mu            sync.RWMutex
	tree          *lsm.LSMTree
	memTerms      map[string]*Bitmap
	documents     map[uint32]*Document
	processor     *TextProcessor
	universe      *Bitmap
	nextDocID     uint32
	docIDMap      map[string]uint32
	reverseDocMap map[uint32]string
	baseDir       string
	docStorePath  string
	memByteSize   int
	maxMemSize    int
	postingSeq    uint64
}

const postingKeySeparator = "\x1f"

func NewLSMIndex(baseDir string, language string, sizeRatio int) (*LSMIndex, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	lsmDir := filepath.Join(baseDir, "lsm")
	tree, err := lsm.NewLSMTree(lsmDir, sizeRatio)
	if err != nil {
		return nil, err
	}

	idx := &LSMIndex{
		tree:          tree,
		memTerms:      make(map[string]*Bitmap),
		documents:     make(map[uint32]*Document),
		processor:     NewTextProcessor(language),
		universe:      NewBitmap(),
		nextDocID:     1,
		docIDMap:      make(map[string]uint32),
		reverseDocMap: make(map[uint32]string),
		baseDir:       baseDir,
		docStorePath:  filepath.Join(baseDir, "documents.json"),
		memByteSize:   0,
		maxMemSize:    10 * 1024 * 1024,
		postingSeq:    1,
	}

	idx.loadDocuments()

	return idx, nil
}

func (idx *LSMIndex) AddDocument(externalID string, title, content string) (uint32, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if oldDocID, exists := idx.docIDMap[externalID]; exists {
		if err := idx.markDocumentDeletedLocked(oldDocID); err != nil {
			return 0, err
		}
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
		bitmap, exists := idx.memTerms[term]
		if !exists {
			bitmap = NewBitmap()
			idx.memTerms[term] = bitmap
			idx.memByteSize += len(term) + 8
		}
		bitmap.Add(docID)
	}

	if idx.memByteSize >= idx.maxMemSize {
		if err := idx.flushMemTermsLocked(); err != nil {
			return docID, err
		}
	}

	return docID, nil
}

func (idx *LSMIndex) flushMemTermsLocked() error {
	if idx.postingSeq == 0 {
		idx.postingSeq = 1
	}

	for term, bitmap := range idx.memTerms {
		data, err := bitmap.Serialize()
		if err != nil {
			return err
		}
		encoded := base64.StdEncoding.EncodeToString(data)
		key := idx.makePostingKey(term, idx.postingSeq)
		if err := idx.tree.Insert(key, encoded); err != nil {
			return err
		}
	}

	idx.postingSeq++

	idx.memTerms = make(map[string]*Bitmap)
	idx.memByteSize = 0

	return idx.tree.Flush()
}

func (idx *LSMIndex) RemoveDocument(externalID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	docID, exists := idx.docIDMap[externalID]
	if !exists {
		return nil
	}

	return idx.markDocumentDeletedLocked(docID)
}

func (idx *LSMIndex) markDocumentDeletedLocked(docID uint32) error {
	idx.universe.Remove(docID)

	externalID := idx.reverseDocMap[docID]
	delete(idx.documents, docID)
	delete(idx.docIDMap, externalID)
	delete(idx.reverseDocMap, docID)

	return nil
}

func (idx *LSMIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if err := idx.flushMemTermsLocked(); err != nil {
		return err
	}

	return idx.saveDocumentsLocked()
}

func (idx *LSMIndex) GetDocument(docID uint32) (*Document, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	doc, ok := idx.documents[docID]
	return doc, ok
}

func (idx *LSMIndex) GetDocuments(docIDs []uint32) []*Document {
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

func (idx *LSMIndex) SearchTerm(term string) *Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	processedTerm := idx.processor.ProcessQuery(term)
	return idx.getTermBitmapLocked(processedTerm)
}

func (idx *LSMIndex) getTermBitmapLocked(term string) *Bitmap {
	result := NewBitmap()

	if memBitmap, ok := idx.memTerms[term]; ok {
		result = result.Or(memBitmap)
	}

	if value, found, err := idx.tree.Get(term); err == nil && found && value != "" {
		result = idx.mergeEncodedBitmap(result, value)
	}

	fromKey, toKey := idx.postingRange(term)
	if records, err := idx.tree.RangeScan(fromKey, toKey); err == nil {
		for _, record := range records {
			if record.Value == "" {
				continue
			}
			result = idx.mergeEncodedBitmap(result, record.Value)
		}
	}

	result = result.And(idx.universe)

	return result
}

func (idx *LSMIndex) mergeEncodedBitmap(base *Bitmap, encoded string) *Bitmap {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return base
	}

	diskBitmap, err := Deserialize(data)
	if err != nil {
		return base
	}

	return base.Or(diskBitmap)
}

func (idx *LSMIndex) makePostingKey(term string, seq uint64) string {
	return fmt.Sprintf("%s%s%020d", term, postingKeySeparator, seq)
}

func (idx *LSMIndex) postingRange(term string) (string, string) {
	prefix := term + postingKeySeparator
	return prefix, prefix + "\xff"
}

func (idx *LSMIndex) GetTermBitmap(term string) *Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.getTermBitmapLocked(term)
}

func (idx *LSMIndex) GetUniverse() *Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.universe.Clone()
}

func (idx *LSMIndex) GetProcessor() *TextProcessor {
	return idx.processor
}

func (idx *LSMIndex) DocumentCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.documents)
}

func (idx *LSMIndex) saveDocumentsLocked() error {
	data := struct {
		Documents     map[uint32]*Document `json:"documents"`
		NextDocID     uint32               `json:"next_doc_id"`
		DocIDMap      map[string]uint32    `json:"doc_id_map"`
		ReverseDocMap map[uint32]string    `json:"reverse_doc_map"`
		Universe      []byte               `json:"universe"`
		PostingSeq    uint64               `json:"posting_seq"`
	}{
		Documents:     idx.documents,
		NextDocID:     idx.nextDocID,
		DocIDMap:      idx.docIDMap,
		ReverseDocMap: idx.reverseDocMap,
		PostingSeq:    idx.postingSeq,
	}

	universeData, err := idx.universe.Serialize()
	if err != nil {
		return err
	}
	data.Universe = universeData

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(idx.docStorePath, jsonData, 0o644)
}

func (idx *LSMIndex) loadDocuments() error {
	data, err := os.ReadFile(idx.docStorePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var stored struct {
		Documents     map[uint32]*Document `json:"documents"`
		NextDocID     uint32               `json:"next_doc_id"`
		DocIDMap      map[string]uint32    `json:"doc_id_map"`
		ReverseDocMap map[uint32]string    `json:"reverse_doc_map"`
		Universe      []byte               `json:"universe"`
		PostingSeq    uint64               `json:"posting_seq"`
	}

	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}

	idx.documents = stored.Documents
	idx.nextDocID = stored.NextDocID
	idx.docIDMap = stored.DocIDMap
	idx.reverseDocMap = stored.ReverseDocMap
	idx.postingSeq = stored.PostingSeq

	if idx.documents == nil {
		idx.documents = make(map[uint32]*Document)
	}
	if idx.docIDMap == nil {
		idx.docIDMap = make(map[string]uint32)
	}
	if idx.reverseDocMap == nil {
		idx.reverseDocMap = make(map[uint32]string)
	}
	if idx.nextDocID == 0 {
		idx.nextDocID = 1
	}
	if idx.postingSeq == 0 {
		idx.postingSeq = 1
	}

	if len(stored.Universe) > 0 {
		universe, err := Deserialize(stored.Universe)
		if err != nil {
			return err
		}
		idx.universe = universe
	}

	return nil
}

func (idx *LSMIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if err := idx.flushMemTermsLocked(); err != nil {
		return err
	}

	return idx.saveDocumentsLocked()
}

func SearchLSM(idx *LSMIndex, queryStr string) ([]*Document, error) {
	parser := NewQueryParser(idx.GetProcessor())
	query, err := parser.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	adapter := &lsmIndexAdapter{idx}
	result := evaluateQueryOnAdapter(query, adapter)
	docIDs := result.ToArray()
	return idx.GetDocuments(docIDs), nil
}

type lsmIndexAdapter struct {
	idx *LSMIndex
}

func evaluateQueryOnAdapter(q Query, adapter *lsmIndexAdapter) *Bitmap {
	switch v := q.(type) {
	case *TermQuery:
		return adapter.idx.SearchTerm(v.Term)
	case *AndQuery:
		left := evaluateQueryOnAdapter(v.Left, adapter)
		right := evaluateQueryOnAdapter(v.Right, adapter)
		return left.And(right)
	case *OrQuery:
		left := evaluateQueryOnAdapter(v.Left, adapter)
		right := evaluateQueryOnAdapter(v.Right, adapter)
		return left.Or(right)
	case *NotQuery:
		inner := evaluateQueryOnAdapter(v.Inner, adapter)
		universe := adapter.idx.GetUniverse()
		return inner.Not(universe)
	default:
		return NewBitmap()
	}
}

type IndexStats struct {
	DocumentCount int    `json:"document_count"`
	MemTermCount  int    `json:"mem_term_count"`
	MemByteSize   int    `json:"mem_byte_size"`
	BaseDir       string `json:"base_dir"`
}

func (idx *LSMIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexStats{
		DocumentCount: len(idx.documents),
		MemTermCount:  len(idx.memTerms),
		MemByteSize:   idx.memByteSize,
		BaseDir:       idx.baseDir,
	}
}
