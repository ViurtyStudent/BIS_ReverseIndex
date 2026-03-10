package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"ViurtyStudent/internal/index"
)

// ==================== Text Processor Tests ====================

func TestTextProcessor_Tokenize(t *testing.T) {
	tp := index.NewTextProcessor("english")

	tests := []struct {
		input    string
		expected []string
	}{
		{"Hello World", []string{"hello", "world"}},
		{"Hello, World!", []string{"hello", "world"}},
		{"one-two-three", []string{"one", "two", "three"}},
		{"   spaces   ", []string{"spaces"}},
		{"UPPERCASE", []string{"uppercase"}},
		{"mix123numbers", []string{"mix123numbers"}},
	}

	for _, tt := range tests {
		result := tp.Tokenize(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("Tokenize(%q): got %v, want %v", tt.input, result, tt.expected)
			continue
		}
		for i := range result {
			if result[i] != tt.expected[i] {
				t.Errorf("Tokenize(%q)[%d]: got %q, want %q", tt.input, i, result[i], tt.expected[i])
			}
		}
	}
}

func TestTextProcessor_StopWords(t *testing.T) {
	tp := index.NewTextProcessor("english")

	tokens := []string{"the", "cat", "is", "on", "the", "mat"}
	filtered := tp.RemoveStopWords(tokens)

	expected := []string{"cat", "mat"}
	if len(filtered) != len(expected) {
		t.Errorf("RemoveStopWords: got %v, want %v", filtered, expected)
		return
	}
	for i := range filtered {
		if filtered[i] != expected[i] {
			t.Errorf("RemoveStopWords[%d]: got %q, want %q", i, filtered[i], expected[i])
		}
	}
}

func TestTextProcessor_RussianStopWords(t *testing.T) {
	tp := index.NewTextProcessor("russian")

	if !tp.IsStopWord("и") {
		t.Error("Expected 'и' to be a stop word")
	}
	if !tp.IsStopWord("в") {
		t.Error("Expected 'в' to be a stop word")
	}
	if tp.IsStopWord("кот") {
		t.Error("Expected 'кот' NOT to be a stop word")
	}
}

func TestTextProcessor_Stemming(t *testing.T) {
	tp := index.NewTextProcessor("english")

	tests := []struct {
		input    string
		expected string
	}{
		{"running", "run"},
		{"cats", "cat"},
		{"better", "better"},
	}

	for _, tt := range tests {
		result := tp.Stem(tt.input)
		if result != tt.expected {
			t.Errorf("Stem(%q): got %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestTextProcessor_Process(t *testing.T) {
	tp := index.NewTextProcessor("english")

	text := "The cats are running in the garden"
	result := tp.Process(text)

	expected := map[string]bool{"cat": true, "run": true, "garden": true}
	for _, term := range result {
		if !expected[term] {
			t.Errorf("Unexpected term in result: %q", term)
		}
	}
}

// ==================== Bitmap Tests ====================

func TestBitmap_Basic(t *testing.T) {
	b := index.NewBitmap()
	b.Add(1)
	b.Add(5)
	b.Add(10)

	if !b.Contains(1) {
		t.Error("Bitmap should contain 1")
	}
	if !b.Contains(5) {
		t.Error("Bitmap should contain 5")
	}
	if b.Contains(2) {
		t.Error("Bitmap should not contain 2")
	}
	if b.Cardinality() != 3 {
		t.Errorf("Cardinality: got %d, want 3", b.Cardinality())
	}
}

func TestBitmap_And(t *testing.T) {
	b1 := index.NewBitmap()
	b1.Add(1)
	b1.Add(2)
	b1.Add(3)

	b2 := index.NewBitmap()
	b2.Add(2)
	b2.Add(3)
	b2.Add(4)

	result := b1.And(b2)
	arr := result.ToArray()

	if len(arr) != 2 {
		t.Errorf("And result: got %v, want [2, 3]", arr)
	}
}

func TestBitmap_Or(t *testing.T) {
	b1 := index.NewBitmap()
	b1.Add(1)
	b1.Add(2)

	b2 := index.NewBitmap()
	b2.Add(3)
	b2.Add(4)

	result := b1.Or(b2)
	if result.Cardinality() != 4 {
		t.Errorf("Or cardinality: got %d, want 4", result.Cardinality())
	}
}

func TestBitmap_Not(t *testing.T) {
	universe := index.NewBitmap()
	for i := uint32(1); i <= 10; i++ {
		universe.Add(i)
	}

	b := index.NewBitmap()
	b.Add(1)
	b.Add(2)
	b.Add(3)

	result := b.Not(universe)
	if result.Cardinality() != 7 {
		t.Errorf("Not cardinality: got %d, want 7", result.Cardinality())
	}
	if result.Contains(1) || result.Contains(2) || result.Contains(3) {
		t.Error("Not result should not contain 1, 2, 3")
	}
}

func TestBitmap_Serialize(t *testing.T) {
	b := index.NewBitmap()
	b.Add(1)
	b.Add(100)
	b.Add(10000)

	data, err := b.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	restored, err := index.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize error: %v", err)
	}

	if !restored.Contains(1) || !restored.Contains(100) || !restored.Contains(10000) {
		t.Error("Deserialized bitmap missing elements")
	}
	if restored.Cardinality() != 3 {
		t.Errorf("Deserialized cardinality: got %d, want 3", restored.Cardinality())
	}
}

// ==================== Inverted Index Tests ====================

func TestInvertedIndex_AddDocument(t *testing.T) {
	idx := index.NewInvertedIndex("english")

	idx.AddDocument("doc1", "Hello World", "This is a test document about cats")
	idx.AddDocument("doc2", "Goodbye", "Another document about dogs")

	if idx.DocumentCount() != 2 {
		t.Errorf("DocumentCount: got %d, want 2", idx.DocumentCount())
	}
}

func TestInvertedIndex_SearchTerm(t *testing.T) {
	idx := index.NewInvertedIndex("english")

	idx.AddDocument("doc1", "", "cats and dogs")
	idx.AddDocument("doc2", "", "only cats here")
	idx.AddDocument("doc3", "", "only dogs here")

	catDocs := idx.SearchTerm("cats")
	if catDocs.Cardinality() != 2 {
		t.Errorf("Search 'cats': got %d docs, want 2", catDocs.Cardinality())
	}

	dogDocs := idx.SearchTerm("dogs")
	if dogDocs.Cardinality() != 2 {
		t.Errorf("Search 'dogs': got %d docs, want 2", dogDocs.Cardinality())
	}
}

func TestInvertedIndex_RemoveDocument(t *testing.T) {
	idx := index.NewInvertedIndex("english")

	idx.AddDocument("doc1", "", "cats")
	idx.AddDocument("doc2", "", "cats")

	if idx.DocumentCount() != 2 {
		t.Errorf("Before remove: got %d, want 2", idx.DocumentCount())
	}

	idx.RemoveDocument("doc1")

	if idx.DocumentCount() != 1 {
		t.Errorf("After remove: got %d, want 1", idx.DocumentCount())
	}

	catDocs := idx.SearchTerm("cats")
	if catDocs.Cardinality() != 1 {
		t.Errorf("After remove, 'cats' search: got %d, want 1", catDocs.Cardinality())
	}
}

// ==================== Query Parser Tests ====================

func TestQueryParser_TermQuery(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "cats")
	idx.AddDocument("2", "", "dogs")

	docs, err := index.Search(idx, "cats")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Term query 'cats': got %d docs, want 1", len(docs))
	}
}

func TestQueryParser_AndQuery(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "cats dogs")
	idx.AddDocument("2", "", "cats birds")
	idx.AddDocument("3", "", "fish dogs")

	docs, err := index.Search(idx, "cats AND dogs")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("AND query: got %d docs, want 1", len(docs))
	}
}

func TestQueryParser_OrQuery(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "cats")
	idx.AddDocument("2", "", "dogs")
	idx.AddDocument("3", "", "birds")

	docs, err := index.Search(idx, "cats OR dogs")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("OR query: got %d docs, want 2", len(docs))
	}
}

func TestQueryParser_NotQuery(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "cats")
	idx.AddDocument("2", "", "dogs")
	idx.AddDocument("3", "", "birds")

	docs, err := index.Search(idx, "NOT cats")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("NOT query: got %d docs, want 2", len(docs))
	}
}

func TestQueryParser_Complex(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "cats dogs")
	idx.AddDocument("2", "", "cats birds")
	idx.AddDocument("3", "", "dogs birds")
	idx.AddDocument("4", "", "fish")

	docs, err := index.Search(idx, "(cats OR dogs) AND NOT birds")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Complex query: got %d docs, want 1", len(docs))
	}
}

func TestQueryParser_Parentheses(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "alpha beta")
	idx.AddDocument("2", "", "alpha gamma")
	idx.AddDocument("3", "", "beta gamma")

	// (alpha AND beta) OR gamma
	docs, err := index.Search(idx, "(alpha AND beta) OR gamma")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	// doc1 has alpha AND beta, doc2 has gamma, doc3 has gamma
	if len(docs) != 3 {
		t.Errorf("Parentheses query: got %d docs, want 3", len(docs))
	}
}

func TestQueryParser_PrefixSearch(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "car cart")
	idx.AddDocument("2", "", "carbon")
	idx.AddDocument("3", "", "dog")

	docs, err := index.Search(idx, "car*")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("Prefix query 'car*': got %d docs, want 2", len(docs))
	}
}

func TestQueryParser_WildcardSearch(t *testing.T) {
	idx := index.NewInvertedIndex("english")
	idx.AddDocument("1", "", "cat")
	idx.AddDocument("2", "", "carpet")
	idx.AddDocument("3", "", "dog")

	docs, err := index.Search(idx, "c*pet")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Wildcard query 'c*pet': got %d docs, want 1", len(docs))
	}
}

// ==================== LSM Index Tests ====================

func TestLSMIndex_AddDocument(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	_, err = idx.AddDocument("doc1", "Hello", "World of cats")
	if err != nil {
		t.Fatalf("AddDocument error: %v", err)
	}

	if idx.DocumentCount() != 1 {
		t.Errorf("DocumentCount: got %d, want 1", idx.DocumentCount())
	}
}

func TestLSMIndex_Search(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	idx.AddDocument("doc1", "", "cats and dogs")
	idx.AddDocument("doc2", "", "only cats")
	idx.AddDocument("doc3", "", "only dogs")

	docs, err := index.SearchLSM(idx, "cats")
	if err != nil {
		t.Fatalf("SearchLSM error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("SearchLSM 'cats': got %d, want 2", len(docs))
	}
}

func TestLSMIndex_BooleanSearch(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	idx.AddDocument("doc1", "", "cats dogs")
	idx.AddDocument("doc2", "", "cats birds")
	idx.AddDocument("doc3", "", "dogs birds")

	docs, err := index.SearchLSM(idx, "cats AND dogs")
	if err != nil {
		t.Fatalf("SearchLSM error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("SearchLSM 'cats AND dogs': got %d, want 1", len(docs))
	}
}

func TestLSMIndex_PrefixSearch(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	idx.AddDocument("1", "", "car cart")
	idx.AddDocument("2", "", "carbon")
	idx.AddDocument("3", "", "dog")

	docs, err := index.SearchLSM(idx, "car*")
	if err != nil {
		t.Fatalf("SearchLSM error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("SearchLSM 'car*': got %d, want 2", len(docs))
	}
}

func TestLSMIndex_WildcardSearch(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	idx.AddDocument("1", "", "cat")
	idx.AddDocument("2", "", "carpet")
	idx.AddDocument("3", "", "dog")

	docs, err := index.SearchLSM(idx, "c*pet")
	if err != nil {
		t.Fatalf("SearchLSM error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("SearchLSM 'c*pet': got %d, want 1", len(docs))
	}
}

func TestLSMIndex_FlushAndReload(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}

	idx.AddDocument("doc1", "Title", "content about cats")
	idx.AddDocument("doc2", "Another", "content about dogs")

	err = idx.Flush()
	if err != nil {
		t.Fatalf("Flush error: %v", err)
	}
	idx.Close()

	idx2, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex (reload) error: %v", err)
	}
	defer idx2.Close()

	if idx2.DocumentCount() != 2 {
		t.Errorf("After reload, DocumentCount: got %d, want 2", idx2.DocumentCount())
	}

	docs, err := index.SearchLSM(idx2, "cats")
	if err != nil {
		t.Fatalf("SearchLSM after reload error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("SearchLSM 'cats' after reload: got %d, want 1", len(docs))
	}
}

func TestLSMIndex_TermAcrossMultipleFlushes(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}

	if _, err := idx.AddDocument("doc1", "", "cats only"); err != nil {
		t.Fatalf("AddDocument doc1 error: %v", err)
	}
	if err := idx.Flush(); err != nil {
		t.Fatalf("Flush #1 error: %v", err)
	}

	if _, err := idx.AddDocument("doc2", "", "cats again"); err != nil {
		t.Fatalf("AddDocument doc2 error: %v", err)
	}
	if err := idx.Flush(); err != nil {
		t.Fatalf("Flush #2 error: %v", err)
	}

	docs, err := index.SearchLSM(idx, "cats")
	if err != nil {
		t.Fatalf("SearchLSM before reload error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("SearchLSM 'cats' before reload: got %d, want 2", len(docs))
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	idx2, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex reload error: %v", err)
	}
	defer idx2.Close()

	docs, err = index.SearchLSM(idx2, "cats")
	if err != nil {
		t.Fatalf("SearchLSM after reload error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("SearchLSM 'cats' after reload: got %d, want 2", len(docs))
	}
}

func TestLSMIndex_RemoveDocument(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	idx.AddDocument("doc1", "", "cats")
	idx.AddDocument("doc2", "", "cats")

	err = idx.RemoveDocument("doc1")
	if err != nil {
		t.Fatalf("RemoveDocument error: %v", err)
	}

	if idx.DocumentCount() != 1 {
		t.Errorf("After remove, DocumentCount: got %d, want 1", idx.DocumentCount())
	}

	docs, err := index.SearchLSM(idx, "cats")
	if err != nil {
		t.Fatalf("SearchLSM error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("After remove, SearchLSM 'cats': got %d, want 1", len(docs))
	}
}

// ==================== Integration Tests ====================

func TestIntegration_RussianText(t *testing.T) {
	idx := index.NewInvertedIndex("russian")

	idx.AddDocument("1", "Война и мир", "Роман о войне и мире в России")
	idx.AddDocument("2", "Преступление и наказание", "Роман о преступлении")
	idx.AddDocument("3", "Анна Каренина", "История о любви")

	docs, err := index.Search(idx, "война")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) < 1 {
		t.Errorf("Search 'война': got %d docs, want at least 1", len(docs))
	}
}

func TestIntegration_MixedLanguage(t *testing.T) {
	idx := index.NewInvertedIndex("mixed")

	idx.AddDocument("1", "Hello Мир", "English and Russian text")
	idx.AddDocument("2", "Привет World", "Russian and English text")

	docs, err := index.Search(idx, "hello")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("Search 'hello': got %d, want 1", len(docs))
	}
}

func TestIntegration_LargeDataset(t *testing.T) {
	dir := t.TempDir()

	idx, err := index.NewLSMIndex(dir, "english", 4)
	if err != nil {
		t.Fatalf("NewLSMIndex error: %v", err)
	}
	defer idx.Close()

	for i := 0; i < 1000; i++ {
		content := fmt.Sprintf("document %d with term%d content", i, i%10)
		idx.AddDocument(fmt.Sprintf("doc%d", i), "", content)
	}

	docs, err := index.SearchLSM(idx, "term5")
	if err != nil {
		t.Fatalf("SearchLSM error: %v", err)
	}
	if len(docs) != 100 {
		t.Errorf("SearchLSM 'term5': got %d, want 100", len(docs))
	}
}

func ExampleInvertedIndex() {
	idx := index.NewInvertedIndex("english")

	idx.AddDocument("doc1", "The Cat in the Hat", "A cat wearing a hat causes chaos")
	idx.AddDocument("doc2", "Dog Days", "A story about a lazy dog in summer")
	idx.AddDocument("doc3", "Cat and Dog", "A tale of friendship between a cat and a dog")

	docs, _ := index.Search(idx, "cat")
	fmt.Printf("Documents with 'cat': %d\n", len(docs))

	docs, _ = index.Search(idx, "cat AND dog")
	fmt.Printf("Documents with 'cat AND dog': %d\n", len(docs))

	docs, _ = index.Search(idx, "(cat OR dog) AND NOT summer")
	fmt.Printf("Documents with '(cat OR dog) AND NOT summer': %d\n", len(docs))
	// Output:
	// Documents with 'cat': 2
	// Documents with 'cat AND dog': 1
	// Documents with '(cat OR dog) AND NOT summer': 2
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func BenchmarkInvertedIndex_AddDocument(b *testing.B) {
	idx := index.NewInvertedIndex("english")
	content := "This is a test document with some words for benchmarking purposes"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.AddDocument(fmt.Sprintf("doc%d", i), "Title", content)
	}
}

func BenchmarkInvertedIndex_Search(b *testing.B) {
	idx := index.NewInvertedIndex("english")

	for i := 0; i < 10000; i++ {
		idx.AddDocument(fmt.Sprintf("doc%d", i), "", fmt.Sprintf("document %d test content", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Search(idx, "test AND content")
	}
}

func BenchmarkLSMIndex_AddDocument(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "bench_lsm_add")
	os.RemoveAll(dir)

	idx, _ := index.NewLSMIndex(dir, "english", 4)
	defer func() {
		idx.Close()
		os.RemoveAll(dir)
	}()

	content := "This is a test document with some words for benchmarking purposes"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.AddDocument(fmt.Sprintf("doc%d", i), "Title", content)
	}
}

func BenchmarkBitmap_And(b *testing.B) {
	b1 := index.NewBitmap()
	b2 := index.NewBitmap()

	for i := uint32(0); i < 10000; i++ {
		b1.Add(i)
		if i%2 == 0 {
			b2.Add(i)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b1.And(b2)
	}
}
