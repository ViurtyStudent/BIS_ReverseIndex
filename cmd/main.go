package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"ViurtyStudent/internal/index"
)

func main() {
	fmt.Println("=== Inverted Index Demo ===")
	fmt.Println("Using Roaring Bitmaps + LSM Tree")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)

	// Выбор типа индекса
	fmt.Println("Select index type:")
	fmt.Println("  1. In-memory (InvertedIndex)")
	fmt.Println("  2. LSM-based (persistent)")
	fmt.Print("Choice [1]: ")

	choice, _ := reader.ReadString('\n')
	choice = strings.TrimSpace(choice)

	var memIdx *index.InvertedIndex
	var lsmIdx *index.LSMIndex
	useLSM := choice == "2"

	// Выбор языка
	fmt.Print("Language (english/russian/mixed) [mixed]: ")
	lang, _ := reader.ReadString('\n')
	lang = strings.TrimSpace(lang)
	if lang == "" {
		lang = "mixed"
	}

	if useLSM {
		fmt.Print("Data directory [./index_data]: ")
		dir, _ := reader.ReadString('\n')
		dir = strings.TrimSpace(dir)
		if dir == "" {
			dir = "./index_data"
		}

		var err error
		lsmIdx, err = index.NewLSMIndex(dir, lang, 4)
		if err != nil {
			fmt.Printf("Error creating LSM index: %v\n", err)
			return
		}
		defer lsmIdx.Close()
		fmt.Printf("LSM Index created at %s\n", dir)
	} else {
		memIdx = index.NewInvertedIndex(lang)
		fmt.Println("In-memory index created")
	}

	fmt.Println()
	printHelp()

	for {
		fmt.Print("\n> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.SplitN(input, " ", 2)
		cmd := strings.ToLower(parts[0])
		args := ""
		if len(parts) > 1 {
			args = parts[1]
		}

		switch cmd {
		case "help", "h", "?":
			printHelp()

		case "add", "a":
			if args == "" {
				fmt.Println("Usage: add <id> <title> | <content>")
				fmt.Println("Example: add doc1 My Title | This is the document content")
				continue
			}
			addDocument(memIdx, lsmIdx, args, useLSM)

		case "search", "s", "q":
			if args == "" {
				fmt.Println("Usage: search <query>")
				fmt.Println("Example: search cats AND dogs")
				continue
			}
			searchDocuments(memIdx, lsmIdx, args, useLSM)

		case "remove", "rm", "delete":
			if args == "" {
				fmt.Println("Usage: remove <id>")
				continue
			}
			removeDocument(memIdx, lsmIdx, args, useLSM)

		case "list", "ls":
			listDocuments(memIdx, lsmIdx, useLSM)

		case "stats":
			showStats(memIdx, lsmIdx, useLSM)

		case "flush":
			if useLSM {
				if err := lsmIdx.Flush(); err != nil {
					fmt.Printf("Flush error: %v\n", err)
				} else {
					fmt.Println("Index flushed to disk")
				}
			} else {
				fmt.Println("Flush is only for LSM index")
			}

		case "demo":
			loadDemoData(memIdx, lsmIdx, useLSM)

		case "exit", "quit", "q!":
			fmt.Println("Bye!")
			return

		default:
			fmt.Printf("Unknown command: %s (type 'help' for commands)\n", cmd)
		}
	}
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  add <id> <title> | <content>  - Add document")
	fmt.Println("  search <query>                - Search with boolean query")
	fmt.Println("  remove <id>                   - Remove document")
	fmt.Println("  list                          - List all documents")
	fmt.Println("  stats                         - Show index statistics")
	fmt.Println("  flush                         - Flush LSM index to disk")
	fmt.Println("  demo                          - Load demo documents")
	fmt.Println("  help                          - Show this help")
	fmt.Println("  exit                          - Exit program")
	fmt.Println()
	fmt.Println("Query syntax:")
	fmt.Println("  term                - Single term search")
	fmt.Println("  prefix*             - Prefix search")
	fmt.Println("  wi*card             - Wildcard search (k-gram)")
	fmt.Println("  term1 AND term2     - Both terms must be present")
	fmt.Println("  term1 OR term2      - Either term must be present")
	fmt.Println("  NOT term            - Term must NOT be present")
	fmt.Println("  (term1 OR term2) AND term3 - Parentheses for grouping")
}

func addDocument(memIdx *index.InvertedIndex, lsmIdx *index.LSMIndex, args string, useLSM bool) {
	// Parse: id title | content
	parts := strings.SplitN(args, " ", 2)
	if len(parts) < 2 {
		fmt.Println("Usage: add <id> <title> | <content>")
		return
	}

	docID := parts[0]
	rest := parts[1]

	title := ""
	content := rest

	if idx := strings.Index(rest, "|"); idx != -1 {
		title = strings.TrimSpace(rest[:idx])
		content = strings.TrimSpace(rest[idx+1:])
	}

	if useLSM {
		id, err := lsmIdx.AddDocument(docID, title, content)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("Added document '%s' (internal ID: %d)\n", docID, id)
	} else {
		id := memIdx.AddDocument(docID, title, content)
		fmt.Printf("Added document '%s' (internal ID: %d)\n", docID, id)
	}
}

func searchDocuments(memIdx *index.InvertedIndex, lsmIdx *index.LSMIndex, query string, useLSM bool) {
	fmt.Printf("Query: %s\n", query)

	var docs []*index.Document
	var err error

	if useLSM {
		docs, err = index.SearchLSM(lsmIdx, query)
	} else {
		docs, err = index.Search(memIdx, query)
	}

	if err != nil {
		fmt.Printf("Search error: %v\n", err)
		return
	}

	if len(docs) == 0 {
		fmt.Println("No documents found")
		return
	}

	fmt.Printf("Found %d document(s):\n", len(docs))
	for _, doc := range docs {
		title := doc.Title
		if title == "" {
			title = "(no title)"
		}
		content := doc.Content
		if len(content) > 80 {
			content = content[:80] + "..."
		}
		fmt.Printf("  [%d] %s\n", doc.ID, title)
		fmt.Printf("      %s\n", content)
	}
}

func removeDocument(memIdx *index.InvertedIndex, lsmIdx *index.LSMIndex, docID string, useLSM bool) {
	if useLSM {
		if err := lsmIdx.RemoveDocument(docID); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
	} else {
		memIdx.RemoveDocument(docID)
	}
	fmt.Printf("Removed document '%s'\n", docID)
}

func listDocuments(memIdx *index.InvertedIndex, lsmIdx *index.LSMIndex, useLSM bool) {
	var count int
	if useLSM {
		count = lsmIdx.DocumentCount()
	} else {
		count = memIdx.DocumentCount()
	}

	if count == 0 {
		fmt.Println("No documents in index")
		return
	}

	fmt.Printf("Documents in index: %d\n", count)

	// Get documents by iterating through IDs (simple approach)
	for i := uint32(1); i <= uint32(count+100); i++ {
		var doc *index.Document
		var ok bool
		if useLSM {
			doc, ok = lsmIdx.GetDocument(i)
		} else {
			doc, ok = memIdx.GetDocument(i)
		}
		if ok {
			title := doc.Title
			if title == "" {
				title = "(no title)"
			}
			content := doc.Content
			if len(content) > 60 {
				content = content[:60] + "..."
			}
			fmt.Printf("  [%d] %s: %s\n", doc.ID, title, content)
		}
	}
}

func showStats(memIdx *index.InvertedIndex, lsmIdx *index.LSMIndex, useLSM bool) {
	if useLSM {
		stats := lsmIdx.Stats()
		fmt.Printf("LSM Index Statistics:\n")
		fmt.Printf("  Documents: %d\n", stats.DocumentCount)
		fmt.Printf("  In-memory terms: %d\n", stats.MemTermCount)
		fmt.Printf("  Memory buffer: %d bytes\n", stats.MemByteSize)
		fmt.Printf("  Base directory: %s\n", stats.BaseDir)
	} else {
		fmt.Printf("In-Memory Index Statistics:\n")
		fmt.Printf("  Documents: %d\n", memIdx.DocumentCount())
		fmt.Printf("  Terms: %d\n", memIdx.TermCount())
	}
}

func loadDemoData(memIdx *index.InvertedIndex, lsmIdx *index.LSMIndex, useLSM bool) {
	demos := []struct {
		id      string
		title   string
		content string
	}{
		{"doc1", "The Cat in the Hat", "A cat wearing a tall striped hat causes chaos in a house while the children's mother is away"},
		{"doc2", "Dog Days of Summer", "A story about a lazy dog enjoying the warm summer days in the backyard"},
		{"doc3", "Cat and Dog Friends", "A tale of unlikely friendship between a cat named Whiskers and a dog named Buddy"},
		{"doc4", "Birds of Paradise", "Documentary about exotic birds living in tropical rainforests"},
		{"doc5", "The Fish Tank", "Guide to maintaining a healthy aquarium with tropical fish"},
		{"doc6", "Война и мир", "Роман Льва Толстого о войне и мире в России начала XIX века"},
		{"doc7", "Преступление и наказание", "Роман Достоевского о студенте Раскольникове и его преступлении"},
		{"doc8", "Programming Cats", "A book about how cats would write code if they could program computers"},
		{"doc9", "Summer Vacation", "Planning the perfect summer vacation with family and friends"},
		{"doc10", "Winter Dogs", "How dogs adapt to cold winter weather and snow"},
	}

	fmt.Println("Loading demo documents...")
	for _, d := range demos {
		if useLSM {
			lsmIdx.AddDocument(d.id, d.title, d.content)
		} else {
			memIdx.AddDocument(d.id, d.title, d.content)
		}
		fmt.Printf("  Added: %s - %s\n", d.id, d.title)
	}

	count := 0
	if useLSM {
		count = lsmIdx.DocumentCount()
	} else {
		count = memIdx.DocumentCount()
	}
	fmt.Printf("\nLoaded %d documents. Try these searches:\n", count)
	fmt.Println("  search cat")
	fmt.Println("  search cat AND dog")
	fmt.Println("  search cat OR dog")
	fmt.Println("  search (cat OR dog) AND NOT summer")
	fmt.Println("  search война")
	fmt.Println("  search fish OR bird")
}
