package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type LSMTree struct {
	baseDir  string
	r        int
	c0       int
	memtable *Memtable
	levels   [][]string
	seq      int
	readers  map[string]*SSTable
	mu       sync.RWMutex
}

func NewLSMTree(baseDir string, r int) (*LSMTree, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	tree := &LSMTree{
		baseDir:  baseDir,
		r:        r,
		c0:       10 * 1024 * 1024,
		memtable: New(),
		levels:   [][]string{},
		seq:      0,
		readers:  map[string]*SSTable{},
	}
	tree.loadExistingSSTables()
	return tree, nil
}

func (t *LSMTree) loadExistingSSTables() {
	entries, err := os.ReadDir(t.baseDir)
	if err != nil {
		return
	}

	levelFiles := make(map[int][]string)
	maxSeq := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) < 4 || name[len(name)-4:] != ".sst" {
			continue
		}

		var level, seq int
		_, err := fmt.Sscanf(name, "L%d_%d.sst", &level, &seq)
		if err != nil {
			continue
		}

		path := filepath.Join(t.baseDir, name)
		levelFiles[level] = append(levelFiles[level], path)
		if seq >= maxSeq {
			maxSeq = seq + 1
		}
	}

	t.seq = maxSeq

	maxLevel := -1
	for level := range levelFiles {
		if level > maxLevel {
			maxLevel = level
		}
	}

	if maxLevel >= 0 {
		t.levels = make([][]string, maxLevel+1)
		for level, files := range levelFiles {
			sort.Sort(sort.Reverse(sort.StringSlice(files)))
			t.levels[level] = files
		}
	}
}

func (t *LSMTree) Insert(key, value string) error {
	if t == nil {
		return fmt.Errorf("lsm tree is nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.memtable.Put(key, []byte(value))
	if t.memtable.ByteSize() >= t.c0 {
		return t.flushLocked()
	}
	return nil
}

func (t *LSMTree) Get(key string) (string, bool, error) {
	if t == nil {
		return "", false, fmt.Errorf("lsm tree is nil")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if rec, ok := t.memtable.GetRecord(key); ok {
		if rec.Tombstone {
			return "", false, nil
		}
		return string(rec.Value), true, nil
	}
	for level := 0; level < len(t.levels); level++ {
		for _, path := range t.levels[level] {
			table, err := t.getTable(path)
			if err != nil {
				return "", false, err
			}
			kv, ok, err := table.Get(key)
			if err != nil {
				return "", false, err
			}
			if ok {
				if kv.Tombstone {
					return "", false, nil
				}
				return kv.Value, true, nil
			}
		}
	}
	return "", false, nil
}

func (t *LSMTree) RangeScan(keyFrom, keyTo string) ([]KV, error) {
	if t == nil {
		return nil, fmt.Errorf("lsm tree is nil")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	var iters []mergeSource

	memIter := t.memtable.RangeScanIter(keyFrom, keyTo)
	defer memIter.Close()
	iters = append(iters, mergeSource{iter: &memIterWrapper{memIter}, priority: 0})

	priority := 1
	for level := 0; level < len(t.levels); level++ {
		for _, path := range t.levels[level] {
			table, err := t.getTable(path)
			if err != nil {
				return nil, err
			}
			ssIter := table.RangeScan(keyFrom, keyTo)
			iters = append(iters, mergeSource{iter: &ssIterWrapper{ssIter}, priority: priority})
			priority++
		}
	}

	return kWayMerge(iters)
}

func (t *LSMTree) Delete(key string) error {
	if t == nil {
		return fmt.Errorf("lsm tree is nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.memtable.Delete(key)
	return nil
}

func (t *LSMTree) Flush() error {
	if t == nil {
		return fmt.Errorf("lsm tree is nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushLocked()
}

func (t *LSMTree) flushLocked() error {
	if t.memtable.Len() == 0 {
		return nil
	}

	var items []KV
	t.memtable.Range(func(rec Record) bool {
		items = append(items, KV{Key: rec.Key, Value: string(rec.Value), Tombstone: rec.Tombstone})
		return true
	})

	if len(t.levels) == 0 {
		t.levels = append(t.levels, []string{})
	}

	path := filepath.Join(t.baseDir, fmt.Sprintf("L0_%06d.sst", t.seq))
	t.seq++
	if err := WriteSSTable(path, items); err != nil {
		return err
	}

	t.levels[0] = append([]string{path}, t.levels[0]...)

	t.memtable.Clear()

	return t.maybeCompact(0)
}

func (t *LSMTree) compactLevel(level int) error {
	if level >= len(t.levels) || len(t.levels[level]) == 0 {
		return nil
	}

	merged := make(map[string]KV)
	for _, path := range t.levels[level] {
		table, err := t.getTable(path)
		if err != nil {
			return err
		}
		iter := table.Iterator()
		for {
			kv, ok, err := iter.Next()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if _, exists := merged[kv.Key]; !exists {
				merged[kv.Key] = kv
			}
		}
	}

	entries := make([]KV, 0, len(merged))
	for _, kv := range merged {
		entries = append(entries, kv)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	for len(t.levels) <= level+1 {
		t.levels = append(t.levels, []string{})
	}

	newPath := filepath.Join(t.baseDir, fmt.Sprintf("L%d_%06d.sst", level+1, t.seq))
	t.seq++
	if err := WriteSSTable(newPath, entries); err != nil {
		return err
	}

	for _, path := range t.levels[level] {
		t.closeTable(path)
		_ = os.Remove(path)
	}
	t.levels[level] = []string{}
	t.levels[level+1] = append([]string{newPath}, t.levels[level+1]...)

	return t.maybeCompact(level + 1)
}

func (t *LSMTree) maybeCompact(level int) error {
	if level >= len(t.levels) {
		return nil
	}
	if t.levelSizeBytes(level) <= t.levelMaxBytes(level) {
		return nil
	}
	return t.compactLevel(level)
}

func (t *LSMTree) levelMaxBytes(level int) int64 {
	max := int64(t.c0)
	for i := 0; i < level; i++ {
		max *= int64(t.r)
	}
	return max
}

func (t *LSMTree) levelSizeBytes(level int) int64 {
	if level >= len(t.levels) {
		return 0
	}
	var total int64
	for _, path := range t.levels[level] {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

func (t *LSMTree) getTable(path string) (*SSTable, error) {
	if t.readers == nil {
		t.readers = map[string]*SSTable{}
	}
	if table, ok := t.readers[path]; ok {
		return table, nil
	}
	table, err := OpenSSTable(path)
	if err != nil {
		return nil, err
	}
	t.readers[path] = table
	return table, nil
}

func (t *LSMTree) closeTable(path string) {
	if t.readers == nil {
		return
	}
	if table, ok := t.readers[path]; ok {
		_ = table.Close()
		delete(t.readers, path)
	}
}
