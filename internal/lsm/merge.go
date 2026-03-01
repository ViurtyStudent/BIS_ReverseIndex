package lsm

import "container/heap"

type kvIterator interface {
	Next() (KV, bool, error)
}

type memIterWrapper struct {
	it *MemtableIterator
}

func (w *memIterWrapper) Next() (KV, bool, error) {
	kv, ok := w.it.Next()
	return kv, ok, nil
}

type ssIterWrapper struct {
	it *SSTableIterator
}

func (w *ssIterWrapper) Next() (KV, bool, error) {
	return w.it.Next()
}

type mergeSource struct {
	iter     kvIterator
	priority int
}

type heapItem struct {
	kv       KV
	priority int
	idx      int
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].kv.Key != h[j].kv.Key {
		return h[i].kv.Key < h[j].kv.Key
	}
	return h[i].priority < h[j].priority
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func kWayMerge(sources []mergeSource) ([]KV, error) {
	h := &mergeHeap{}
	heap.Init(h)

	for i, src := range sources {
		kv, ok, err := src.iter.Next()
		if err != nil {
			return nil, err
		}
		if ok {
			heap.Push(h, heapItem{kv: kv, priority: src.priority, idx: i})
		}
	}

	var result []KV
	var lastKey string
	first := true

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		if first || item.kv.Key != lastKey {
			if !item.kv.Tombstone {
				result = append(result, item.kv)
			}
			lastKey = item.kv.Key
			first = false
		}

		kv, ok, err := sources[item.idx].iter.Next()
		if err != nil {
			return nil, err
		}
		if ok {
			heap.Push(h, heapItem{kv: kv, priority: sources[item.idx].priority, idx: item.idx})
		}
	}

	return result, nil
}
