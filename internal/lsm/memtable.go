package lsm

import "sync"

type Record struct {
	Key       string
	Value     []byte
	Tombstone bool
}

const (
	red   bool = true
	black bool = false
)

type node struct {
	rec          Record
	color        bool
	parent, left *node
	right        *node
}

type Memtable struct {
	mu       sync.RWMutex
	root     *node
	size     int
	byteSize int
}

func New() *Memtable {
	return &Memtable{}
}

func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Memtable) ByteSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byteSize
}

func (m *Memtable) GetRecord(key string) (rec Record, exists bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	n := m.findNode(key)
	if n == nil {
		return Record{}, false
	}
	rec = n.rec
	if rec.Value != nil {
		rec.Value = append([]byte(nil), rec.Value...)
	}
	return rec, true
}

func (m *Memtable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.root == nil {
		m.root = &node{
			rec:   Record{Key: key, Value: append([]byte(nil), value...), Tombstone: false},
			color: black,
		}
		m.size = 1
		m.byteSize = len(key) + len(value)
		return
	}

	cur := m.root
	for {
		if key == cur.rec.Key {
			m.byteSize -= len(cur.rec.Value)
			cur.rec.Value = append([]byte(nil), value...)
			cur.rec.Tombstone = false
			m.byteSize += len(value)
			return
		}
		if key < cur.rec.Key {
			if cur.left == nil {
				n := &node{
					rec:    Record{Key: key, Value: append([]byte(nil), value...), Tombstone: false},
					color:  red,
					parent: cur,
				}
				cur.left = n
				m.size++
				m.byteSize += len(key) + len(value)
				m.insertFixup(n)
				return
			}
			cur = cur.left
		} else {
			if cur.right == nil {
				n := &node{
					rec:    Record{Key: key, Value: append([]byte(nil), value...), Tombstone: false},
					color:  red,
					parent: cur,
				}
				cur.right = n
				m.size++
				m.byteSize += len(key) + len(value)
				m.insertFixup(n)
				return
			}
			cur = cur.right
		}
	}
}

func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := m.findNode(key)
	if n != nil {
		n.rec.Tombstone = true
		return
	}

	if m.root == nil {
		m.root = &node{
			rec:   Record{Key: key, Value: nil, Tombstone: true},
			color: black,
		}
		m.size = 1
		return
	}

	cur := m.root
	for {
		if key < cur.rec.Key {
			if cur.left == nil {
				nn := &node{
					rec:    Record{Key: key, Value: nil, Tombstone: true},
					color:  red,
					parent: cur,
				}
				cur.left = nn
				m.size++
				m.insertFixup(nn)
				return
			}
			cur = cur.left
		} else {
			if cur.right == nil {
				nn := &node{
					rec:    Record{Key: key, Value: nil, Tombstone: true},
					color:  red,
					parent: cur,
				}
				cur.right = nn
				m.size++
				m.insertFixup(nn)
				return
			}
			cur = cur.right
		}
	}
}

func (m *Memtable) Range(fn func(rec Record) bool) {
	m.mu.RLock()
	var stack []*node
	cur := m.root
	for cur != nil || len(stack) > 0 {
		for cur != nil {
			stack = append(stack, cur)
			cur = cur.left
		}
		cur = stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		rec := cur.rec
		if rec.Value != nil {
			rec.Value = append([]byte(nil), rec.Value...)
		}
		if !fn(rec) {
			m.mu.RUnlock()
			return
		}
		cur = cur.right
	}
	m.mu.RUnlock()
}

func (m *Memtable) findNode(key string) *node {
	cur := m.root
	for cur != nil {
		if key == cur.rec.Key {
			return cur
		}
		if key < cur.rec.Key {
			cur = cur.left
		} else {
			cur = cur.right
		}
	}
	return nil
}

func (m *Memtable) insertFixup(z *node) {
	for z.parent != nil && z.parent.color == red {
		if z.parent == z.parent.parent.left {
			y := z.parent.parent.right
			if y != nil && y.color == red {
				z.parent.color = black
				y.color = black
				z.parent.parent.color = red
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					m.rotateLeft(z)
				}
				z.parent.color = black
				z.parent.parent.color = red
				m.rotateRight(z.parent.parent)
			}
		} else {
			y := z.parent.parent.left
			if y != nil && y.color == red {
				z.parent.color = black
				y.color = black
				z.parent.parent.color = red
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					m.rotateRight(z)
				}
				z.parent.color = black
				z.parent.parent.color = red
				m.rotateLeft(z.parent.parent)
			}
		}
	}
	m.root.color = black
}

func (m *Memtable) rotateLeft(x *node) {
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		m.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

func (m *Memtable) rotateRight(x *node) {
	y := x.left
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		m.root = y
	} else if x == x.parent.right {
		x.parent.right = y
	} else {
		x.parent.left = y
	}
	y.right = x
	x.parent = y
}

func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.root = nil
	m.size = 0
	m.byteSize = 0
}

type MemtableIterator struct {
	m       *Memtable
	stack   []*node
	keyFrom string
	keyTo   string
	started bool
}

func (m *Memtable) RangeScanIter(keyFrom, keyTo string) *MemtableIterator {
	m.mu.RLock()
	return &MemtableIterator{
		m:       m,
		stack:   nil,
		keyFrom: keyFrom,
		keyTo:   keyTo,
		started: false,
	}
}

func (it *MemtableIterator) Next() (KV, bool) {
	if it.m == nil {
		return KV{}, false
	}

	if !it.started {
		it.started = true
		cur := it.m.root
		for cur != nil {
			if cur.rec.Key < it.keyFrom {
				cur = cur.right
			} else {
				it.stack = append(it.stack, cur)
				cur = cur.left
			}
		}
	}

	for len(it.stack) > 0 {
		cur := it.stack[len(it.stack)-1]
		it.stack = it.stack[:len(it.stack)-1]

		if cur.rec.Key >= it.keyTo {
			it.stack = nil
			return KV{}, false
		}

		right := cur.right
		for right != nil {
			if right.rec.Key < it.keyFrom {
				right = right.right
			} else {
				it.stack = append(it.stack, right)
				right = right.left
			}
		}

		if cur.rec.Key >= it.keyFrom {
			val := string(cur.rec.Value)
			return KV{Key: cur.rec.Key, Value: val, Tombstone: cur.rec.Tombstone}, true
		}
	}

	return KV{}, false
}

func (it *MemtableIterator) Close() {
	if it.m != nil {
		it.m.mu.RUnlock()
		it.m = nil
	}
}
