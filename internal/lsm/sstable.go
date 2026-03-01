package lsm

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
)

const defaultFalsePositiveRate = 0.01

type KV struct {
	Key       string
	Value     string
	Tombstone bool
}

type IndexEntry struct {
	Key    string
	Offset int64
}

type SSTable struct {
	file  *os.File
	bloom *BloomFilter
	index []IndexEntry
}

type SSTableIterator struct {
	table *SSTable
	start int
	end   int
	curr  int
}

func WriteSSTable(path string, items []KV) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, item := range items {
		if err := writeRecord(file, item); err != nil {
			return err
		}
	}
	return nil
}

func OpenSSTable(path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	index, err := buildIndex(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	var bloom *BloomFilter
	if len(index) > 0 {
		bloom = NewOptimalBloomFilter(len(index), defaultFalsePositiveRate)
		for _, entry := range index {
			bloom.Add(entry.Key)
		}
	}
	return &SSTable{
		file:  file,
		bloom: bloom,
		index: index,
	}, nil
}

func (s *SSTable) Close() error {
	if s == nil || s.file == nil {
		return nil
	}

	err := s.file.Close()
	s.file = nil
	return err
}

func (s *SSTable) Get(key string) (KV, bool, error) {
	if s == nil {
		return KV{}, false, nil
	}

	if s.file == nil {
		return KV{}, false, os.ErrClosed
	}

	if len(s.index) == 0 {
		return KV{}, false, nil
	}

	if s.bloom != nil && !s.bloom.Check(key) {
		return KV{}, false, nil
	}

	pos := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key >= key
	})

	if pos >= len(s.index) || s.index[pos].Key != key {
		return KV{}, false, nil
	}

	kv, err := readRecordAt(s.file, s.index[pos].Offset)
	if err != nil {
		return KV{}, false, err
	}

	return kv, true, nil
}

func (s *SSTable) RangeScan(keyFrom, keyTo string) *SSTableIterator {
	if s == nil || len(s.index) == 0 || keyTo < keyFrom {
		return &SSTableIterator{table: s, start: 0, end: 0}
	}
	start := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key >= keyFrom
	})
	end := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key >= keyTo
	})
	return &SSTableIterator{table: s, start: start, end: end, curr: start}
}

func (s *SSTable) Iterator() *SSTableIterator {
	end := 0
	if s != nil {
		end = len(s.index)
	}
	return &SSTableIterator{table: s, start: 0, end: end, curr: 0}
}

func (it *SSTableIterator) Next() (KV, bool, error) {
	if it == nil || it.table == nil || it.curr >= it.end {
		return KV{}, false, nil
	}
	entry := it.table.index[it.curr]
	kv, err := readRecordAt(it.table.file, entry.Offset)
	if err != nil {
		return KV{}, false, err
	}
	it.curr++
	return kv, true, nil
}

func readRecordAt(file *os.File, offset int64) (KV, error) {
	header := make([]byte, 9)

	if _, err := file.ReadAt(header, offset); err != nil {
		return KV{}, err
	}

	keyLen := binary.BigEndian.Uint32(header[:4])
	valueLen := binary.BigEndian.Uint32(header[4:8])
	tombstone := header[8] != 0

	keyBuf := make([]byte, keyLen)
	valBuf := make([]byte, valueLen)

	if _, err := file.ReadAt(keyBuf, offset+9); err != nil {
		return KV{}, err
	}

	if _, err := file.ReadAt(valBuf, offset+9+int64(keyLen)); err != nil {
		return KV{}, err
	}

	return KV{Key: string(keyBuf), Value: string(valBuf), Tombstone: tombstone}, nil
}

func readHeader(file *os.File) (uint32, uint32, error) {
	var header [9]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return 0, 0, err
	}
	keyLen := binary.BigEndian.Uint32(header[:4])
	valueLen := binary.BigEndian.Uint32(header[4:8])
	return keyLen, valueLen, nil
}

func writeRecord(file *os.File, kv KV) error {
	keyLen := uint32(len(kv.Key))
	valueLen := uint32(len(kv.Value))

	var header [9]byte
	binary.BigEndian.PutUint32(header[:4], keyLen)
	binary.BigEndian.PutUint32(header[4:8], valueLen)
	if kv.Tombstone {
		header[8] = 1
	}

	if _, err := file.Write(header[:]); err != nil {
		return err
	}
	if _, err := file.Write([]byte(kv.Key)); err != nil {
		return err
	}
	_, err := file.Write([]byte(kv.Value))
	return err
}

func buildIndex(file *os.File) ([]IndexEntry, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	index := make([]IndexEntry, 0)
	for {
		offset, _ := file.Seek(0, io.SeekCurrent)
		keyLen, valueLen, err := readHeader(file)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBuf); err != nil {
			return nil, err
		}
		if _, err := io.CopyN(io.Discard, file, int64(valueLen)); err != nil {
			return nil, err
		}
		index = append(index, IndexEntry{Key: string(keyBuf), Offset: offset})
	}
	return index, nil
}
