package lsm

import (
	"hash/fnv"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type BloomFilter struct {
	bitSet []bool
	size   int
	seeds  []uint64
}

func NewBloomFilter(size int, hashCount int) *BloomFilter {
	bitSet := make([]bool, size)
	seeds := make([]uint64, hashCount)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < hashCount; i++ {
		seeds[i] = rng.Uint64()
	}

	return &BloomFilter{
		bitSet: bitSet,
		size:   size,
		seeds:  seeds,
	}
}

func NewOptimalBloomFilter(numElements int, falsePositiveRate float64) *BloomFilter {
	if numElements <= 0 {
		numElements = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}
	m := -float64(numElements) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
	k := (m / float64(numElements)) * math.Ln2
	if m < 8 {
		m = 8
	}
	if k < 1 {
		k = 1
	}
	return NewBloomFilter(int(math.Ceil(m)), int(math.Ceil(k)))
}

func (bf *BloomFilter) Add(item string) {
	for _, seed := range bf.seeds {
		hashFunc := fnv.New64a()
		hashFunc.Write([]byte(strconv.FormatUint(seed, 10)))
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.size)
		bf.bitSet[index] = true
	}
}

func (bf *BloomFilter) Check(item string) bool {
	for _, seed := range bf.seeds {
		hashFunc := fnv.New64a()
		hashFunc.Write([]byte(strconv.FormatUint(seed, 10)))
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.size)
		if !bf.bitSet[index] {
			return false
		}
	}
	return true
}
