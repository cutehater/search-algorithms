package bloom_filter

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"

	"github.com/bits-and-blooms/bitset"
)

const (
	bitsNumber = 500000
)

type BloomFilter interface {
	Add(element []byte) error
	CheckContains(element []byte) (bool, error)
}

type bloomFilter struct {
	hashFuncs []hash.Hash64
	filter    *bitset.BitSet
}

func New(elementsNumber int) *bloomFilter {
	k := getOptimalHashFuncsNumber(elementsNumber)
	bloomFilter := &bloomFilter{
		hashFuncs: make([]hash.Hash64, k),
		filter:    bitset.New(bitsNumber),
	}

	for i := 0; i < k; i++ {
		bloomFilter.hashFuncs[i] = fnv.New64a()
	}

	return bloomFilter
}

func (b *bloomFilter) Add(element []byte) error {
	for _, h := range b.hashFuncs {
		h.Reset()
		_, err := h.Write(element)
		if err != nil {
			return fmt.Errorf("adding element to bloom filter: %w", err)
		}

		index := indexFromHash(h.Sum64())
		b.filter.Set(index)
	}
	return nil
}

func (b *bloomFilter) CheckContains(element []byte) (bool, error) {
	for _, h := range b.hashFuncs {
		h.Reset()
		_, err := h.Write(element)
		if err != nil {
			return false, fmt.Errorf("checking element in bloom filter: %w", err)
		}

		index := indexFromHash(h.Sum64())
		if !b.filter.Test(index) {
			return false, nil
		}
	}
	return true, nil
}

func getOptimalHashFuncsNumber(elementsNumber int) int {
	return int(math.Ceil(bitsNumber / float64(elementsNumber) * math.Ln2))
}

func indexFromHash(hash uint64) uint {
	return uint(hash % uint64(bitsNumber))
}
