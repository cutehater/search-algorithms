package sstable

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"hw1/internal/bloom_filter"
	"hw1/internal/common"
)

type SearchResult int

const (
	SearchResultNotFound SearchResult = iota
	SearchResultFound
	SearchResultRemoved
)

type SSTable struct {
	metaFile    *os.File
	dataFile    *os.File
	size        int
	bloomFilter bloom_filter.BloomFilter
}

func New(metaFilepath string, dataFilepath string, tablesToMerge []*SSTable) (*SSTable, error) {
	if len(tablesToMerge) != common.MaxLevelSize {
		return nil, fmt.Errorf("number of tables to merge is not equal to level size")
	}

	sizeEstimation := 0
	for _, table := range tablesToMerge {
		sizeEstimation += table.size
	}

	s := &SSTable{bloomFilter: bloom_filter.New(sizeEstimation)}

	var err error
	s.metaFile, err = createFile(metaFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}

	s.dataFile, err = createFile(dataFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}

	err = s.merge(tablesToMerge)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMergingTables, err)
	}

	return s, nil
}

func NewFromMap(metaFilepath string, dataFilepath string, valuesToAdd map[string]struct{}, valuesToDelete map[string]struct{}) (*SSTable, error) {
	s := &SSTable{bloomFilter: bloom_filter.New(common.FirstLevelSize)}

	var err error

	s.metaFile, err = createFile(metaFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}
	metaWriter := bufio.NewWriter(s.metaFile)
	defer metaWriter.Flush()

	s.dataFile, err = createFile(dataFilepath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileCreating, err)
	}
	dataWriter := bufio.NewWriter(s.dataFile)
	defer dataWriter.Flush()

	valuesSorted := make([]TableElement, len(valuesToAdd)+len(valuesToDelete))
	i := 0
	for value := range valuesToAdd {
		valuesSorted[i] = TableElement{Value: value}
		i++
	}
	for value := range valuesToDelete {
		valuesSorted[i] = TableElement{Value: value, IsTombstone: true}
		i++
	}
	sort.Slice(valuesSorted, func(i, j int) bool {
		return valuesSorted[i].Value < valuesSorted[j].Value
	})

	offset := 0
	for _, value := range valuesSorted {
		err = s.writeElement(metaWriter, dataWriter, &value, &offset)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *SSTable) SearchKey(key string) (SearchResult, error) {
	if ok, err := s.bloomFilter.CheckContains([]byte(key)); err != nil {
		return SearchResultNotFound, fmt.Errorf("%w: %w", ErrBloomFilter, err)
	} else if !ok {
		return SearchResultNotFound, nil
	}

	left, right := -1, s.size
	for right-left > 1 {
		mid := (left + right) / 2
		midKey, err := tableElementFromFileRandom(s.metaFile, s.dataFile, int64(mid))
		if err != nil {
			return SearchResultNotFound, err
		}
		if midKey.Value == key {
			if !midKey.IsTombstone {
				return SearchResultFound, nil
			} else {
				return SearchResultRemoved, nil
			}
		} else if midKey.Value < key {
			left = mid
		} else {
			right = mid
		}
	}

	return SearchResultNotFound, nil
}

func (s *SSTable) SearchRange(keyL string, keyR string) ([]*TableElement, error) {
	left, right := -1, s.size
	for right-left > 1 {
		mid := (left + right) / 2
		midKey, err := tableElementFromFileRandom(s.metaFile, s.dataFile, int64(mid))
		if err != nil {
			return nil, err
		}
		if midKey.Value < keyL {
			left = mid
		} else {
			right = mid
		}
	}
	L := left

	left, right = -1, s.size
	for right-left > 1 {
		mid := (left + right) / 2
		midKey, err := tableElementFromFileRandom(s.metaFile, s.dataFile, int64(mid))
		if err != nil {
			return nil, err
		}
		if midKey.Value <= keyR {
			left = mid
		} else {
			right = mid
		}
	}
	R := left

	_, err := setDataFileOffset(s.metaFile, s.dataFile, int64(L), true)
	if err != nil {
		return nil, err
	}
	metaReader := bufio.NewReader(s.metaFile)
	dataReader := bufio.NewReader(s.dataFile)

	result := make([]*TableElement, R-L+1)
	for i := 0; i < R-L+1; i++ {
		element, err := tableElementFromFileConsecutive(metaReader, dataReader)
		if err != nil {
			return nil, err
		}

		result[i] = element
	}

	return result, nil
}

func (s *SSTable) Close() error {
	err := s.metaFile.Close()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFileClosing, err)
	}

	err = s.dataFile.Close()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFileClosing, err)
	}

	return nil
}

func (s *SSTable) Remove() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = os.Remove(s.metaFile.Name())
	if err != nil {
		return err
	}

	err = os.Remove(s.dataFile.Name())
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTable) merge(tablesToMerge []*SSTable) error {
	queue := priorityQueue{}
	heap.Init(&queue)

	metaWriter := bufio.NewWriter(s.metaFile)
	defer metaWriter.Flush()
	dataWriter := bufio.NewWriter(s.dataFile)
	defer dataWriter.Flush()

	metaReaders := make([]*bufio.Reader, len(tablesToMerge))
	dataReaders := make([]*bufio.Reader, len(tablesToMerge))

	for i := 0; i < len(tablesToMerge); i++ {
		if _, err := setDataFileOffset(tablesToMerge[i].metaFile, tablesToMerge[i].dataFile, 0, true); err != nil {
			return err
		}
		metaReaders[i] = bufio.NewReader(tablesToMerge[i].metaFile)
		dataReaders[i] = bufio.NewReader(tablesToMerge[i].dataFile)

		element, err := tableElementFromFileConsecutive(metaReaders[i], dataReaders[i])
		if err != nil {
			return err
		}
		heap.Push(&queue, &mergeItem{
			value:     *element,
			readerIdx: i,
		})
	}

	var lastInserted string
	offset := 0
	for queue.Len() > 0 {
		element := heap.Pop(&queue).(*mergeItem)

		if offset == 0 || lastInserted != element.value.Value {
			err := s.writeElement(metaWriter, dataWriter, &element.value, &offset)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrWritingElement, err)
			}
		}

		newElement, err := tableElementFromFileConsecutive(metaReaders[element.readerIdx], dataReaders[element.readerIdx])
		if err != nil && err != io.EOF {
			return err
		}
		if err != io.EOF {
			heap.Push(&queue, &mergeItem{
				value:     *newElement,
				readerIdx: element.readerIdx,
			})
		}

		lastInserted = element.value.Value
	}

	return nil
}

func (s *SSTable) writeElement(metaDataWriter *bufio.Writer, dataWriter *bufio.Writer, element *TableElement, offset *int) error {
	elementBytes, err := element.toBytes()
	if err != nil {
		return err
	}
	if _, err = dataWriter.Write(elementBytes); err != nil {
		return err
	}

	elementMetaData := meta{
		offset: int64(*offset),
		length: int64(len(elementBytes) - 1),
	}
	elementMetaDataBytes, err := elementMetaData.toBytes()
	if err != nil {
		return err
	}
	_, err = metaDataWriter.Write(elementMetaDataBytes)
	if err != nil {
		return err
	}

	err = s.bloomFilter.Add([]byte(element.Value))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBloomFilter, err)
	}

	s.size++
	*offset += len(elementBytes)

	return nil
}

func createFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0770); err != nil {
		return nil, err
	}
	return os.Create(path)
}
