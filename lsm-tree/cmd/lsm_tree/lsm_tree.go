package lsm_tree

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"

	"hw1/internal/common"
	"hw1/internal/sstable"
)

type LSMTree struct {
	sstables            [][]*sstable.SSTable
	ramComponent        map[string]struct{}
	ramComponentRemoved map[string]struct{}
	fileCnt             int
}

func New() *LSMTree {
	return &LSMTree{
		ramComponent:        make(map[string]struct{}),
		ramComponentRemoved: make(map[string]struct{}),
		sstables:            make([][]*sstable.SSTable, 1),
	}
}

func (l *LSMTree) Add(s string) error {
	l.ramComponent[s] = struct{}{}
	delete(l.ramComponentRemoved, s)

	if len(l.ramComponent)+len(l.ramComponentRemoved) == common.FirstLevelSize {
		err := l.flushRAMComponent()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFlushingRAMComponent, err)
		}
	}

	return nil
}

func (l *LSMTree) Delete(s string) error {
	l.ramComponentRemoved[s] = struct{}{}
	delete(l.ramComponent, s)

	if len(l.ramComponent)+len(l.ramComponentRemoved) == common.FirstLevelSize {
		err := l.flushRAMComponent()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFlushingRAMComponent, err)
		}
	}

	return nil
}

func (l *LSMTree) SearchKey(s string) (bool, error) {
	if _, ok := l.ramComponent[s]; ok {
		return true, nil
	}
	if _, ok := l.ramComponentRemoved[s]; ok {
		return false, nil
	}

	for level := range len(l.sstables) {
		for i := len(l.sstables[level]) - 1; i >= 0; i-- {
			searchResult, err := l.sstables[level][i].SearchKey(s)
			if err != nil {
				return false, fmt.Errorf("%w: %w", ErrSearching, err)
			}
			if searchResult == sstable.SearchResultFound {
				return true, nil
			}
			if searchResult == sstable.SearchResultRemoved {
				return false, nil
			}
		}
	}

	return false, nil
}

func (l *LSMTree) SearchRange(keyL string, keyR string) ([]string, error) {
	if keyL > keyR {
		return nil, fmt.Errorf("invalid key range")
	}

	resElements := make([]*sstable.TableElement, 0)
	for key := range l.ramComponent {
		if key >= keyL && key <= keyR {
			resElements = append(resElements, &sstable.TableElement{Value: key})
		}
	}
	for key := range l.ramComponentRemoved {
		if key >= keyL && key <= keyR {
			resElements = append(resElements, &sstable.TableElement{Value: key, IsTombstone: true})
		}
	}

	for level := range len(l.sstables) {
		for i := len(l.sstables[level]) - 1; i >= 0; i-- {
			searchResult, err := l.sstables[level][i].SearchRange(keyL, keyR)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrSearching, err)
			}
			resElements = append(resElements, searchResult...)
		}
	}

	sort.SliceStable(resElements, func(i, j int) bool {
		return resElements[i].Value < resElements[j].Value
	})

	res := make([]string, 0)
	for i, element := range resElements {
		if !element.IsTombstone && (i == 0 || resElements[i-1].Value != element.Value) {
			res = append(res, element.Value)
		}
	}

	return res, nil
}

func (l *LSMTree) Clear() {
	for level := range l.sstables {
		for _, sst := range l.sstables[level] {
			_ = sst.Remove()
		}
	}
}

func (l *LSMTree) flushRAMComponent() error {
	newSSTable, err := sstable.NewFromMap(
		filepath.Join(common.MetaDataDir, strconv.Itoa(l.fileCnt)),
		filepath.Join(common.DataDir, strconv.Itoa(l.fileCnt)),
		l.ramComponent,
		l.ramComponentRemoved,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCreatingSSTable, err)
	}

	l.sstables[0] = append(l.sstables[0], newSSTable)
	l.fileCnt++
	l.ramComponent = make(map[string]struct{})
	l.ramComponentRemoved = make(map[string]struct{})

	err = l.mergeSSTables()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMergingSSTables, err)
	}

	return nil
}

func (l *LSMTree) mergeSSTables() error {
	for level := 0; level < len(l.sstables); level++ {
		if len(l.sstables[level]) == common.MaxLevelSize {
			newSSTable, err := sstable.New(
				filepath.Join(common.MetaDataDir, strconv.Itoa(l.fileCnt)),
				filepath.Join(common.DataDir, strconv.Itoa(l.fileCnt)),
				l.sstables[level],
			)
			if err != nil {
				return err
			}

			for _, table := range l.sstables[level] {
				err = table.Remove()
				if err != nil {
					return fmt.Errorf("%w: %w", ErrRemovingSSTable, err)
				}
			}
			l.sstables[level] = make([]*sstable.SSTable, 0)

			if level == len(l.sstables)-1 {
				l.sstables = l.sstables[:level+1]
			}

			l.fileCnt++
			if len(l.sstables) == level+1 {
				l.sstables = append(l.sstables, make([]*sstable.SSTable, 0))
			}
			l.sstables[level+1] = append(l.sstables[level+1], newSSTable)
		}
	}

	return nil
}
