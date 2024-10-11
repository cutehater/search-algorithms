package lsm_tree

import "errors"

var (
	ErrCreatingSSTable      = errors.New("error creating sstable")
	ErrFlushingRAMComponent = errors.New("error flushing lsm tree RAM component")
	ErrMergingSSTables      = errors.New("error merging sstables")
	ErrRemovingSSTable      = errors.New("error removing sstable")
	ErrSearching            = errors.New("error searching sstable")
)
