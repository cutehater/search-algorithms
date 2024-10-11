package sstable

import "errors"

var (
	ErrFileClosing     = errors.New("error closing file")
	ErrFileCreating    = errors.New("failed to create file")
	ErrFileSeeking     = errors.New("file seeking failed")
	ErrReadingFromFile = errors.New("failed to read from file")
	ErrSetFileOffset   = errors.New("failed to set file offset")
	ErrWritingBytes    = errors.New("failed writing bytes value")

	ErrBloomFilter    = errors.New("bloom filter error")
	ErrMergingTables  = errors.New("error merging sstables")
	ErrWritingElement = errors.New("error writing sstable element")
)
