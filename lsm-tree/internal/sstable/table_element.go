package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type TableElement struct {
	Value       string
	IsTombstone bool
}

func (e *TableElement) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	if _, err := buf.WriteString(e.Value); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, e.IsTombstone); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}

	return buf.Bytes(), nil
}

func tableElementFromFileRandom(metaFile *os.File, dataFile *os.File, elementIdx int64) (*TableElement, error) {
	elementMeta, err := setDataFileOffset(metaFile, dataFile, elementIdx, false)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSetFileOffset, err)
	}

	dataReader := bufio.NewReader(dataFile)
	element, err := tableElementFromBytes(dataReader, int(elementMeta.length))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}

	return element, nil
}

func tableElementFromFileConsecutive(metaReader *bufio.Reader, dataReader *bufio.Reader) (*TableElement, error) {
	elementMeta, err := metaFromBytes(metaReader)
	if err != nil {
		return nil, err
	}

	element, err := tableElementFromBytes(dataReader, int(elementMeta.length))
	if err != nil {
		return nil, err
	}

	return element, nil
}

func tableElementFromBytes(reader io.Reader, valueLength int) (*TableElement, error) {
	valueBytes := make([]byte, valueLength)
	readBytes := 0

	for readBytes < valueLength {
		n, err := reader.Read(valueBytes[readBytes:])
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
		}
		readBytes += n
	}

	tombstoneByte := make([]byte, 1)
	_, err := reader.Read(tombstoneByte)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}

	return &TableElement{
		Value:       string(valueBytes),
		IsTombstone: tombstoneByte[0] == 1,
	}, nil
}

func setDataFileOffset(metaFile *os.File, dataFile *os.File, elementIdx int64, setCorrectMetaOffset bool) (*meta, error) {
	err := setMetaFileOffset(metaFile, elementIdx)
	if err != nil {
		return nil, err
	}

	metaReader := bufio.NewReader(metaFile)
	elementMeta, err := metaFromBytes(metaReader)
	if err != nil {
		return nil, err
	}

	_, err = dataFile.Seek(elementMeta.offset, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFileSeeking, err)
	}

	if setCorrectMetaOffset {
		err = setMetaFileOffset(metaFile, elementIdx)
		if err != nil {
			return nil, err
		}
	}

	return elementMeta, nil
}
