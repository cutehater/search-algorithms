package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type meta struct {
	offset int64
	length int64
}

func (m *meta) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, m.offset); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, m.length); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWritingBytes, err)
	}

	return buf.Bytes(), nil
}

func metaFromBytes(reader io.Reader) (*meta, error) {
	var offset, length int64

	if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: %w", ErrReadingFromFile, err)
	}

	return &meta{
		offset: offset,
		length: length,
	}, nil
}

func setMetaFileOffset(file *os.File, elementIdx int64) error {
	offset := elementIdx * int64(binary.Size(meta{}))
	_, err := file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFileSeeking, err)
	}
	return nil
}
