# LSM-Tree with tiered compaction

## Prerequisites

- [Go](https://golang.org/doc/install) (version 1.22 or higher)

## Installation and running tests

```bash
git clone https://github.com/cutehater/search-algorithms.git
cd search-algorithms/lsm-tree
go mod tidy
cd test
go test -bench=.
