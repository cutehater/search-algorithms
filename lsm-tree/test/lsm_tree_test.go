package test

import (
	"math/rand"
	"slices"
	"testing"

	"hw1/cmd/lsm_tree"
	"hw1/internal/common"
)

const (
	minASCIISymbol       = 33
	rangeASCII           = 93
	maxRandLength        = 200
	elementsNumber       = common.MaxLevelSize * common.FirstLevelSize * 5
	elementsToFindNumber = 10000
)

func randString() string {
	length := rand.Intn(maxRandLength) + 8
	b := make([]byte, length)
	for i := range b {
		b[i] = byte(rand.Intn(rangeASCII) + minASCIISymbol)
	}
	return string(b)
}

func addElementsToTree(b *testing.B, LSMTree *lsm_tree.LSMTree) (elementsToFind []string) {
	b.Helper()

	elementsToFindIdxs := make(map[int]struct{})
	for i := 0; i < elementsToFindNumber; i++ {
		elementsToFindIdxs[rand.Intn(elementsNumber)] = struct{}{}
	}

	for i := 0; i < elementsNumber; i++ {
		s := randString()

		err := LSMTree.Add(s)
		if err != nil {
			b.Fatal(err)
		}

		if _, ok := elementsToFindIdxs[i]; ok {
			elementsToFind = append(elementsToFind, s)
		}
	}

	return elementsToFind
}

func BenchmarkAddElements(b *testing.B) {
	LSMTree := lsm_tree.New()
	defer LSMTree.Clear()

	for i := 0; i < b.N; i++ {
		err := LSMTree.Add(randString())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeleteElements(b *testing.B) {
	LSMTree := lsm_tree.New()
	defer LSMTree.Clear()
	elementsToRemove := addElementsToTree(b, LSMTree)

	for _, elem := range elementsToRemove {
		err := LSMTree.Delete(elem)
		if err != nil {
			b.Fatal(err)
		}
	}

	_ = addElementsToTree(b, LSMTree)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, err := LSMTree.SearchKey(elementsToRemove[i%len(elementsToRemove)])
		if err != nil {
			b.Fatal(err)
		}
		if ok {
			b.Fatal("Non-existing element found")
		}
	}
}

func BenchmarkSearchExistingElements(b *testing.B) {
	LSMTree := lsm_tree.New()
	defer LSMTree.Clear()
	elementsToFind := addElementsToTree(b, LSMTree)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, err := LSMTree.SearchKey(elementsToFind[i%len(elementsToFind)])
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("Existing element not found")
		}
	}
}

func BenchmarkSearchNonExistingElements(b *testing.B) {
	LSMTree := lsm_tree.New()
	defer LSMTree.Clear()
	_ = addElementsToTree(b, LSMTree)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, err := LSMTree.SearchKey(randString())
		if err != nil {
			b.Fatal(err)
		}
		if ok {
			b.Fatal("Non-existing element found")
		}
	}
}

func BenchmarkSearchKeyRange(b *testing.B) {
	LSMTree := lsm_tree.New()
	defer LSMTree.Clear()
	elementsToFind := addElementsToTree(b, LSMTree)
	slices.Sort(elementsToFind)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		L := elementsToFind[i%len(elementsToFind)]
		R := elementsToFind[(i+1)%len(elementsToFind)]
		if L > R {
			L = elementsToFind[(i-1)%len(elementsToFind)]
			R = elementsToFind[i%len(elementsToFind)]
		}

		res, err := LSMTree.SearchRange(L, R)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) < 2 {
			b.Fatal("Not enough elements found")
		}
	}
}
