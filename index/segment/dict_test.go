//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segment

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/blevesearch/vellum/levenshtein"
)

func buildTestSegmentForDict() (*segment, error) {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("desc", "apple ball cat dog egg fish bat", true, true, false),
	}

	results := []Document{doc}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		return nil, err
	}
	return LoadBytes(zipData)
}

func TestDictionary(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, err := buildTestSegmentForDict()
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	segPath := filepath.Join(path, "ice")
	err = persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	seg, closeFunc, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeFunc()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	// test basic full iterator
	expected := []string{"apple", "ball", "bat", "cat", "dog", "egg", "fish"}
	itr := dict.Iterator(nil, nil, nil)
	checkIterator(t, itr, expected)

	// test prefixes iterator
	expected = []string{"ball", "bat"}
	kBeg := []byte("b")
	kEnd := incrementBytes(kBeg)
	itr = dict.Iterator(nil, kBeg, kEnd)
	checkIterator(t, itr, expected)

	// test range iterator
	expected = []string{"cat", "dog"}
	itr = dict.Iterator(nil, []byte("cat"), []byte("egg"))
	checkIterator(t, itr, expected)
}

func checkIterator(t *testing.T, itr DictionaryIterator, expected []string) {
	var got []string
	next, err := itr.Next()
	for next != nil && err == nil {
		got = append(got, next.Term())
		next, err = itr.Next()
	}
	if err != nil {
		t.Fatalf("dict itr error: %v", err)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %v, got: %v", expected, got)
	}
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i]++
		if rv[i] != 0 {
			// didn't overflow, so stop
			break
		}
	}
	return rv
}

func TestDictionaryError(t *testing.T) {
	builders := make(map[int]*levenshtein.LevenshteinAutomatonBuilder)
	for i := 1; i <= 3; i++ {
		lb, err := levenshtein.NewLevenshteinAutomatonBuilder(uint8(i), false)
		if err != nil {
			t.Errorf("NewLevenshteinAutomatonBuilder(%d, false) failed, err: %v", i, err)
		}
		builders[i] = lb
	}

	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, err := buildTestSegmentForDict()
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	segPath := filepath.Join(path, "ice")
	err = persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	seg, closeFunc, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeFunc()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		editDistance    int
		query           string
		numValsExpected int
	}{
		{
			query:           "summer",
			editDistance:    2,
			numValsExpected: 0,
		},
		{
			query:           "cat",
			editDistance:    1,
			numValsExpected: 2, // cat & bat
		},
		{
			query:           "cat",
			editDistance:    2,
			numValsExpected: 2, // cat & bat
		},
		{
			query:           "cat",
			editDistance:    3,
			numValsExpected: 5,
		},
	}

	for _, test := range tests {
		lb := builders[test.editDistance]
		a, err := lb.BuildDfa(test.query, uint8(test.editDistance))
		if err != nil {
			t.Fatalf("error building dfa: %v", err)
		}
		itr := dict.Iterator(a, nil, nil)
		count := 0
		next, err := itr.Next()
		for err == nil && next != nil {
			count++
			next, err = itr.Next()
		}
		if err != nil {
			t.Fatalf("unexpected err from dict iterator: %v", err)
		}
		if count != test.numValsExpected {
			t.Errorf("expected to see %d vals, saw %d", test.numValsExpected, count)
		}
	}
}

func TestDictionaryBug1156(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, err := buildTestSegmentForDict()
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	segPath := filepath.Join(path, "ice")
	err = persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	seg, closeFunc, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeFunc()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	// test range iterator
	expected := []string{"cat", "dog", "egg", "fish"}
	var got []string
	itr := dict.Iterator(nil, []byte("cat"), nil)
	next, err := itr.Next()
	for next != nil && err == nil {
		got = append(got, next.Term())
		next, err = itr.Next()
	}
	if err != nil {
		t.Fatalf("dict itr error: %v", err)
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %v, got: %v", expected, got)
	}
}
