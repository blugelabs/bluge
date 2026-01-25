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

	"github.com/RoaringBitmap/roaring"
)

func TestOpen(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	seg, closeF, err := createDiskSegment(buildTestSegment, filepath.Join(path, "ice"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	expectFields := map[string]struct{}{
		"_id":  {},
		"_all": {},
		"name": {},
		"desc": {},
		"tag":  {},
	}
	fields := seg.Fields()
	if len(fields) != len(expectFields) {
		t.Errorf("expected %d fields, only got %d", len(expectFields), len(fields))
	}
	for _, field := range fields {
		if _, ok := expectFields[field]; !ok {
			t.Errorf("got unexpected field: %s", field)
		}
	}

	docCount := seg.Count()
	if docCount != 1 {
		t.Errorf("expected count 1, got %d", docCount)
	}

	dict := expectFieldInSegment(t, seg, "_id")
	postingsItr := expectTermInDictionary(t, dict, "a")
	checkField(t, postingsItr, "_id", 1, false, true)

	// check the name field
	dict = expectFieldInSegment(t, seg, "name")
	postingsItr = expectTermInDictionary(t, dict, "wow")
	checkField(t, postingsItr, "name", 1, true, true)

	// check the _all field (composite)
	dict = expectFieldInSegment(t, seg, "_all")
	postingsItr = expectTermInDictionary(t, dict, "wow")
	checkField(t, postingsItr, "name", 5, true, true)

	// now try a field with array positions
	dict = expectFieldInSegment(t, seg, "tag")
	postingsItr = expectTermInDictionary(t, dict, "dark")
	checkField(t, postingsItr, "tag", 2, true, false)

	// now try and visit a document
	expectNumberOfStoredFields(t, seg, 0, 5)
}

func expectNumberOfStoredFields(t *testing.T, seg *segment, docNum uint64, expectedCount int) {
	var fieldValuesSeen int
	err := seg.VisitStoredFields(docNum, func(field string, value []byte) bool {
		fieldValuesSeen++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if fieldValuesSeen != expectedCount {
		t.Errorf("expected %d field values, got %d", expectedCount, fieldValuesSeen)
	}
}

func expectFieldInSegment(t *testing.T, seg *segment, field string) Dictionary {
	dict, err := seg.Dictionary(field)
	if err != nil {
		t.Fatal(err)
	}
	if dict == nil {
		t.Fatal("got nil dict, expected non-nil")
	}
	return dict
}

func expectTermInDictionary(t *testing.T, dict Dictionary, term string) PostingsIterator {
	postingsList, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postingsList == nil {
		t.Fatal("got nil postings list, expected non-nil")
	}
	var postingsItr PostingsIterator
	postingsItr, err = postingsList.Iterator(true, true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postingsItr == nil {
		t.Fatal("got nil iterator, expected non-nil")
	}
	return postingsItr
}

func checkField(t *testing.T, postingsItr PostingsIterator, expectField string, expectDecodedNorm int,
	expectLocations, checkStartEndPos bool) {
	count := 0
	nextPosting, err := postingsItr.Next()
	for nextPosting != nil && err == nil {
		count++
		if nextPosting.Frequency() != 1 {
			t.Errorf("expected frequency 1, got %d", nextPosting.Frequency())
		}
		if nextPosting.Number() != 0 {
			t.Errorf("expected doc number 0, got %d", nextPosting.Number())
		}
		decodedNorm := decodeNorm("", float32(nextPosting.Norm()))
		if decodedNorm != expectDecodedNorm {
			t.Errorf("expected decoded norm length %d, got %d", expectDecodedNorm, decodedNorm)
		}
		if expectLocations {
			var numLocs int
			for _, loc := range nextPosting.Locations() {
				numLocs++
				if loc.Field() != expectField {
					t.Errorf("expected loc field to be '%s', got '%s'", expectField, loc.Field())
				}
				if checkStartEndPos {
					if loc.Start() != 0 {
						t.Errorf("expected loc start to be 0, got %d", loc.Start())
					}
					if loc.End() != 3 {
						t.Errorf("expected loc end to be 3, got %d", loc.End())
					}
					if loc.Pos() != 1 {
						t.Errorf("expected loc pos to be 1, got %d", loc.Pos())
					}
				}
			}
			if numLocs != nextPosting.Frequency() {
				t.Errorf("expected %d locations, got %d", nextPosting.Frequency(), numLocs)
			}
		}

		nextPosting, err = postingsItr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Errorf("expected count to be 1, got %d", count)
	}
}

type testIdentifier string

func (i testIdentifier) Field() string {
	return "_id"
}

func (i testIdentifier) Term() []byte {
	return []byte(i)
}

func TestOpenMulti(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	segPath := filepath.Join(path, "ice")
	seg, closeF, err := createDiskSegment(buildTestSegmentMulti, segPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	if seg.Count() != 2 {
		t.Errorf("expected count 2, got %d", seg.Count())
	}

	// check the desc field
	dict := expectFieldInSegment(t, seg, "desc")
	postingsItr := expectTermInDictionary(t, dict, "thing")
	count := 0
	nextPosting, err := postingsItr.Next()
	for nextPosting != nil && err == nil {
		count++
		nextPosting, err = postingsItr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Errorf("expected count to be 2, got %d", count)
	}

	// get docnum of a
	exclude, err := seg.DocsMatchingTerms([]Term{testIdentifier("a")})
	if err != nil {
		t.Fatal(err)
	}

	// look for term 'thing' excluding doc 'a'
	postingsListExcluding, err := dict.PostingsList([]byte("thing"), exclude, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postingsListExcluding == nil {
		t.Fatal("got nil postings list, expected non-nil")
	}

	postingsListExcludingCount := postingsListExcluding.Count()
	if postingsListExcludingCount != 1 {
		t.Errorf("expected count from postings list to be 1, got %d", postingsListExcludingCount)
	}

	postingsItrExcluding, err := postingsListExcluding.Iterator(true, true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postingsItrExcluding == nil {
		t.Fatal("got nil iterator, expected non-nil")
	}

	count = 0
	nextPosting, err = postingsItrExcluding.Next()
	for nextPosting != nil && err == nil {
		count++
		nextPosting, err = postingsItrExcluding.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Errorf("expected count to be 1, got %d", count)
	}
}

func TestOpenMultiWithTwoChunks(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	segPath := filepath.Join(path, "ice")
	seg, closeF, err := createDiskSegment(func() (*segment, error) {
		return buildTestSegmentMultiWithChunkFactor(1)
	}, segPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	if seg.Count() != 2 {
		t.Errorf("expected count 2, got %d", seg.Count())
	}

	// check the desc field
	dict := expectFieldInSegment(t, seg, "desc")
	postingsItr := expectTermInDictionary(t, dict, "thing")
	count := 0
	nextPosting, err := postingsItr.Next()
	for nextPosting != nil && err == nil {
		count++
		nextPosting, err = postingsItr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Errorf("expected count to be 2, got %d", count)
	}

	// get docnum of a
	exclude, err := seg.DocsMatchingTerms([]Term{testIdentifier("a")})
	if err != nil {
		t.Fatal(err)
	}

	// look for term 'thing' excluding doc 'a'
	postingsListExcluding, err := dict.PostingsList([]byte("thing"), exclude, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postingsListExcluding == nil {
		t.Fatal("got nil postings list, expected non-nil")
	}

	postingsItrExcluding, err := postingsListExcluding.Iterator(true, true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postingsItrExcluding == nil {
		t.Fatal("got nil iterator, expected non-nil")
	}

	count = 0
	nextPosting, err = postingsItrExcluding.Next()
	for nextPosting != nil && err == nil {
		count++
		nextPosting, err = postingsItrExcluding.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Errorf("expected count to be 1, got %d", count)
	}
}

func TestSegmentVisitableDocValueFieldsList(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	segPath := filepath.Join(path, "ice")
	testSeg, err := buildTestSegmentMultiWithChunkFactor(1)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	err = persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	_, closeF, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}

	err = closeF()
	if err != nil {
		t.Fatalf("error closing segment: %v", err)
	}

	segPath2 := filepath.Join(path, "segment2.ice")
	testSeg2, _, err := buildTestSegmentWithDefaultFieldMapping(1)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	err = persistZipToFile(testSeg2, segPath2)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	seg, close2, err := openZipFromFile(segPath2)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}

	defer func() {
		cerr := close2()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	fields := []string{"desc", "name", "tag"}
	fieldTerms := make(map[string][]string)
	docValueReader, err := seg.DocumentValueReader(fields)
	if err != nil {
		t.Fatalf("err building document value reader: %v", err)
	}
	err = docValueReader.VisitDocumentValues(0, func(field string, term []byte) {
		fieldTerms[field] = append(fieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}

	expectedFieldTerms := map[string][]string{
		"name": {"wow"},
		"desc": {"some", "thing"},
		"tag":  {"cold"},
	}
	if !reflect.DeepEqual(fieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, fieldTerms)
	}
}

func TestSegmentDocsWithNonOverlappingFields(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, err := buildTestSegmentMultiWithDifferentFields(true, true)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	segPath := filepath.Join(path, "ice")
	err = persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	seg, closeF, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	if seg.Count() != 2 {
		t.Errorf("expected 2, got %d", seg.Count())
	}

	expectFields := map[string]struct{}{
		"_id":           {},
		"_all":          {},
		"name":          {},
		"dept":          {},
		"manages.id":    {},
		"manages.count": {},
		"reportsTo.id":  {},
	}

	fields := seg.Fields()
	if len(fields) != len(expectFields) {
		t.Errorf("expected %d fields, only got %d", len(expectFields), len(fields))
	}
	for _, field := range fields {
		if _, ok := expectFields[field]; !ok {
			t.Errorf("got unexpected field: %s", field)
		}
	}
}

func TestMergedSegmentDocsWithNonOverlappingFields(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg1, err := buildTestSegmentMultiWithDifferentFields(true, false)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	segPath := filepath.Join(path, "segment1.ice")
	err = persistZipToFile(testSeg1, segPath)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	testSeg2, err := buildTestSegmentMultiWithDifferentFields(false, true)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	segPath2 := filepath.Join(path, "segment2.ice")
	err = persistZipToFile(testSeg2, segPath2)
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	segment1, close1, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := close1()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	segment2, close2, err := openZipFromFile(segPath2)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := close2()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	segsToMerge := make([]Segment, 2)
	segsToMerge[0] = segment1
	segsToMerge[1] = segment2

	segPath3 := filepath.Join(path, "segment3.ice")
	nBytes, err := mergeSegments(segsToMerge, []*roaring.Bitmap{nil, nil}, segPath3)
	if err != nil {
		t.Fatal(err)
	}
	if nBytes == 0 {
		t.Fatalf("expected a non zero total_compaction_written_bytes")
	}

	segmentM, closeM, err := openZipFromFile(segPath3)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		cerr := closeM()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	if segmentM.Count() != 2 {
		t.Errorf("expected 2, got %d", segmentM.Count())
	}

	checkExpectedFields(t, segmentM.Fields())
}

func checkExpectedFields(t *testing.T, fields []string) {
	expectFields := map[string]struct{}{
		"_id":           {},
		"_all":          {},
		"name":          {},
		"dept":          {},
		"manages.id":    {},
		"manages.count": {},
		"reportsTo.id":  {},
	}
	if len(fields) != len(expectFields) {
		t.Errorf("expected %d fields, only got %d", len(expectFields), len(fields))
	}
	for _, field := range fields {
		if _, ok := expectFields[field]; !ok {
			t.Errorf("got unexpected field: %s", field)
		}
	}
}
