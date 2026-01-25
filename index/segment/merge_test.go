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
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestMerge(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, _ := buildTestSegmentMulti()
	segPath := filepath.Join(path, "ice")
	err := persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatal(err)
	}

	testSeg2, _, _ := buildTestSegmentMulti2()
	segPath2 := filepath.Join(path, "segment2.ice")
	err = persistZipToFile(testSeg2, segPath2)
	if err != nil {
		t.Fatal(err)
	}

	seg, closeF, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segment2, close2, err := openZipFromFile(segPath2)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := close2()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segsToMerge := make([]Segment, 2)
	segsToMerge[0] = seg
	segsToMerge[1] = segment2

	segPath3 := filepath.Join(path, "segment3.ice")
	_, err = mergeSegments(segsToMerge, []*roaring.Bitmap{nil, nil}, segPath3)
	if err != nil {
		t.Fatal(err)
	}

	seg3, close3, err := openZipFromFile(segPath3)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		cerr := close3()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	if seg3.Count() != 4 {
		t.Fatalf("wrong count")
	}
	if len(seg3.Fields()) != 5 {
		t.Fatalf("wrong # fields: %#v\n", seg3.Fields())
	}

	testMergeWithSelf(t, seg3, 4)
}

func TestMergeWithEmptySegment(t *testing.T) {
	testMergeWithEmptySegments(t, true, 1)
}

func TestMergeWithEmptySegments(t *testing.T) {
	testMergeWithEmptySegments(t, true, 5)
}

func TestMergeWithEmptySegmentFirst(t *testing.T) {
	testMergeWithEmptySegments(t, false, 1)
}

func TestMergeWithEmptySegmentsFirst(t *testing.T) {
	testMergeWithEmptySegments(t, false, 5)
}

func testMergeWithEmptySegments(t *testing.T, before bool, numEmptySegments int) {
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
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	var segsToMerge []Segment

	if before {
		segsToMerge = append(segsToMerge, seg)
	}

	for i := 0; i < numEmptySegments; i++ {
		fname := fmt.Sprintf("segment-empty-%d.ice", i)
		segPath := filepath.Join(path, fname)
		createAndPersistEmptySegment(t, segPath)

		var emptyFileSegment *segment
		var emptyClose closeFunc
		emptyFileSegment, emptyClose, err = openZipFromFile(segPath)
		if err != nil {
			t.Fatalf("error opening segment: %v", err)
		}
		defer func(emptyClose closeFunc) {
			cerr := emptyClose()
			if cerr != nil {
				t.Fatalf("error closing segment: %v", err)
			}
		}(emptyClose)

		segsToMerge = append(segsToMerge, emptyFileSegment)
	}

	if !before {
		segsToMerge = append(segsToMerge, seg)
	}

	segPath3 := filepath.Join(path, "segment3.ice")

	drops := make([]*roaring.Bitmap, len(segsToMerge))

	_, err = mergeSegments(segsToMerge, drops, segPath3)
	if err != nil {
		t.Fatal(err)
	}

	segCur, closeCur, err := openZipFromFile(segPath3)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		cerr := closeCur()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	if segCur.Count() != 2 {
		t.Fatalf("wrong count, numEmptySegments: %d, got count: %d", numEmptySegments, segCur.Count())
	}
	if len(segCur.Fields()) != 5 {
		t.Fatalf("wrong # fields: %#v\n", segCur.Fields())
	}

	testMergeWithSelf(t, segCur, 2)
}

func createAndPersistEmptySegment(t *testing.T, path string) {
	zipData, _, err := NewZip([]Document{}, encodeNorm)
	if err != nil {
		t.Fatal(err)
	}
	emptySegment, err := LoadBytes(zipData)
	if err != nil {
		t.Fatal(err)
	}
	err = persistZipToFile(emptySegment, path)
	if err != nil {
		t.Fatal(err)
	}
}

func testMergeWithSelf(t *testing.T, segCur Segment, expectedCount uint64) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	// trying merging the segment with itself for a few rounds
	var diffs []string

	for i := 0; i < 10; i++ {
		fname := fmt.Sprintf("segment-self-%d.ice", i)

		segPath := filepath.Join(path, fname)

		segsToMerge := make([]Segment, 1)
		segsToMerge[0] = segCur

		_, err := mergeSegments(segsToMerge, []*roaring.Bitmap{nil, nil}, segPath)
		if err != nil {
			t.Fatal(err)
		}

		segNew, closeNew, err := openZipFromFile(segPath)
		if err != nil {
			t.Fatalf("error opening merged segment: %v", err)
		}
		defer func(close closeFunc) {
			cerr := close()
			if cerr != nil {
				t.Fatalf("error closing segment: %v", err)
			}
		}(closeNew)

		if segNew.Count() != expectedCount {
			t.Fatalf("wrong count")
		}
		if len(segNew.Fields()) != 5 {
			t.Fatalf("wrong # fields: %#v\n", segNew.Fields())
		}

		diff := compareSegments(segCur, segNew)
		if diff != "" {
			diffs = append(diffs, fname+" is different than previous:\n"+diff)
		}

		segCur = segNew
	}

	if len(diffs) > 0 {
		t.Errorf("mismatches after repeated self-merging: %v", strings.Join(diffs, "\n"))
	}
}

func compareSegments(a, b Segment) string {
	var rv []string

	if a.Count() != b.Count() {
		return "counts"
	}

	afields := append([]string(nil), a.Fields()...)
	bfields := append([]string(nil), b.Fields()...)
	sort.Strings(afields)
	sort.Strings(bfields)
	if !reflect.DeepEqual(afields, bfields) {
		return "fields"
	}

	for _, fieldName := range afields {
		var doneReason string
		var done bool
		rv, doneReason, done = compareSegmentsField(a, b, fieldName, rv)
		if done {
			return doneReason
		}
	}

	return strings.Join(rv, "\n")
}

func compareSegmentsField(a, b Segment, fieldName string, rv []string) (errors []string,
	doneReason string, done bool) {
	adict, err := a.Dictionary(fieldName)
	if err != nil {
		return nil, fmt.Sprintf("adict err: %v", err), true
	}
	bdict, err := b.Dictionary(fieldName)
	if err != nil {
		return nil, fmt.Sprintf("bdict err: %v", err), true
	}

	adictLen := getDictFSTLen(adict)
	bdictLen := getDictFSTLen(bdict)
	if adictLen != bdictLen {
		rv = append(rv, fmt.Sprintf("field %s, dict fst Len()'s  different: %v %v",
			fieldName, adictLen, bdictLen))
	}

	aitr := adict.Iterator(nil, nil, nil)
	bitr := bdict.Iterator(nil, nil, nil)
	rv = compareSegmentsDictionaryIterators(a, b, fieldName, rv, aitr, bitr, adict, bdict)
	return rv, "", false
}

func getDictFSTLen(dict Dictionary) int {
	switch d := dict.(type) {
	case *dictionary:
		return d.fst.Len()
	default:
		return 0
	}
}

func getDictPostingsList(dict Dictionary, term []byte) (PostingsList, error) {
	switch d := dict.(type) {
	case *dictionary:
		return d.postingsList(term, nil, nil)
	default:
		return dict.PostingsList(term, nil, nil)
	}
}

func compareSegmentsDictionaryIterators(a, b Segment, fieldName string, rv []string,
	aitr, bitr DictionaryIterator, adict, bdict Dictionary) []string {
	for {
		anext, aerr := aitr.Next()
		bnext, berr := bitr.Next()
		if aerr != berr {
			rv = append(rv, fmt.Sprintf("field %s, dict iterator Next() errors different: %v %v",
				fieldName, aerr, berr))
			break
		}
		if !reflect.DeepEqual(anext, bnext) {
			rv = append(rv, fmt.Sprintf("field %s, dict iterator Next() results different: %#v %#v",
				fieldName, anext, bnext))
			// keep going to try to see more diff details at the postingsList level
		}
		if aerr != nil || anext == nil ||
			berr != nil || bnext == nil {
			break
		}

		rv = compareSegmentsDictionaryEntry(a, b, fieldName, rv, anext, bnext, adict, bdict)
	}
	return rv
}

func compareSegmentsDictionaryEntry(a, b Segment, fieldName string, rv []string,
	anext, bnext DictionaryEntry, adict, bdict Dictionary) []string {
	for _, next := range []DictionaryEntry{anext, bnext} {
		if next == nil {
			continue
		}

		aplist, aerr := getDictPostingsList(adict, []byte(next.Term()))
		bplist, berr := getDictPostingsList(bdict, []byte(next.Term()))
		if aerr != berr {
			rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList() errors different: %v %v",
				fieldName, next.Term(), aerr, berr))
		}

		if (aplist != nil) != (bplist != nil) {
			rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList() results different: %v %v",
				fieldName, next.Term(), aplist, bplist))
			break
		}

		if aerr != nil || aplist == nil ||
			berr != nil || bplist == nil {
			break
		}

		if aplist.Count() != bplist.Count() {
			rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList().Count()'s different: %v %v",
				fieldName, next.Term(), aplist.Count(), bplist.Count()))
		}

		apitr, err := aplist.Iterator(true, true, true, nil)
		if err != nil {
			rv = append(rv, fmt.Sprintf("error getting a iterator: %v", err))
			break
		}
		bpitr, err := bplist.Iterator(true, true, true, nil)
		if err != nil {
			rv = append(rv, fmt.Sprintf("error getting b iterator: %v", err))
			break
		}
		if (apitr != nil) != (bpitr != nil) {
			rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList.Iterator() results different: %v %v",
				fieldName, next.Term(), apitr, bpitr))
			break
		}

		rv = compareSegmentPostingIterator(a, b, fieldName, rv, apitr, bpitr, next)
	}
	return rv
}

func compareSegmentPostingIterator(a, b Segment, fieldName string, rv []string, apitr, bpitr PostingsIterator,
	next DictionaryEntry) []string {
	for {
		apitrn, aerr := apitr.Next()
		bpitrn, berr := bpitr.Next()
		if aerr != berr {
			rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() errors different: %v %v",
				fieldName, next.Term(), aerr, berr))
		}

		if (apitrn != nil) != (bpitrn != nil) {
			rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() results different: %v %v",
				fieldName, next.Term(), apitrn, bpitrn))
			break
		}

		if aerr != nil || apitrn == nil ||
			berr != nil || bpitrn == nil {
			break
		}

		rv = compareSegmentPosting(apitrn, bpitrn, rv, fieldName, next)

		for loci, aloc := range apitrn.Locations() {
			bloc := bpitrn.Locations()[loci]

			if (aloc != nil) != (bloc != nil) {
				rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() loc different: %v %v",
					fieldName, next.Term(), aloc, bloc))
				break
			}

			if aloc.Field() != bloc.Field() ||
				aloc.Start() != bloc.Start() ||
				aloc.End() != bloc.End() ||
				aloc.Pos() != bloc.Pos() {
				rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() loc details different: %v %v",
					fieldName, next.Term(), aloc, bloc))
			}
		}

		if fieldName == _idFieldName {
			rv = compareSegmentStoredFields(a, b, next, apitrn, bpitrn, rv)
		}
	}
	return rv
}

func compareSegmentStoredFields(a, b Segment, next DictionaryEntry, apitrn, bpitrn Posting, rv []string) []string {
	docID := next.Term()
	docNumA := apitrn.Number()
	docNumB := bpitrn.Number()
	afields := map[string]interface{}{}
	err := a.VisitStoredFields(apitrn.Number(),
		func(field string, value []byte) bool {
			afields[field+"-value"] = append([]byte(nil), value...)
			return true
		})
	if err != nil {
		rv = append(rv, fmt.Sprintf("a.VisitStoredFields err: %v", err))
	}
	bfields := map[string]interface{}{}
	err = b.VisitStoredFields(bpitrn.Number(),
		func(field string, value []byte) bool {
			bfields[field+"-value"] = append([]byte(nil), value...)
			return true
		})
	if err != nil {
		rv = append(rv, fmt.Sprintf("b.VisitStoredFields err: %v", err))
	}
	if !reflect.DeepEqual(afields, bfields) {
		rv = append(rv, fmt.Sprintf("afields != bfields,"+
			" id: %s, docNumA: %d, docNumB: %d,"+
			" afields: %#v, bfields: %#v",
			docID, docNumA, docNumB, afields, bfields))
	}
	return rv
}

func compareSegmentPosting(apitrn, bpitrn Posting, rv []string, fieldName string, next DictionaryEntry) []string {
	if apitrn.Number() != bpitrn.Number() {
		rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Number()'s different: %v %v",
			fieldName, next.Term(), apitrn.Number(), bpitrn.Number()))
	}

	if apitrn.Frequency() != bpitrn.Frequency() {
		rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Frequency()'s different: %v %v",
			fieldName, next.Term(), apitrn.Frequency(), bpitrn.Frequency()))
	}

	if apitrn.Norm() != bpitrn.Norm() {
		rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Norm()'s different: %v %v",
			fieldName, next.Term(), apitrn.Norm(), bpitrn.Norm()))
	}

	if len(apitrn.Locations()) != len(bpitrn.Locations()) {
		rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Locations() len's different: %v %v",
			fieldName, next.Term(), len(apitrn.Locations()), len(bpitrn.Locations())))
	}
	return rv
}

func TestMergeAndDrop(t *testing.T) {
	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()
	docsToDrop[0].AddInt(1)
	docsToDrop[1] = roaring.NewBitmap()
	docsToDrop[1].AddInt(1)
	testMergeAndDrop(t, docsToDrop)
}

func TestMergeAndDropAllFromOneSegment(t *testing.T) {
	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()
	docsToDrop[0].AddInt(0)
	docsToDrop[0].AddInt(1)
	docsToDrop[1] = roaring.NewBitmap()
	testMergeAndDrop(t, docsToDrop)
}

func testMergeAndDrop(t *testing.T, docsToDrop []*roaring.Bitmap) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, _ := buildTestSegmentMulti()
	segPath := filepath.Join(path, "ice")
	err := persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatal(err)
	}
	seg, closeF, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	testSeg2, _, _ := buildTestSegmentMulti2()
	segPath2 := filepath.Join(path, "segment2.ice")
	err = persistZipToFile(testSeg2, segPath2)
	if err != nil {
		t.Fatal(err)
	}

	segment2, close2, err := openZipFromFile(segPath2)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := close2()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segsToMerge := make([]Segment, 2)
	segsToMerge[0] = seg
	segsToMerge[1] = segment2

	testMergeAndDropSegments(t, segsToMerge, docsToDrop, 2)
}

func TestMergeWithUpdates(t *testing.T) {
	segmentDocIds := [][]string{
		{"a", "b"},
		{"b", "c"}, // doc "b" updated
	}

	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()
	docsToDrop[0].AddInt(1) // doc "b" updated
	docsToDrop[1] = roaring.NewBitmap()

	testMergeWithUpdates(t, segmentDocIds, docsToDrop, 3)
}

func TestMergeWithUpdatesOnManySegments(t *testing.T) {
	segmentDocIds := [][]string{
		{"a", "b"},
		{"b", "c"}, // doc "b" updated
		{"c", "d"}, // doc "c" updated
		{"d", "e"}, // doc "d" updated
	}

	docsToDrop := makeDeletedBitmaps(len(segmentDocIds))
	docsToDrop[0].AddInt(1) // doc "b" updated
	docsToDrop[1].AddInt(1) // doc "c" updated
	docsToDrop[2].AddInt(1) // doc "d" updated

	testMergeWithUpdates(t, segmentDocIds, docsToDrop, 5)
}

func makeDeletedBitmaps(num int) []*roaring.Bitmap {
	rv := make([]*roaring.Bitmap, num)
	for i := range rv {
		rv[i] = roaring.NewBitmap()
	}
	return rv
}

func TestMergeWithUpdatesOnOneDoc(t *testing.T) {
	segmentDocIds := [][]string{
		{"a", "b"},
		{"a", "c"}, // doc "a" updated
		{"a", "d"}, // doc "a" updated
		{"a", "e"}, // doc "a" updated
	}

	docsToDrop := makeDeletedBitmaps(len(segmentDocIds))
	docsToDrop[0].AddInt(0) // doc "a" updated
	docsToDrop[1].AddInt(0) // doc "a" updated
	docsToDrop[2].AddInt(0) // doc "a" updated

	testMergeWithUpdates(t, segmentDocIds, docsToDrop, 5)
}

func testMergeWithUpdates(t *testing.T, segmentDocIds [][]string, docsToDrop []*roaring.Bitmap, expectedNumDocs uint64) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	var segsToMerge []Segment

	// convert segmentDocIds to segsToMerge
	for i, docIds := range segmentDocIds {
		fname := fmt.Sprintf("segment%d.ice", i)

		segPath := filepath.Join(path, fname)

		testSeg, _, _ := buildTestSegmentMultiHelper(docIds)
		err := persistZipToFile(testSeg, segPath)
		if err != nil {
			t.Fatal(err)
		}
		seg, closeF, err := openZipFromFile(segPath)
		if err != nil {
			t.Fatalf("error opening segment: %v", err)
		}
		defer func(closeF closeFunc) {
			cerr := closeF()
			if cerr != nil {
				t.Fatalf("error closing segment: %v", err)
			}
		}(closeF)

		segsToMerge = append(segsToMerge, seg)
	}

	testMergeAndDropSegments(t, segsToMerge, docsToDrop, expectedNumDocs)
}

func testMergeAndDropSegments(t *testing.T, segsToMerge []Segment, docsToDrop []*roaring.Bitmap, expectedNumDocs uint64) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	segPath := filepath.Join(path, "segment-merged.ice")
	_, err := mergeSegments(segsToMerge, docsToDrop, segPath)
	if err != nil {
		t.Fatal(err)
	}

	segm, closem, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		cerr := closem()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	if segm.Count() != expectedNumDocs {
		t.Fatalf("wrong count, got: %d, wanted: %d", segm.Count(), expectedNumDocs)
	}
	if len(segm.Fields()) != 5 {
		t.Fatalf("wrong # fields: %#v\n", segm.Fields())
	}

	testMergeWithSelf(t, segm, expectedNumDocs)
}

func TestMergeBytesWritten(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	testSeg, _ := buildTestSegmentMulti()
	segPath := filepath.Join(path, "ice")
	err := persistZipToFile(testSeg, segPath)
	if err != nil {
		t.Fatal(err)
	}

	testSeg2, _, _ := buildTestSegmentMulti2()
	segPath2 := filepath.Join(path, "segment2.ice")
	err = persistZipToFile(testSeg2, segPath2)
	if err != nil {
		t.Fatal(err)
	}

	seg, closeF, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := closeF()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segment2, close2, err := openZipFromFile(segPath2)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := close2()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segsToMerge := make([]Segment, 2)
	segsToMerge[0] = seg
	segsToMerge[1] = segment2

	segPath3 := filepath.Join(path, "segment3.ice")
	nBytes, err := mergeSegments(segsToMerge, []*roaring.Bitmap{nil, nil}, segPath3)
	if err != nil {
		t.Fatal(err)
	}

	if nBytes == 0 {
		t.Fatalf("expected a non zero total_compaction_written_bytes")
	}

	seg3, close3, err := openZipFromFile(segPath3)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		cerr := close3()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	if seg3.Count() != 4 {
		t.Fatalf("wrong count")
	}
	if len(seg3.Fields()) != 5 {
		t.Fatalf("wrong # fields: %#v\n", seg3.Fields())
	}

	testMergeWithSelf(t, seg3, 4)
}

func TestUnder32Bits(t *testing.T) {
	if !under32Bits(0) || !under32Bits(uint64(0x7fffffff)) {
		t.Errorf("under32Bits bad")
	}
	if under32Bits(uint64(0x80000000)) || under32Bits(uint64(0x80000001)) {
		t.Errorf("under32Bits wrong")
	}
}
