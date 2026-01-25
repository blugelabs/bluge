//  Copyright (c) 2020 The Bluge Authors.
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
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestMergeBasic(t *testing.T) {
	// Create first ZIP segment
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "cool", true, true, false),
		NewFakeField("desc", "other stuff", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	zipData1, _, err := NewZip([]Document{doc1, doc2}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating first ZIP segment: %v", err)
	}

	seg1, err := LoadBytes(zipData1)
	if err != nil {
		t.Fatalf("error loading first ZIP segment: %v", err)
	}

	// Create second ZIP segment
	doc3 := &FakeDocument{
		NewFakeField("_id", "c", true, false, false),
		NewFakeField("name", "nice", true, true, false),
		NewFakeField("desc", "more things", true, true, false),
	}
	doc3.FakeComposite("_all", []string{"_id"})

	doc4 := &FakeDocument{
		NewFakeField("_id", "d", true, false, false),
		NewFakeField("name", "great", true, true, false),
		NewFakeField("desc", "final stuff", true, true, false),
	}
	doc4.FakeComposite("_all", []string{"_id"})

	zipData2, _, err := NewZip([]Document{doc3, doc4}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating second ZIP segment: %v", err)
	}

	seg2, err := LoadBytes(zipData2)
	if err != nil {
		t.Fatalf("error loading second ZIP segment: %v", err)
	}

	// Merge the segments
	merger, err := Merge([]Segment{seg1, seg2}, []*roaring.Bitmap{nil, nil}, 1024)
	if err != nil {
		t.Fatalf("error creating merger: %v", err)
	}

	var buf bytes.Buffer
	_, err = merger.WriteTo(&buf, nil)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	// Load the merged segment
	mergedSeg, err := LoadBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("error loading merged segment: %v", err)
	}

	// Verify count
	if mergedSeg.Count() != 4 {
		t.Errorf("expected 4 docs, got %d", mergedSeg.Count())
	}

	// Verify all documents are present
	expectedIDs := []string{"a", "b", "c", "d"}
	for docNum, expectedID := range expectedIDs {
		var foundID string
		err = mergedSeg.VisitStoredFields(uint64(docNum), func(field string, value []byte) bool {
			if field == "_id" {
				foundID = string(value)
			}
			return true
		})
		if err != nil {
			t.Fatalf("error visiting stored fields for doc %d: %v", docNum, err)
		}
		if foundID != expectedID {
			t.Errorf("expected doc %d to have _id '%s', got '%s'", docNum, expectedID, foundID)
		}
	}

	// Verify dictionary
	dict, err := mergedSeg.Dictionary("name")
	if err != nil {
		t.Fatalf("error getting dictionary: %v", err)
	}

	// Each name term should have exactly 1 document
	for _, term := range []string{"wow", "cool", "nice", "great"} {
		postings, err := dict.PostingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatalf("error getting postings for '%s': %v", term, err)
		}
		if postings.Count() != 1 {
			t.Errorf("expected 1 posting for '%s', got %d", term, postings.Count())
		}
	}
}

func TestMergeWithDrops(t *testing.T) {
	// Create first ZIP segment
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "first", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "second", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	zipData1, _, err := NewZip([]Document{doc1, doc2}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating first ZIP segment: %v", err)
	}

	seg1, err := LoadBytes(zipData1)
	if err != nil {
		t.Fatalf("error loading first ZIP segment: %v", err)
	}

	// Create second ZIP segment
	doc3 := &FakeDocument{
		NewFakeField("_id", "c", true, false, false),
		NewFakeField("name", "third", true, true, false),
	}
	doc3.FakeComposite("_all", []string{"_id"})

	doc4 := &FakeDocument{
		NewFakeField("_id", "d", true, false, false),
		NewFakeField("name", "fourth", true, true, false),
	}
	doc4.FakeComposite("_all", []string{"_id"})

	zipData2, _, err := NewZip([]Document{doc3, doc4}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating second ZIP segment: %v", err)
	}

	seg2, err := LoadBytes(zipData2)
	if err != nil {
		t.Fatalf("error loading second ZIP segment: %v", err)
	}

	// Drop doc "b" from segment 1 and doc "c" from segment 2
	drops := []*roaring.Bitmap{roaring.NewBitmap(), roaring.NewBitmap()}
	drops[0].AddInt(1) // drop doc "b" (docNum 1 in seg1)
	drops[1].AddInt(0) // drop doc "c" (docNum 0 in seg2)

	// Merge the segments
	merger, err := Merge([]Segment{seg1, seg2}, drops, 1024)
	if err != nil {
		t.Fatalf("error creating merger: %v", err)
	}

	var buf bytes.Buffer
	_, err = merger.WriteTo(&buf, nil)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	// Load the merged segment
	mergedSeg, err := LoadBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("error loading merged segment: %v", err)
	}

	// Verify count (4 - 2 dropped = 2)
	if mergedSeg.Count() != 2 {
		t.Errorf("expected 2 docs, got %d", mergedSeg.Count())
	}

	// Verify only docs "a" and "d" are present
	expectedIDs := []string{"a", "d"}
	for docNum, expectedID := range expectedIDs {
		var foundID string
		err = mergedSeg.VisitStoredFields(uint64(docNum), func(field string, value []byte) bool {
			if field == "_id" {
				foundID = string(value)
			}
			return true
		})
		if err != nil {
			t.Fatalf("error visiting stored fields for doc %d: %v", docNum, err)
		}
		if foundID != expectedID {
			t.Errorf("expected doc %d to have _id '%s', got '%s'", docNum, expectedID, foundID)
		}
	}

	// Verify dropped terms are not in dictionary
	dict, err := mergedSeg.Dictionary("name")
	if err != nil {
		t.Fatalf("error getting dictionary: %v", err)
	}

	// "second" and "third" should not be found
	for _, term := range []string{"second", "third"} {
		postings, err := dict.PostingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatalf("error getting postings for '%s': %v", term, err)
		}
		if postings.Count() != 0 {
			t.Errorf("expected 0 postings for dropped term '%s', got %d", term, postings.Count())
		}
	}

	// "first" and "fourth" should still be present
	for _, term := range []string{"first", "fourth"} {
		postings, err := dict.PostingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatalf("error getting postings for '%s': %v", term, err)
		}
		if postings.Count() != 1 {
			t.Errorf("expected 1 posting for '%s', got %d", term, postings.Count())
		}
	}
}

func TestMergeWithEmpty(t *testing.T) {
	// Create a non-empty ZIP segment
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "hello", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	zipData1, _, err := NewZip([]Document{doc1}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating first ZIP segment: %v", err)
	}

	seg1, err := LoadBytes(zipData1)
	if err != nil {
		t.Fatalf("error loading first ZIP segment: %v", err)
	}

	// Create an empty ZIP segment
	zipData2, _, err := NewZip([]Document{}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating empty ZIP segment: %v", err)
	}

	seg2, err := LoadBytes(zipData2)
	if err != nil {
		t.Fatalf("error loading empty ZIP segment: %v", err)
	}

	// Merge non-empty with empty
	merger, err := Merge([]Segment{seg1, seg2}, []*roaring.Bitmap{nil, nil}, 1024)
	if err != nil {
		t.Fatalf("error creating merger: %v", err)
	}

	var buf bytes.Buffer
	_, err = merger.WriteTo(&buf, nil)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	mergedSeg, err := LoadBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("error loading merged segment: %v", err)
	}

	if mergedSeg.Count() != 1 {
		t.Errorf("expected 1 doc, got %d", mergedSeg.Count())
	}

	// Merge empty with non-empty (order reversed)
	merger2, err := Merge([]Segment{seg2, seg1}, []*roaring.Bitmap{nil, nil}, 1024)
	if err != nil {
		t.Fatalf("error creating merger: %v", err)
	}

	var buf2 bytes.Buffer
	_, err = merger2.WriteTo(&buf2, nil)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	mergedSeg2, err := LoadBytes(buf2.Bytes())
	if err != nil {
		t.Fatalf("error loading merged segment: %v", err)
	}

	if mergedSeg2.Count() != 1 {
		t.Errorf("expected 1 doc, got %d", mergedSeg2.Count())
	}
}

func TestMergeWithSelf(t *testing.T) {
	// Create a ZIP segment
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "hello", true, true, false),
		NewFakeField("tag", "cold", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "world", true, true, false),
		NewFakeField("tag", "hot", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	zipData, _, err := NewZip([]Document{doc1, doc2}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	// Merge segment with itself multiple times
	currentSeg := seg
	for i := 0; i < 5; i++ {
		merger, err := Merge([]Segment{currentSeg}, []*roaring.Bitmap{nil}, 1024)
		if err != nil {
			t.Fatalf("iteration %d: error creating merger: %v", i, err)
		}

		var buf bytes.Buffer
		_, err = merger.WriteTo(&buf, nil)
		if err != nil {
			t.Fatalf("iteration %d: error merging: %v", i, err)
		}

		newSeg, err := LoadBytes(buf.Bytes())
		if err != nil {
			t.Fatalf("iteration %d: error loading merged segment: %v", i, err)
		}

		if newSeg.Count() != 2 {
			t.Errorf("iteration %d: expected 2 docs, got %d", i, newSeg.Count())
		}

		// Verify stored fields are still correct
		var foundID string
		err = newSeg.VisitStoredFields(0, func(field string, value []byte) bool {
			if field == "_id" {
				foundID = string(value)
			}
			return true
		})
		if err != nil {
			t.Fatalf("iteration %d: error visiting stored fields: %v", i, err)
		}
		if foundID != "a" {
			t.Errorf("iteration %d: expected doc 0 to have _id 'a', got '%s'", i, foundID)
		}

		currentSeg = newSeg
	}
}

func TestMergeWithDocValues(t *testing.T) {
	// Create first ZIP segment with doc values
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("category", "books", false, false, true), // docValues=true
	}
	doc1.FakeComposite("_all", []string{"_id"})

	zipData1, _, err := NewZip([]Document{doc1}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating first ZIP segment: %v", err)
	}

	seg1, err := LoadBytes(zipData1)
	if err != nil {
		t.Fatalf("error loading first ZIP segment: %v", err)
	}

	// Create second ZIP segment with doc values
	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("category", "electronics", false, false, true), // docValues=true
	}
	doc2.FakeComposite("_all", []string{"_id"})

	zipData2, _, err := NewZip([]Document{doc2}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating second ZIP segment: %v", err)
	}

	seg2, err := LoadBytes(zipData2)
	if err != nil {
		t.Fatalf("error loading second ZIP segment: %v", err)
	}

	// Merge the segments
	merger, err := Merge([]Segment{seg1, seg2}, []*roaring.Bitmap{nil, nil}, 1024)
	if err != nil {
		t.Fatalf("error creating merger: %v", err)
	}

	var buf bytes.Buffer
	_, err = merger.WriteTo(&buf, nil)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	// Load the merged segment
	mergedSeg, err := LoadBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("error loading merged segment: %v", err)
	}

	// Get doc value reader
	dvReader, err := mergedSeg.DocumentValueReader([]string{"category"})
	if err != nil {
		t.Fatalf("error getting doc value reader: %v", err)
	}

	// Verify doc values for doc 0
	var doc0Values []string
	err = dvReader.VisitDocumentValues(0, func(field string, term []byte) {
		doc0Values = append(doc0Values, string(term))
	})
	if err != nil {
		t.Fatalf("error visiting doc values for doc 0: %v", err)
	}
	if len(doc0Values) != 1 || doc0Values[0] != "books" {
		t.Errorf("expected doc 0 to have category=books, got %v", doc0Values)
	}

	// Verify doc values for doc 1
	var doc1Values []string
	err = dvReader.VisitDocumentValues(1, func(field string, term []byte) {
		doc1Values = append(doc1Values, string(term))
	})
	if err != nil {
		t.Fatalf("error visiting doc values for doc 1: %v", err)
	}
	if len(doc1Values) != 1 || doc1Values[0] != "electronics" {
		t.Errorf("expected doc 1 to have category=electronics, got %v", doc1Values)
	}
}

func TestMergeDocumentNumbers(t *testing.T) {
	// Create first ZIP segment
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	zipData1, _, err := NewZip([]Document{doc1, doc2}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating first ZIP segment: %v", err)
	}

	seg1, err := LoadBytes(zipData1)
	if err != nil {
		t.Fatalf("error loading first ZIP segment: %v", err)
	}

	// Create second ZIP segment
	doc3 := &FakeDocument{
		NewFakeField("_id", "c", true, false, false),
	}
	doc3.FakeComposite("_all", []string{"_id"})

	zipData2, _, err := NewZip([]Document{doc3}, encodeNorm)
	if err != nil {
		t.Fatalf("error creating second ZIP segment: %v", err)
	}

	seg2, err := LoadBytes(zipData2)
	if err != nil {
		t.Fatalf("error loading second ZIP segment: %v", err)
	}

	// Merge with drops
	drops := []*roaring.Bitmap{roaring.NewBitmap(), roaring.NewBitmap()}
	drops[0].AddInt(1) // drop doc "b" (docNum 1 in seg1)

	merger, err := Merge([]Segment{seg1, seg2}, drops, 1024)
	if err != nil {
		t.Fatalf("error creating merger: %v", err)
	}

	var buf bytes.Buffer
	_, err = merger.WriteTo(&buf, nil)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	// Check document number mapping
	docNums := merger.DocumentNumbers()
	if len(docNums) != 2 {
		t.Fatalf("expected 2 document number arrays, got %d", len(docNums))
	}

	// Segment 1: doc 0 -> 0, doc 1 -> dropped (docDropped sentinel)
	if docNums[0][0] != 0 {
		t.Errorf("expected seg1 doc 0 to map to 0, got %d", docNums[0][0])
	}
	if docNums[0][1] != docDropped {
		t.Errorf("expected seg1 doc 1 to be dropped, got %d", docNums[0][1])
	}

	// Segment 2: doc 0 -> 1
	if docNums[1][0] != 1 {
		t.Errorf("expected seg2 doc 0 to map to 1, got %d", docNums[1][0])
	}
}
