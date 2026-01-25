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
	"reflect"
	"sort"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

// segmentFormat defines how to build and load segments for a specific format
type segmentFormat struct {
	name  string
	build func(docs []Document) (Segment, []byte, error)
	load  func(data []byte) (Segment, error)
}

var zipFormat = segmentFormat{
	name: "zip",
	build: func(docs []Document) (Segment, []byte, error) {
		data, _, err := NewZip(docs, encodeNorm)
		if err != nil {
			return nil, nil, err
		}
		seg, err := LoadBytes(data)
		if err != nil {
			return nil, nil, err
		}
		return seg, data, nil
	},
	load: func(data []byte) (Segment, error) {
		return LoadBytes(data)
	},
}

// allFormats includes only ZIP format since flat format is deprecated
var allFormats = []segmentFormat{zipFormat}

// TestFunctionalBuildAndSearch tests the basic build -> search workflow for both formats
func TestFunctionalBuildAndSearch(t *testing.T) {
	docs := createTestDocuments()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			// Verify document count
			if seg.Count() != 3 {
				t.Errorf("expected 3 docs, got %d", seg.Count())
			}

			// Test searching for unique terms
			testSearchTerm(t, seg, "title", "introduction", 1)
			testSearchTerm(t, seg, "title", "advanced", 1)
			testSearchTerm(t, seg, "title", "reference", 1)

			// Test searching for shared terms
			testSearchTerm(t, seg, "category", "programming", 2)
			testSearchTerm(t, seg, "author", "alice", 1)

			// Test searching for non-existent terms
			testSearchTerm(t, seg, "title", "nonexistent", 0)
			testSearchTerm(t, seg, "nonexistent_field", "value", 0)
		})
	}
}

// TestFunctionalStoredFields tests stored field retrieval for both formats
func TestFunctionalStoredFields(t *testing.T) {
	docs := createTestDocuments()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			// Verify stored fields for each document
			expectedIDs := []string{"doc1", "doc2", "doc3"}
			for i, expectedID := range expectedIDs {
				var foundID string
				err := seg.VisitStoredFields(uint64(i), func(field string, value []byte) bool {
					if field == "_id" {
						foundID = string(value)
					}
					return true
				})
				if err != nil {
					t.Errorf("error visiting stored fields for doc %d: %v", i, err)
				}
				if foundID != expectedID {
					t.Errorf("doc %d: expected _id=%s, got %s", i, expectedID, foundID)
				}
			}
		})
	}
}

// TestFunctionalCollectionStats tests collection statistics for both formats
func TestFunctionalCollectionStats(t *testing.T) {
	docs := createTestDocuments()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			// Test stats for a field present in all docs
			stats, err := seg.CollectionStats("category")
			if err != nil {
				t.Fatalf("error getting collection stats: %v", err)
			}
			if stats.TotalDocumentCount() != 3 {
				t.Errorf("expected TotalDocumentCount=3, got %d", stats.TotalDocumentCount())
			}
			if stats.DocumentCount() != 3 {
				t.Errorf("expected DocumentCount=3, got %d", stats.DocumentCount())
			}

			// Test stats for non-existent field
			emptyStats, err := seg.CollectionStats("nonexistent")
			if err != nil {
				t.Fatalf("error getting stats for nonexistent field: %v", err)
			}
			if emptyStats.TotalDocumentCount() != 0 {
				t.Errorf("expected TotalDocumentCount=0 for nonexistent field")
			}
		})
	}
}

// TestFunctionalDictionaryIteration tests dictionary iteration for both formats
func TestFunctionalDictionaryIteration(t *testing.T) {
	docs := createTestDocuments()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			dict, err := seg.Dictionary("title")
			if err != nil {
				t.Fatalf("error getting dictionary: %v", err)
			}

			// Iterate through all terms
			var terms []string
			itr := dict.Iterator(nil, nil, nil)
			for {
				entry, err := itr.Next()
				if err != nil {
					t.Fatalf("error iterating: %v", err)
				}
				if entry == nil {
					break
				}
				terms = append(terms, entry.Term())
			}
			itr.Close()

			// Should have 3 unique terms in title field
			sort.Strings(terms)
			expectedTerms := []string{"advanced", "introduction", "reference"}
			if !reflect.DeepEqual(terms, expectedTerms) {
				t.Errorf("expected terms %v, got %v", expectedTerms, terms)
			}
		})
	}
}

// TestFunctionalDocsMatchingTerms tests DocsMatchingTerms for both formats
func TestFunctionalDocsMatchingTerms(t *testing.T) {
	docs := createTestDocuments()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			// Search for docs with category=programming
			terms := []Term{
				&testTerm{field: "category", term: []byte("programming")},
			}
			bitmap, err := seg.DocsMatchingTerms(terms)
			if err != nil {
				t.Fatalf("error matching terms: %v", err)
			}
			if bitmap.GetCardinality() != 2 {
				t.Errorf("expected 2 docs matching category=programming, got %d", bitmap.GetCardinality())
			}

			// Search for docs with multiple terms (OR)
			terms = []Term{
				&testTerm{field: "author", term: []byte("alice")},
				&testTerm{field: "author", term: []byte("bob")},
			}
			bitmap, err = seg.DocsMatchingTerms(terms)
			if err != nil {
				t.Fatalf("error matching terms: %v", err)
			}
			if bitmap.GetCardinality() != 2 {
				t.Errorf("expected 2 docs matching alice OR bob, got %d", bitmap.GetCardinality())
			}
		})
	}
}

// TestFunctionalDocValues tests doc values for both formats
func TestFunctionalDocValues(t *testing.T) {
	docs := createDocValuesTestDocuments()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			dvReader, err := seg.DocumentValueReader([]string{"sortkey"})
			if err != nil {
				t.Fatalf("error getting doc value reader: %v", err)
			}

			// Verify doc values for each document
			expectedValues := []string{"aaa", "bbb", "ccc"}
			for i, expected := range expectedValues {
				var foundValue string
				err := dvReader.VisitDocumentValues(uint64(i), func(field string, term []byte) {
					if field == "sortkey" {
						foundValue = string(term)
					}
				})
				if err != nil {
					t.Errorf("error visiting doc values for doc %d: %v", i, err)
				}
				if foundValue != expected {
					t.Errorf("doc %d: expected sortkey=%s, got %s", i, expected, foundValue)
				}
			}
		})
	}
}

// TestFunctionalSegmentRoundtrip verifies segment can be built and read back correctly
func TestFunctionalSegmentRoundtrip(t *testing.T) {
	docs := createTestDocuments()

	// Build ZIP segment
	seg, data, err := zipFormat.build(docs)
	if err != nil {
		t.Fatalf("error building zip segment: %v", err)
	}

	// Load it back
	loadedSeg, err := zipFormat.load(data)
	if err != nil {
		t.Fatalf("error loading zip segment: %v", err)
	}

	// Verify document counts match
	if seg.Count() != loadedSeg.Count() {
		t.Errorf("count mismatch: original=%d, loaded=%d", seg.Count(), loadedSeg.Count())
	}

	// Verify fields match
	origFields := seg.Fields()
	loadedFields := loadedSeg.Fields()
	sort.Strings(origFields)
	sort.Strings(loadedFields)
	if !reflect.DeepEqual(origFields, loadedFields) {
		t.Errorf("fields mismatch: original=%v, loaded=%v", origFields, loadedFields)
	}

	// Verify search results match for each field
	for _, field := range origFields {
		origDict, _ := seg.Dictionary(field)
		loadedDict, _ := loadedSeg.Dictionary(field)

		// Get all terms from both
		origTerms := getAllTerms(t, origDict)
		loadedTerms := getAllTerms(t, loadedDict)
		sort.Strings(origTerms)
		sort.Strings(loadedTerms)

		if !reflect.DeepEqual(origTerms, loadedTerms) {
			t.Errorf("field %s: terms mismatch: original=%v, loaded=%v", field, origTerms, loadedTerms)
		}

		// Verify postings match for each term
		for _, term := range origTerms {
			origPL, _ := origDict.PostingsList([]byte(term), nil, nil)
			loadedPL, _ := loadedDict.PostingsList([]byte(term), nil, nil)

			if origPL.Count() != loadedPL.Count() {
				t.Errorf("field %s, term %s: postings count mismatch: original=%d, loaded=%d",
					field, term, origPL.Count(), loadedPL.Count())
			}
		}
	}
}

// TestFunctionalEmptySegment tests edge case of empty segments
func TestFunctionalEmptySegment(t *testing.T) {
	docs := []Document{}

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building empty segment: %v", err)
			}

			if seg.Count() != 0 {
				t.Errorf("expected 0 docs, got %d", seg.Count())
			}
		})
	}
}

// TestFunctionalSingleDocSegment tests edge case of single document segment
func TestFunctionalSingleDocSegment(t *testing.T) {
	doc := &FakeDocument{
		NewFakeField("_id", "only", true, false, false),
		NewFakeField("title", "solo", true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})
	docs := []Document{doc}

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building single doc segment: %v", err)
			}

			if seg.Count() != 1 {
				t.Errorf("expected 1 doc, got %d", seg.Count())
			}

			// Search for the term
			testSearchTerm(t, seg, "title", "solo", 1)

			// Verify stored field
			var foundID string
			seg.VisitStoredFields(0, func(field string, value []byte) bool {
				if field == "_id" {
					foundID = string(value)
				}
				return true
			})
			if foundID != "only" {
				t.Errorf("expected _id=only, got %s", foundID)
			}
		})
	}
}

// TestFunctionalMergeZip tests the merge workflow for ZIP format
func TestFunctionalMergeZip(t *testing.T) {
	t.Run("basic-merge", func(t *testing.T) {
		// Build first segment
		docs1 := []Document{
			createDoc("doc1", "title", "first", "category", "a"),
			createDoc("doc2", "title", "second", "category", "a"),
		}
		seg1, _, err := zipFormat.build(docs1)
		if err != nil {
			t.Fatalf("error building segment 1: %v", err)
		}

		// Build second segment
		docs2 := []Document{
			createDoc("doc3", "title", "third", "category", "b"),
			createDoc("doc4", "title", "fourth", "category", "b"),
		}
		seg2, _, err := zipFormat.build(docs2)
		if err != nil {
			t.Fatalf("error building segment 2: %v", err)
		}

		// Merge segments using ZIP merger
		merger, err := Merge([]Segment{seg1, seg2}, []*roaring.Bitmap{nil, nil}, 1024*1024)
		if err != nil {
			t.Fatalf("error creating merger: %v", err)
		}
		var mergedBuf bytes.Buffer
		_, err = merger.WriteTo(&mergedBuf, nil)
		if err != nil {
			t.Fatalf("error merging segments: %v", err)
		}

		// Load merged segment
		mergedSeg, err := zipFormat.load(mergedBuf.Bytes())
		if err != nil {
			t.Fatalf("error loading merged segment: %v", err)
		}

		// Verify merged segment has all documents
		if mergedSeg.Count() != 4 {
			t.Errorf("expected 4 docs in merged segment, got %d", mergedSeg.Count())
		}

		// Verify terms from both source segments are findable
		testSearchTerm(t, mergedSeg, "title", "first", 1)
		testSearchTerm(t, mergedSeg, "title", "second", 1)
		testSearchTerm(t, mergedSeg, "title", "third", 1)
		testSearchTerm(t, mergedSeg, "title", "fourth", 1)

		// Verify category terms
		testSearchTerm(t, mergedSeg, "category", "a", 2)
		testSearchTerm(t, mergedSeg, "category", "b", 2)

		// Verify stored fields are retrievable
		expectedIDs := []string{"doc1", "doc2", "doc3", "doc4"}
		for i := uint64(0); i < mergedSeg.Count(); i++ {
			var foundID string
			mergedSeg.VisitStoredFields(i, func(field string, value []byte) bool {
				if field == "_id" {
					foundID = string(value)
				}
				return true
			})
			// Note: order may not match due to merge, so just verify ID is one of expected
			found := false
			for _, id := range expectedIDs {
				if foundID == id {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("doc %d: unexpected _id=%s", i, foundID)
			}
		}
	})

	t.Run("merge-with-drops", func(t *testing.T) {
		// Build first segment
		docs1 := []Document{
			createDoc("docA", "name", "keep1", "type", "x"),
			createDoc("docB", "name", "drop", "type", "x"),
		}
		seg1, _, err := zipFormat.build(docs1)
		if err != nil {
			t.Fatalf("error building segment 1: %v", err)
		}

		// Build second segment
		docs2 := []Document{
			createDoc("docC", "name", "keep2", "type", "y"),
		}
		seg2, _, err := zipFormat.build(docs2)
		if err != nil {
			t.Fatalf("error building segment 2: %v", err)
		}

		// Drop docB (index 1 in segment 1)
		drop1 := roaring.New()
		drop1.Add(1)

		// Merge segments with drop using ZIP merger
		merger, err := Merge([]Segment{seg1, seg2}, []*roaring.Bitmap{drop1, nil}, 1024*1024)
		if err != nil {
			t.Fatalf("error creating merger: %v", err)
		}
		var mergedBuf bytes.Buffer
		_, err = merger.WriteTo(&mergedBuf, nil)
		if err != nil {
			t.Fatalf("error merging segments: %v", err)
		}

		// Load merged segment
		mergedSeg, err := zipFormat.load(mergedBuf.Bytes())
		if err != nil {
			t.Fatalf("error loading merged segment: %v", err)
		}

		// Verify merged segment has 2 documents (one dropped)
		if mergedSeg.Count() != 2 {
			t.Errorf("expected 2 docs in merged segment, got %d", mergedSeg.Count())
		}

		// Verify dropped term is no longer searchable
		testSearchTerm(t, mergedSeg, "name", "drop", 0)

		// Verify kept terms are still searchable
		testSearchTerm(t, mergedSeg, "name", "keep1", 1)
		testSearchTerm(t, mergedSeg, "name", "keep2", 1)
	})
}

// Helper functions

func createTestDocuments() []Document {
	doc1 := &FakeDocument{
		NewFakeField("_id", "doc1", true, false, false),
		NewFakeField("title", "introduction", true, true, false),
		NewFakeField("category", "programming", true, true, false),
		NewFakeField("author", "alice", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "doc2", true, false, false),
		NewFakeField("title", "advanced", true, true, false),
		NewFakeField("category", "programming", true, true, false),
		NewFakeField("author", "bob", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	doc3 := &FakeDocument{
		NewFakeField("_id", "doc3", true, false, false),
		NewFakeField("title", "reference", true, true, false),
		NewFakeField("category", "documentation", true, true, false),
		NewFakeField("author", "charlie", true, true, false),
	}
	doc3.FakeComposite("_all", []string{"_id"})

	return []Document{doc1, doc2, doc3}
}

func createDocValuesTestDocuments() []Document {
	doc1 := &FakeDocument{
		NewFakeField("_id", "doc1", true, false, false),
		NewFakeField("sortkey", "aaa", false, false, true),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "doc2", true, false, false),
		NewFakeField("sortkey", "bbb", false, false, true),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	doc3 := &FakeDocument{
		NewFakeField("_id", "doc3", true, false, false),
		NewFakeField("sortkey", "ccc", false, false, true),
	}
	doc3.FakeComposite("_all", []string{"_id"})

	return []Document{doc1, doc2, doc3}
}

// createDocValuesTestDocumentsWithIDDocValues creates documents where _id also has doc values enabled
// This reproduces the bluge integration test case where _id is Sortable
func createDocValuesTestDocumentsWithIDDocValues() []Document {
	doc1 := &FakeDocument{
		NewFakeField("_id", "doc1", true, false, true), // _id with doc values
		NewFakeField("sortkey", "aaa", false, false, true),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	return []Document{doc1}
}

// TestDocValuesWithID tests that doc values work for the _id field
// This is the case in bluge where NewDocument creates _id with Sortable
func TestDocValuesWithID(t *testing.T) {
	docs := createDocValuesTestDocumentsWithIDDocValues()

	for _, format := range allFormats {
		t.Run(format.name, func(t *testing.T) {
			seg, _, err := format.build(docs)
			if err != nil {
				t.Fatalf("error building segment: %v", err)
			}

			// Read doc values for _id field
			dvReader, err := seg.DocumentValueReader([]string{"_id"})
			if err != nil {
				t.Fatalf("error getting doc value reader for _id: %v", err)
			}

			var foundValue string
			err = dvReader.VisitDocumentValues(0, func(field string, term []byte) {
				if field == "_id" {
					foundValue = string(term)
				}
			})
			if err != nil {
				t.Errorf("error visiting doc values for _id: %v", err)
			}
			if foundValue != "doc1" {
				t.Errorf("expected _id doc value=doc1, got %s", foundValue)
			}
		})
	}
}

// TestDocValuesWithIDViaNew tests using segment.New (the bluge code path)
// This uses newSegmentFromConstruction instead of LoadBytes
func TestDocValuesWithIDViaNew(t *testing.T) {
	docs := createDocValuesTestDocumentsWithIDDocValues()

	// Use New which calls newSegmentFromConstruction
	seg, _, err := New(docs, encodeNorm)
	if err != nil {
		t.Fatalf("error creating segment via New: %v", err)
	}

	// Read doc values for _id field
	dvReader, err := seg.DocumentValueReader([]string{"_id"})
	if err != nil {
		t.Fatalf("error getting doc value reader for _id: %v", err)
	}

	var foundValue string
	err = dvReader.VisitDocumentValues(0, func(field string, term []byte) {
		if field == "_id" {
			foundValue = string(term)
		}
	})
	if err != nil {
		t.Errorf("error visiting doc values for _id: %v", err)
	}
	if foundValue != "doc1" {
		t.Errorf("expected _id doc value=doc1, got %s", foundValue)
	}
}

func createDoc(id, field1Name, field1Value, field2Name, field2Value string) Document {
	doc := &FakeDocument{
		NewFakeField("_id", id, true, false, false),
		NewFakeField(field1Name, field1Value, true, true, false),
		NewFakeField(field2Name, field2Value, true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})
	return doc
}

func testSearchTerm(t *testing.T, seg Segment, field, term string, expectedCount uint64) {
	t.Helper()
	dict, err := seg.Dictionary(field)
	if err != nil {
		t.Fatalf("error getting dictionary for %s: %v", field, err)
	}

	pl, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil {
		t.Fatalf("error getting postings for %s=%s: %v", field, term, err)
	}

	if pl.Count() != expectedCount {
		t.Errorf("expected %d docs for %s=%s, got %d", expectedCount, field, term, pl.Count())
	}
}

func getAllTerms(t *testing.T, dict Dictionary) []string {
	t.Helper()
	var terms []string
	itr := dict.Iterator(nil, nil, nil)
	for {
		entry, err := itr.Next()
		if err != nil {
			t.Fatalf("error iterating dictionary: %v", err)
		}
		if entry == nil {
			break
		}
		terms = append(terms, entry.Term())
	}
	itr.Close()
	return terms
}

// testTerm implements Term for testing
type testTerm struct {
	field string
	term  []byte
}

func (t *testTerm) Field() string {
	return t.field
}

func (t *testTerm) Term() []byte {
	return t.term
}

// TestZipEntries tests the ZipEntries method
func TestZipEntries(t *testing.T) {
	docs := createTestDocuments()

	seg, _, err := zipFormat.build(docs)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}

	// Cast to *segment to access ZipEntries
	zipSeg, ok := seg.(*segment)
	if !ok {
		t.Fatal("expected *segment type")
	}

	entries, err := zipSeg.ZipEntries()
	if err != nil {
		t.Fatalf("error getting ZIP entries: %v", err)
	}

	if len(entries) == 0 {
		t.Fatal("expected at least one ZIP entry")
	}

	// Verify we have expected entries
	expectedEntries := map[string]bool{
		"_footer":       false,
		"_stored/data":  false,
		"_stored/index": false,
	}

	for _, entry := range entries {
		if _, ok := expectedEntries[entry.Name]; ok {
			expectedEntries[entry.Name] = true
		}
	}

	for name, found := range expectedEntries {
		if !found {
			t.Errorf("expected to find entry %q", name)
		}
	}

	// Verify entry sizes are populated
	for _, entry := range entries {
		if entry.UncompressedSize == 0 {
			t.Errorf("entry %q has zero uncompressed size", entry.Name)
		}
	}
}
