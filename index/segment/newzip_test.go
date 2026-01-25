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
	"archive/zip"
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestNewZip(t *testing.T) {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
		NewFakeField("tag", "cold", true, true, false),
		NewFakeField("tag", "dark", true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})

	results := []Document{doc}

	zipData, size, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	if size == 0 {
		t.Fatal("expected non-zero size")
	}

	if len(zipData) == 0 {
		t.Fatal("expected non-empty ZIP data")
	}

	// Parse the ZIP to verify structure
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatalf("error reading ZIP: %v", err)
	}

	// Check expected entries
	expectedEntries := map[string]bool{
		"_stored/data":  false,
		"_stored/index": false,
		"_footer":       false,
	}

	// Add expected field entries
	expectedFields := []string{"_id", "_all", "name", "desc", "tag"}
	for _, field := range expectedFields {
		expectedEntries[field+"/meta"] = false
		expectedEntries[field+"/dictionary"] = false
		expectedEntries[field+"/postings"] = false
	}

	for _, file := range zipReader.File {
		t.Logf("ZIP entry: %s (size: %d)", file.Name, file.UncompressedSize64)
		if _, ok := expectedEntries[file.Name]; ok {
			expectedEntries[file.Name] = true
		}
	}

	// Verify required entries exist
	for entry, found := range expectedEntries {
		if !found && !strings.HasSuffix(entry, "/docvalues") {
			// docvalues are optional, others are required
			if entry == "_stored/data" || entry == "_stored/index" || entry == "_footer" ||
				strings.HasSuffix(entry, "/meta") || strings.HasSuffix(entry, "/dictionary") ||
				strings.HasSuffix(entry, "/postings") {
				t.Errorf("expected entry %s not found in ZIP", entry)
			}
		}
	}

	// Parse and verify footer
	var footerFile *zip.File
	for _, file := range zipReader.File {
		if file.Name == "_footer" {
			footerFile = file
			break
		}
	}

	if footerFile == nil {
		t.Fatal("_footer entry not found")
	}

	footerReader, err := footerFile.Open()
	if err != nil {
		t.Fatalf("error opening footer: %v", err)
	}
	defer footerReader.Close()

	footerData := make([]byte, footerFile.UncompressedSize64)
	_, err = footerReader.Read(footerData)
	if err != nil {
		t.Fatalf("error reading footer: %v", err)
	}

	// Parse footer: numDocs (8) + version (4) + chunkMode (4) + manifest
	if len(footerData) < 16 {
		t.Fatalf("footer too short: %d bytes", len(footerData))
	}

	numDocs := binary.BigEndian.Uint64(footerData[0:8])
	version := binary.BigEndian.Uint32(footerData[8:12])
	chunkMode := binary.BigEndian.Uint32(footerData[12:16])

	if numDocs != 1 {
		t.Errorf("expected 1 doc, got %d", numDocs)
	}

	if version != Version {
		t.Errorf("expected version %d, got %d", Version, version)
	}

	if chunkMode != defaultChunkMode {
		t.Errorf("expected chunkMode %d, got %d", defaultChunkMode, chunkMode)
	}

	t.Logf("Footer: numDocs=%d, version=%d, chunkMode=%d", numDocs, version, chunkMode)
}

func TestNewZipMultiDoc(t *testing.T) {
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "cool", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	results := []Document{doc1, doc2}

	zipData, size, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	if size == 0 {
		t.Fatal("expected non-zero size")
	}

	// Parse the ZIP to verify structure
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatalf("error reading ZIP: %v", err)
	}

	// Verify stored index has correct size (2 docs * 8 bytes per offset)
	for _, file := range zipReader.File {
		if file.Name == "_stored/index" {
			if file.UncompressedSize64 != 16 {
				t.Errorf("expected stored index size 16, got %d", file.UncompressedSize64)
			}
		}
		t.Logf("ZIP entry: %s (size: %d)", file.Name, file.UncompressedSize64)
	}
}

func TestNewZipEmpty(t *testing.T) {
	results := []Document{}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating empty ZIP segment: %v", err)
	}

	// Parse the ZIP to verify it's valid
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatalf("error reading ZIP: %v", err)
	}

	// Should have at least a footer
	hasFooter := false
	for _, file := range zipReader.File {
		t.Logf("ZIP entry: %s (size: %d)", file.Name, file.UncompressedSize64)
		if file.Name == "_footer" {
			hasFooter = true
		}
	}

	if !hasFooter {
		t.Error("expected _footer entry in empty segment")
	}
}

func TestNewZipWithDocValues(t *testing.T) {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", false, false, true), // docValues=true
	}
	doc.FakeComposite("_all", []string{"_id"})

	results := []Document{doc}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	// Parse the ZIP to verify structure
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		t.Fatalf("error reading ZIP: %v", err)
	}

	// Should have docvalues entry for "name" field
	hasDocValues := false
	for _, file := range zipReader.File {
		t.Logf("ZIP entry: %s (size: %d)", file.Name, file.UncompressedSize64)
		if file.Name == "name/docvalues" {
			hasDocValues = true
		}
	}

	if !hasDocValues {
		t.Error("expected name/docvalues entry")
	}
}

func TestZipDocValuesRoundtrip(t *testing.T) {
	// Create a document with doc values
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("color", "red", false, false, true),  // docValues=true
		NewFakeField("color", "blue", false, false, true), // multiple values
		NewFakeField("size", "large", false, false, true), // another field with doc values
	}
	doc.FakeComposite("_all", []string{"_id"})

	results := []Document{doc}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	// Load the ZIP segment
	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	// Get a DocumentValueReader
	dvReader, err := seg.DocumentValueReader([]string{"color", "size"})
	if err != nil {
		t.Fatalf("error getting doc value reader: %v", err)
	}

	// Visit doc values for document 0
	var visitedValues []string
	err = dvReader.VisitDocumentValues(0, func(field string, term []byte) {
		visitedValues = append(visitedValues, field+":"+string(term))
		t.Logf("doc value: field=%s, term=%s", field, string(term))
	})
	if err != nil {
		t.Fatalf("error visiting doc values: %v", err)
	}

	// We expect "color:red", "color:blue", "size:large"
	if len(visitedValues) != 3 {
		t.Errorf("expected 3 doc values, got %d: %v", len(visitedValues), visitedValues)
	}

	// Verify specific values are present
	expectedValues := map[string]bool{
		"color:red":  false,
		"color:blue": false,
		"size:large": false,
	}
	for _, v := range visitedValues {
		if _, ok := expectedValues[v]; ok {
			expectedValues[v] = true
		}
	}
	for v, found := range expectedValues {
		if !found {
			t.Errorf("expected doc value %s not found", v)
		}
	}
}

func TestZipDocValuesMultiDoc(t *testing.T) {
	// Create multiple documents with doc values
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("category", "books", false, false, true),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("category", "electronics", false, false, true),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	results := []Document{doc1, doc2}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	dvReader, err := seg.DocumentValueReader([]string{"category"})
	if err != nil {
		t.Fatalf("error getting doc value reader: %v", err)
	}

	// Visit doc values for document 0
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

	// Visit doc values for document 1
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

func TestZipCollectionStats(t *testing.T) {
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "hello world", true, true, false), // 2 terms
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "hello there", true, true, false), // 2 terms
	}
	doc2.FakeComposite("_all", []string{"_id"})

	results := []Document{doc1, doc2}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	// Get collection stats for "name" field
	stats, err := seg.CollectionStats("name")
	if err != nil {
		t.Fatalf("error getting collection stats: %v", err)
	}

	// Total doc count should be 2
	if stats.TotalDocumentCount() != 2 {
		t.Errorf("expected TotalDocumentCount=2, got %d", stats.TotalDocumentCount())
	}

	// Doc count (docs with this field) should be 2
	if stats.DocumentCount() != 2 {
		t.Errorf("expected DocumentCount=2, got %d", stats.DocumentCount())
	}

	// Sum total term freq should be 4 (2 terms * 2 docs)
	if stats.SumTotalTermFrequency() != 4 {
		t.Errorf("expected SumTotalTermFrequency=4, got %d", stats.SumTotalTermFrequency())
	}

	// Non-existent field should return zeros
	emptyStats, err := seg.CollectionStats("nonexistent")
	if err != nil {
		t.Fatalf("error getting stats for nonexistent field: %v", err)
	}
	if emptyStats.TotalDocumentCount() != 0 {
		t.Errorf("expected TotalDocumentCount=0 for nonexistent field, got %d", emptyStats.TotalDocumentCount())
	}
}

func TestLoadBytes(t *testing.T) {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
		NewFakeField("tag", "cold", true, true, false),
		NewFakeField("tag", "dark", true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})

	results := []Document{doc}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	// Load the ZIP segment
	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	// Verify basic properties
	if seg.Count() != 1 {
		t.Errorf("expected 1 doc, got %d", seg.Count())
	}

	if seg.Version() != Version {
		t.Errorf("expected version %d, got %d", Version, seg.Version())
	}

	// Verify fields
	fields := seg.Fields()
	expectedFields := map[string]bool{
		"_id":  false,
		"_all": false,
		"name": false,
		"desc": false,
		"tag":  false,
	}
	for _, field := range fields {
		if _, ok := expectedFields[field]; ok {
			expectedFields[field] = true
		}
	}
	for field, found := range expectedFields {
		if !found {
			t.Errorf("expected field %s not found", field)
		}
	}

	// Verify dictionary lookup
	dict, err := seg.Dictionary("name")
	if err != nil {
		t.Fatalf("error getting dictionary: %v", err)
	}
	if dict == nil {
		t.Fatal("dictionary is nil")
	}

	// Verify term lookup
	postings, err := dict.PostingsList([]byte("wow"), nil, nil)
	if err != nil {
		t.Fatalf("error getting postings: %v", err)
	}
	if postings == nil {
		t.Fatal("postings is nil")
	}
	if postings.Count() != 1 {
		t.Errorf("expected postings count 1, got %d", postings.Count())
	}

	// Verify stored fields
	var storedFieldCount int
	err = seg.VisitStoredFields(0, func(field string, value []byte) bool {
		storedFieldCount++
		t.Logf("stored field: %s = %s", field, string(value))
		return true
	})
	if err != nil {
		t.Fatalf("error visiting stored fields: %v", err)
	}
	if storedFieldCount != 5 {
		t.Errorf("expected 5 stored fields, got %d", storedFieldCount)
	}
}

func TestLoadMultiDoc(t *testing.T) {
	doc1 := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
	}
	doc1.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "cool", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	results := []Document{doc1, doc2}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	// Load the ZIP segment
	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	if seg.Count() != 2 {
		t.Errorf("expected 2 docs, got %d", seg.Count())
	}

	// Verify stored fields for doc 0
	var doc0ID string
	err = seg.VisitStoredFields(0, func(field string, value []byte) bool {
		if field == "_id" {
			doc0ID = string(value)
		}
		return true
	})
	if err != nil {
		t.Fatalf("error visiting stored fields for doc 0: %v", err)
	}
	if doc0ID != "a" {
		t.Errorf("expected doc 0 _id to be 'a', got '%s'", doc0ID)
	}

	// Verify stored fields for doc 1
	var doc1ID string
	err = seg.VisitStoredFields(1, func(field string, value []byte) bool {
		if field == "_id" {
			doc1ID = string(value)
		}
		return true
	})
	if err != nil {
		t.Fatalf("error visiting stored fields for doc 1: %v", err)
	}
	if doc1ID != "b" {
		t.Errorf("expected doc 1 _id to be 'b', got '%s'", doc1ID)
	}

	// Verify dictionary for terms in both docs
	dict, err := seg.Dictionary("name")
	if err != nil {
		t.Fatalf("error getting dictionary: %v", err)
	}

	// "wow" should be in doc 0 only
	postings, err := dict.PostingsList([]byte("wow"), nil, nil)
	if err != nil {
		t.Fatalf("error getting postings for 'wow': %v", err)
	}
	if postings.Count() != 1 {
		t.Errorf("expected 1 posting for 'wow', got %d", postings.Count())
	}

	// "cool" should be in doc 1 only
	postings, err = dict.PostingsList([]byte("cool"), nil, nil)
	if err != nil {
		t.Fatalf("error getting postings for 'cool': %v", err)
	}
	if postings.Count() != 1 {
		t.Errorf("expected 1 posting for 'cool', got %d", postings.Count())
	}
}

func TestLoadDictionaryIterator(t *testing.T) {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "alpha beta gamma", true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})

	results := []Document{doc}

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		t.Fatalf("error creating ZIP segment: %v", err)
	}

	seg, err := LoadBytes(zipData)
	if err != nil {
		t.Fatalf("error loading ZIP segment: %v", err)
	}

	dict, err := seg.Dictionary("name")
	if err != nil {
		t.Fatalf("error getting dictionary: %v", err)
	}

	// Iterate through all terms
	itr := dict.Iterator(nil, nil, nil)
	var terms []string
	for {
		entry, err := itr.Next()
		if err != nil {
			t.Fatalf("error iterating: %v", err)
		}
		if entry == nil {
			break
		}
		terms = append(terms, entry.Term())
		t.Logf("term: %s, count: %d", entry.Term(), entry.Count())
	}
	itr.Close()

	if len(terms) != 3 {
		t.Errorf("expected 3 terms, got %d: %v", len(terms), terms)
	}
}

func TestZipWriterOffsetTracking(t *testing.T) {
	// Use the internal function to get access to the writer
	zw := newZipSegmentWriter()
	zw.SetFieldNames([]string{"_id", "_all", "desc", "name"})

	// Write stored fields
	storedData := zw.StoredData()
	storedData.Write([]byte("test stored data"))
	storedIndex := zw.StoredIndex()
	storedIndex.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})

	// Write field data for _id
	meta := zw.FieldMeta("_id")
	meta.Write([]byte{1, 1})
	dict := zw.FieldDictionary("_id")
	dict.Write([]byte("dict data for id"))
	post := zw.FieldPostings("_id")
	post.Write([]byte("postings for id"))
	zw.FinalizeField()

	// Write field data for name (with doc values)
	meta = zw.FieldMeta("name")
	meta.Write([]byte{1, 1})
	dict = zw.FieldDictionary("name")
	dict.Write([]byte("dict data for name"))
	post = zw.FieldPostings("name")
	post.Write([]byte("postings for name"))
	dv := zw.FieldDocValues("name")
	dv.Write([]byte("docvalues for name"))
	zw.FinalizeField()

	// Write the ZIP
	zipData, err := zw.WriteZip(1, Version, 1024)
	if err != nil {
		t.Fatalf("error writing ZIP: %v", err)
	}

	// Get tracked offsets
	offsets := zw.GetOffsets()

	// Verify stored offsets are non-zero
	if offsets.StoredDataOffset == 0 {
		t.Error("expected non-zero StoredDataOffset")
	}
	if offsets.StoredDataSize != 16 { // "test stored data"
		t.Errorf("expected StoredDataSize=16, got %d", offsets.StoredDataSize)
	}
	if offsets.StoredIndexOffset == 0 {
		t.Error("expected non-zero StoredIndexOffset")
	}
	if offsets.StoredIndexSize != 8 {
		t.Errorf("expected StoredIndexSize=8, got %d", offsets.StoredIndexSize)
	}

	// Verify we can read data at the tracked offsets
	// Check stored data
	storedBytes := zipData[offsets.StoredDataOffset : offsets.StoredDataOffset+offsets.StoredDataSize]
	if string(storedBytes) != "test stored data" {
		t.Errorf("stored data mismatch at offset %d: got %q", offsets.StoredDataOffset, string(storedBytes))
	}

	// Check _id field exists (fieldID 0)
	if len(offsets.FieldOffsets) < 1 {
		t.Fatal("expected at least 1 field offset")
	}
	idOffsets := offsets.FieldOffsets[0] // _id is first field
	if idOffsets.MetaSize != 2 {
		t.Errorf("expected _id MetaSize=2, got %d", idOffsets.MetaSize)
	}
	if idOffsets.DictSize == 0 {
		t.Error("expected non-zero _id DictSize")
	}
	if idOffsets.PostSize == 0 {
		t.Error("expected non-zero _id PostSize")
	}

	// Check name field has doc values (fieldID 3 - since fields are sorted: _all, _id, desc, name)
	// Actually, the order depends on how SetFieldNames was called
	// We set: _id, _all, desc, name - so name is index 3
	if len(offsets.FieldOffsets) < 4 {
		t.Fatalf("expected 4 field offsets, got %d", len(offsets.FieldOffsets))
	}
	nameOffsets := offsets.FieldOffsets[3] // name is index 3
	if nameOffsets.DvSize == 0 {
		t.Error("expected non-zero name DvSize for doc values")
	}

	t.Logf("StoredData: offset=%d, size=%d", offsets.StoredDataOffset, offsets.StoredDataSize)
	t.Logf("StoredIndex: offset=%d, size=%d", offsets.StoredIndexOffset, offsets.StoredIndexSize)
	for i, fo := range offsets.FieldOffsets {
		t.Logf("Field %d: meta=%d/%d, dict=%d/%d, post=%d/%d, dv=%d/%d",
			i, fo.MetaOffset, fo.MetaSize, fo.DictOffset, fo.DictSize,
			fo.PostOffset, fo.PostSize, fo.DvOffset, fo.DvSize)
	}
}
