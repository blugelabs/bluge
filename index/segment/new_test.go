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
	"testing"
)

func TestBuild(t *testing.T) {
	path, cleanup := setupTestDir(t)
	defer cleanup()

	// Test segment build
	zs, err := buildTestSegment()
	if err != nil {
		t.Fatal(err)
	}
	segPath := filepath.Join(path, "ice")
	err = persistZipToFile(zs, segPath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we can load it back
	loaded, closeF, err := openZipFromFile(segPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeF()

	if loaded.Count() != 1 {
		t.Errorf("expected 1 doc, got %d", loaded.Count())
	}
}

func buildTestSegment() (*segment, error) {
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
		return nil, err
	}
	return LoadBytes(zipData)
}

func buildTestSegmentMulti() (*segment, error) {
	results := buildTestAnalysisResultsMulti()

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		return nil, err
	}
	return LoadBytes(zipData)
}

func buildTestSegmentMultiWithChunkFactor(chunkFactor uint32) (*segment, error) {
	results := buildTestAnalysisResultsMulti()

	zipData, _, err := newZipWithChunkMode(results, encodeNorm, chunkFactor)
	if err != nil {
		return nil, err
	}
	return LoadBytes(zipData)
}

func buildTestSegmentMultiWithDifferentFields(includeDocA, includeDocB bool) (*segment, error) {
	results := buildTestAnalysisResultsMultiWithDifferentFields(includeDocA, includeDocB)

	zipData, _, err := NewZip(results, encodeNorm)
	if err != nil {
		return nil, err
	}
	return LoadBytes(zipData)
}

func buildTestSegmentWithDefaultFieldMapping(chunkFactor uint32) (*segment, []string, error) {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", false, false, true),
		NewFakeField("desc", "some thing", false, false, true),
		NewFakeField("tag", "cold", false, false, true),
	}
	doc.FakeComposite("_all", []string{"_id"})

	var fields []string
	fields = append(fields, "_id", "name", "desc", "tag")

	results := []Document{doc}

	zipData, _, err := newZipWithChunkMode(results, encodeNorm, chunkFactor)
	if err != nil {
		return nil, nil, err
	}
	seg, err := LoadBytes(zipData)
	return seg, fields, err
}

func buildTestAnalysisResultsMulti() []Document {
	doc := &FakeDocument{
		NewFakeField("_id", "a", true, false, false),
		NewFakeField("name", "wow", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
		NewFakeField("tag", "dark", true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", "b", true, false, false),
		NewFakeField("name", "who", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
		NewFakeField("tag", "cold", true, true, false),
		NewFakeField("tag", "dark", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	results := []Document{
		doc, doc2,
	}

	return results
}

func buildTestAnalysisResultsMultiWithDifferentFields(includeDocA, includeDocB bool) []Document {
	var results []Document

	if includeDocA {
		doc := &FakeDocument{
			NewFakeField("_id", "a", false, false, false),
			NewFakeField("name", "ABC", false, false, true),
			NewFakeField("dept", "ABC dept", false, false, true),
			NewFakeField("manages.id", "XYZ", false, false, true),
			NewFakeField("manages.count", "1", false, false, true),
		}
		doc.FakeComposite("_all", []string{"_id"})
		results = append(results, doc)
	}

	if includeDocB {
		doc := &FakeDocument{
			NewFakeField("_id", "b", true, false, false),
			NewFakeField("name", "XYZ", false, false, true),
			NewFakeField("dept", "ABC dept", false, false, true),
			NewFakeField("reportsTo.id", "ABC", false, false, true),
		}
		doc.FakeComposite("_all", []string{"_id"})
		results = append(results, doc)
	}

	return results
}

func buildTestSegmentMulti2() (*segment, uint64, error) {
	return buildTestSegmentMultiHelper([]string{"c", "d"})
}

func buildTestSegmentMultiHelper(docIds []string) (*segment, uint64, error) {
	doc := &FakeDocument{
		NewFakeField("_id", docIds[0], true, false, false),
		NewFakeField("name", "mat", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
		NewFakeField("tag", "cold", true, true, false),
		NewFakeField("tag", "dark", true, true, false),
	}
	doc.FakeComposite("_all", []string{"_id"})

	doc2 := &FakeDocument{
		NewFakeField("_id", docIds[1], true, false, false),
		NewFakeField("name", "joa", true, true, false),
		NewFakeField("desc", "some thing", true, true, false),
		NewFakeField("tag", "cold", true, true, false),
		NewFakeField("tag", "dark", true, true, false),
	}
	doc2.FakeComposite("_all", []string{"_id"})

	results := []Document{doc, doc2}

	zipData, size, err := newZipWithChunkMode(results, encodeNorm, 1024)
	if err != nil {
		return nil, 0, err
	}
	seg, err := LoadBytes(zipData)
	return seg, size, err
}
