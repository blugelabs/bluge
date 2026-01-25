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
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

// benchFormat defines how to build and load segments for benchmarking
type benchFormat struct {
	name  string
	build func(docs []Document) ([]byte, error)
	load  func(data []byte) (Segment, error)
}

var benchZip = benchFormat{
	name: "zip",
	build: func(docs []Document) ([]byte, error) {
		data, _, err := NewZip(docs, encodeNorm)
		return data, err
	},
	load: func(data []byte) (Segment, error) {
		return LoadBytes(data)
	},
}

// benchFormats is the list of formats to benchmark.
// ZIP is the only supported format.
var benchFormats = []benchFormat{benchZip}

// generateBenchDocs creates n documents for benchmarking
func generateBenchDocs(n int) []Document {
	return generateBenchDocsWithPrefix(n, "")
}

// generateBenchDocsWithPrefix creates n documents with a prefix for unique IDs across segments
func generateBenchDocsWithPrefix(n int, prefix string) []Document {
	docs := make([]Document, n)
	for i := 0; i < n; i++ {
		doc := &FakeDocument{
			NewFakeField("_id", fmt.Sprintf("%sdoc%d", prefix, i), true, false, false),
			NewFakeField("title", fmt.Sprintf("document number %d title", i), true, true, false),
			NewFakeField("body", fmt.Sprintf("this is the body content for document %d with some extra words", i), true, true, false),
			NewFakeField("category", fmt.Sprintf("cat%d", i%10), true, true, false),
			NewFakeField("author", fmt.Sprintf("author%d", i%5), true, true, false),
			NewFakeField("sortkey", fmt.Sprintf("%08d", i), false, false, true),
		}
		doc.FakeComposite("_all", []string{"_id"})
		docs[i] = doc
	}
	return docs
}

// BenchmarkSegmentBuild benchmarks segment building for various document counts
func BenchmarkSegmentBuild(b *testing.B) {
	docCounts := []int{10, 100, 1000}

	for _, format := range benchFormats {
		for _, count := range docCounts {
			docs := generateBenchDocs(count)
			b.Run(fmt.Sprintf("%s/%d-docs", format.name, count), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_, err := format.build(docs)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkSegmentLoad benchmarks segment loading
func BenchmarkSegmentLoadBytes(b *testing.B) {
	docCounts := []int{10, 100, 1000}

	for _, format := range benchFormats {
		for _, count := range docCounts {
			docs := generateBenchDocs(count)
			data, err := format.build(docs)
			if err != nil {
				b.Fatal(err)
			}

			b.Run(fmt.Sprintf("%s/%d-docs", format.name, count), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for i := 0; i < b.N; i++ {
					_, err := format.load(data)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkDictionaryLookup benchmarks looking up terms in the dictionary
func BenchmarkDictionaryLookup(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dict, err := seg.Dictionary("title")
				if err != nil {
					b.Fatal(err)
				}
				// Look up a term that exists
				_, err = dict.PostingsList([]byte("document"), nil, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPostingsIteration benchmarks iterating through postings
func BenchmarkPostingsIteration(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		dict, err := seg.Dictionary("title")
		if err != nil {
			b.Fatal(err)
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				pl, err := dict.PostingsList([]byte("document"), nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				// Note: ZIP segments don't fully support freq/norm iteration yet,
				// so we benchmark without freq/norm for fair comparison
				itr, err := pl.Iterator(false, false, false, nil)
				if err != nil {
					b.Fatal(err)
				}
				posting, err := itr.Next()
				for posting != nil && err == nil {
					_ = posting.Number()
					posting, err = itr.Next()
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStoredFieldRetrieval benchmarks retrieving stored fields
func BenchmarkStoredFieldRetrieval(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			docNum := uint64(500) // Middle document
			for i := 0; i < b.N; i++ {
				err := seg.VisitStoredFields(docNum, func(field string, value []byte) bool {
					_ = field
					_ = value
					return true
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTermIteration benchmarks iterating through all terms in a field
func BenchmarkTermIteration(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dict, err := seg.Dictionary("title")
				if err != nil {
					b.Fatal(err)
				}
				itr := dict.Iterator(nil, nil, nil)
				entry, err := itr.Next()
				for entry != nil && err == nil {
					_ = entry.Term()
					_ = entry.Count()
					entry, err = itr.Next()
				}
				if err != nil {
					b.Fatal(err)
				}
				itr.Close()
			}
		})
	}
}

// BenchmarkDocValueRetrieval benchmarks retrieving doc values
func BenchmarkDocValueRetrieval(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dvReader, err := seg.DocumentValueReader([]string{"sortkey"})
				if err != nil {
					b.Fatal(err)
				}
				// Read doc values for middle 100 documents
				for docNum := uint64(450); docNum < 550; docNum++ {
					err := dvReader.VisitDocumentValues(docNum, func(field string, term []byte) {
						_ = field
						_ = term
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkDocsMatchingTerms benchmarks the DocsMatchingTerms operation
func BenchmarkDocsMatchingTerms(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		terms := []Term{
			&benchTerm{field: "category", term: []byte("cat0")},
			&benchTerm{field: "category", term: []byte("cat1")},
			&benchTerm{field: "category", term: []byte("cat2")},
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := seg.DocsMatchingTerms(terms)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCollectionStats benchmarks retrieving collection statistics
func BenchmarkCollectionStats(b *testing.B) {
	docs := generateBenchDocs(1000)

	for _, format := range benchFormats {
		data, err := format.build(docs)
		if err != nil {
			b.Fatal(err)
		}
		seg, err := format.load(data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(format.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := seg.CollectionStats("title")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMerge benchmarks merging multiple segments
func BenchmarkMerge(b *testing.B) {
	// Test with different segment counts and document counts per segment
	configs := []struct {
		segmentCount int
		docsPerSeg   int
	}{
		{2, 100},
		{2, 1000},
		{5, 100},
		{5, 1000},
		{10, 100},
	}

	for _, cfg := range configs {
		// Build ZIP segments
		zipSegments := make([]*segment, cfg.segmentCount)
		for i := 0; i < cfg.segmentCount; i++ {
			docs := generateBenchDocsWithPrefix(cfg.docsPerSeg, fmt.Sprintf("seg%d-", i))
			data, _, err := NewZip(docs, encodeNorm)
			if err != nil {
				b.Fatal(err)
			}
			loaded, err := LoadBytes(data)
			if err != nil {
				b.Fatal(err)
			}
			zipSegments[i] = loaded
		}

		// Benchmark ZIP merge
		b.Run(fmt.Sprintf("%d-segs/%d-docs-per-seg", cfg.segmentCount, cfg.docsPerSeg), func(b *testing.B) {
			b.ReportAllocs()
			drops := make([]*roaring.Bitmap, cfg.segmentCount)
			segs := make([]Segment, cfg.segmentCount)
			for i, seg := range zipSegments {
				segs[i] = seg
			}

			for i := 0; i < b.N; i++ {
				merger, err := Merge(segs, drops, 1024)
				if err != nil {
					b.Fatal(err)
				}
				var buf bytes.Buffer
				_, err = merger.WriteTo(&buf, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMergeWithDrops benchmarks merging with document deletion
func BenchmarkMergeWithDrops(b *testing.B) {
	segmentCount := 5
	docsPerSeg := 500
	dropPercentage := 20 // Drop 20% of docs

	// Build ZIP segments
	zipSegments := make([]*segment, segmentCount)
	for i := 0; i < segmentCount; i++ {
		docs := generateBenchDocsWithPrefix(docsPerSeg, fmt.Sprintf("seg%d-", i))
		data, _, err := NewZip(docs, encodeNorm)
		if err != nil {
			b.Fatal(err)
		}
		loaded, err := LoadBytes(data)
		if err != nil {
			b.Fatal(err)
		}
		zipSegments[i] = loaded
	}

	// Create drop bitmaps (drop first 20% of each segment)
	drops := make([]*roaring.Bitmap, segmentCount)
	for i := 0; i < segmentCount; i++ {
		drops[i] = roaring.New()
		dropCount := docsPerSeg * dropPercentage / 100
		for j := 0; j < dropCount; j++ {
			drops[i].Add(uint32(j))
		}
	}

	// Benchmark ZIP merge with drops
	b.Run("with-drops", func(b *testing.B) {
		b.ReportAllocs()
		segs := make([]Segment, segmentCount)
		for i, seg := range zipSegments {
			segs[i] = seg
		}

		for i := 0; i < b.N; i++ {
			merger, err := Merge(segs, drops, 1024)
			if err != nil {
				b.Fatal(err)
			}
			var buf bytes.Buffer
			_, err = merger.WriteTo(&buf, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMergeSelfRepeat benchmarks repeated self-merging (compaction pattern)
func BenchmarkMergeSelfRepeat(b *testing.B) {
	docsCount := 500

	// Build initial ZIP segment
	docs := generateBenchDocs(docsCount)
	zipData, _, err := NewZip(docs, encodeNorm)
	if err != nil {
		b.Fatal(err)
	}
	zipLoaded, err := LoadBytes(zipData)
	if err != nil {
		b.Fatal(err)
	}

	// Benchmark ZIP self-merge
	b.Run("self-merge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			merger, err := Merge([]Segment{zipLoaded}, []*roaring.Bitmap{nil}, 1024)
			if err != nil {
				b.Fatal(err)
			}
			var buf bytes.Buffer
			_, err = merger.WriteTo(&buf, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// benchTerm implements Term for benchmarking
type benchTerm struct {
	field string
	term  []byte
}

func (t *benchTerm) Field() string {
	return t.field
}

func (t *benchTerm) Term() []byte {
	return t.term
}
