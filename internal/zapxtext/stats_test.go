// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package zap

import (
	"bytes"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func TestFieldStatsSurviveMerge(t *testing.T) {
	first := newStatsTestSegment(t,
		newStatsTestDocument("1", "alpha alpha beta"),
		newStatsTestDocument("2", "alpha"))
	second := newStatsTestSegment(t,
		newStatsTestDocument("3", "gamma gamma gamma"))

	tests := []struct {
		name      string
		drops     []*roaring.Bitmap
		wantDocs  uint64
		wantTerms uint64
	}{
		{
			name:      "without drops",
			drops:     []*roaring.Bitmap{nil, nil},
			wantDocs:  3,
			wantTerms: 7,
		},
		{
			name:      "with drops",
			drops:     []*roaring.Bitmap{roaring.BitmapOf(0), nil},
			wantDocs:  2,
			wantTerms: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			_, _, err := MergeToWriter([]segment.Segment{first, second}, test.drops,
				&output, make(chan struct{}))
			if err != nil {
				t.Fatal(err)
			}
			merged, err := LoadBytes(output.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			statsProvider := merged.(interface {
				FieldStats(string) (uint64, uint64, bool)
			})
			documentCount, sumTotalTermFrequency, ok := statsProvider.FieldStats("body")
			if !ok {
				t.Fatal("expected body field statistics")
			}
			if documentCount != test.wantDocs || sumTotalTermFrequency != test.wantTerms {
				t.Fatalf("stats: got docs=%d terms=%d want docs=%d terms=%d",
					documentCount, sumTotalTermFrequency, test.wantDocs, test.wantTerms)
			}
		})
	}
}

func newStatsTestSegment(t *testing.T, documents ...index.Document) segment.Segment {
	t.Helper()
	result, _, err := New(documents)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type statsTestDocument struct {
	id     string
	fields []*statsTestField
}

func newStatsTestDocument(id, body string) *statsTestDocument {
	return &statsTestDocument{
		id: id,
		fields: []*statsTestField{
			newStatsTestField("_id", id, index.IndexField|index.StoreField),
			newStatsTestField("body", body, index.IndexField|index.DocValues),
		},
	}
}

func (d *statsTestDocument) ID() string { return d.id }
func (d *statsTestDocument) Size() int {
	var size int
	for _, field := range d.fields {
		size += len(field.name) + len(field.value)
	}
	return size
}
func (d *statsTestDocument) VisitFields(visitor index.FieldVisitor) {
	for _, field := range d.fields {
		visitor(field)
	}
}
func (d *statsTestDocument) VisitComposite(index.CompositeFieldVisitor) {}
func (d *statsTestDocument) HasComposite() bool                         { return false }
func (d *statsTestDocument) NumPlainTextBytes() uint64                  { return uint64(d.Size()) }
func (d *statsTestDocument) AddIDField()                                {}
func (d *statsTestDocument) StoredFieldsBytes() uint64                  { return uint64(len(d.id)) }
func (d *statsTestDocument) Indexed() bool                              { return true }

type statsTestField struct {
	name        string
	value       []byte
	options     index.FieldIndexingOptions
	length      int
	frequencies index.TokenFrequencies
}

func newStatsTestField(name, value string, options index.FieldIndexingOptions) *statsTestField {
	field := &statsTestField{
		name:        name,
		value:       []byte(value),
		options:     options,
		frequencies: make(index.TokenFrequencies),
	}
	for _, term := range strings.Fields(value) {
		field.length++
		frequency := field.frequencies[term]
		if frequency == nil {
			frequency = &index.TokenFreq{Term: []byte(term)}
			field.frequencies[term] = frequency
		}
		frequency.SetFrequency(frequency.Frequency() + 1)
	}
	return field
}

func (f *statsTestField) Name() string                                     { return f.name }
func (f *statsTestField) Value() []byte                                    { return f.value }
func (f *statsTestField) ArrayPositions() []uint64                         { return nil }
func (f *statsTestField) EncodedFieldType() byte                           { return 't' }
func (f *statsTestField) Analyze()                                         {}
func (f *statsTestField) Options() index.FieldIndexingOptions              { return f.options }
func (f *statsTestField) AnalyzedLength() int                              { return f.length }
func (f *statsTestField) AnalyzedTokenFrequencies() index.TokenFrequencies { return f.frequencies }
func (f *statsTestField) NumPlainTextBytes() uint64                        { return uint64(len(f.value)) }
