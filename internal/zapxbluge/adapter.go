// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package zapxbluge

import (
	"fmt"
	"io"
	"math"

	roaringv1 "github.com/RoaringBitmap/roaring"
	roaringv2 "github.com/RoaringBitmap/roaring/v2"
	bleveindex "github.com/blevesearch/bleve_index_api"
	scorchseg "github.com/blevesearch/scorch_segment_api/v2"
	blugeseg "github.com/blugelabs/bluge_segment_api"

	zapxtext "github.com/blugelabs/bluge/internal/zapxtext"
)

const (
	Type    = zapxtext.Type
	Version = zapxtext.Version
)

func New(results []blugeseg.Document, normCalc func(string, int) float32) (blugeseg.Segment, uint64, error) {
	zapDocs := make([]bleveindex.Document, len(results))
	for i, doc := range results {
		zapDocs[i] = &documentAdapter{doc: doc}
	}

	seg, bytesWritten, err := zapxtext.NewWithNormCalc(zapDocs, normCalc)
	if err != nil {
		return nil, 0, err
	}
	return &segmentAdapter{inner: seg}, bytesWritten, nil
}

func Load(data *blugeseg.Data) (blugeseg.Segment, error) {
	if data == nil {
		return nil, fmt.Errorf("nil segment data")
	}
	bytes, err := data.Read(0, data.Len())
	if err != nil {
		return nil, err
	}
	seg, err := zapxtext.LoadBytes(bytes)
	if err != nil {
		return nil, err
	}
	return &segmentAdapter{inner: seg}, nil
}

func Merge(segments []blugeseg.Segment, drops []*roaringv1.Bitmap, mergeBufferSize int) blugeseg.Merger {
	zapSegments := make([]scorchseg.Segment, len(segments))
	for i, segment := range segments {
		if adapter, ok := segment.(*segmentAdapter); ok {
			zapSegments[i] = adapter.inner
		}
	}

	zapDrops := make([]*roaringv2.Bitmap, len(drops))
	for i, drop := range drops {
		zapDrops[i] = roaring1To2(drop)
	}

	return &merger{
		segments: zapSegments,
		drops:    zapDrops,
	}
}

type documentAdapter struct {
	doc blugeseg.Document
}

func (d *documentAdapter) ID() string {
	var id string
	d.doc.EachField(func(field blugeseg.Field) {
		if id == "" && field.Name() == "_id" {
			id = string(field.Value())
		}
	})
	return id
}

func (d *documentAdapter) Size() int {
	var rv int
	d.doc.EachField(func(field blugeseg.Field) {
		rv += len(field.Name()) + len(field.Value())
	})
	return rv
}

func (d *documentAdapter) VisitFields(visitor bleveindex.FieldVisitor) {
	d.doc.EachField(func(field blugeseg.Field) {
		visitor(&fieldAdapter{field: field})
	})
}

func (d *documentAdapter) VisitComposite(visitor bleveindex.CompositeFieldVisitor) {}

func (d *documentAdapter) HasComposite() bool {
	return false
}

func (d *documentAdapter) NumPlainTextBytes() uint64 {
	var rv uint64
	d.doc.EachField(func(field blugeseg.Field) {
		rv += uint64(len(field.Value()))
	})
	return rv
}

func (d *documentAdapter) AddIDField() {}

func (d *documentAdapter) StoredFieldsBytes() uint64 {
	var rv uint64
	d.doc.EachField(func(field blugeseg.Field) {
		if field.Store() {
			rv += uint64(len(field.Value()))
		}
	})
	return rv
}

func (d *documentAdapter) Indexed() bool {
	var indexed bool
	d.doc.EachField(func(field blugeseg.Field) {
		if field.Index() {
			indexed = true
		}
	})
	return indexed
}

type fieldAdapter struct {
	field blugeseg.Field
}

func (f *fieldAdapter) Name() string {
	return f.field.Name()
}

func (f *fieldAdapter) Value() []byte {
	return f.field.Value()
}

func (f *fieldAdapter) ArrayPositions() []uint64 {
	return nil
}

func (f *fieldAdapter) EncodedFieldType() byte {
	return 't'
}

func (f *fieldAdapter) Analyze() {}

func (f *fieldAdapter) Options() bleveindex.FieldIndexingOptions {
	var rv bleveindex.FieldIndexingOptions
	if f.field.Index() {
		rv |= bleveindex.IndexField
	}
	if f.field.Store() {
		rv |= bleveindex.StoreField
	}
	if f.field.IndexDocValues() {
		rv |= bleveindex.DocValues
	}
	if f.hasLocations() {
		rv |= bleveindex.IncludeTermVectors
	}
	return rv
}

func (f *fieldAdapter) AnalyzedLength() int {
	return f.field.Length()
}

func (f *fieldAdapter) AnalyzedTokenFrequencies() bleveindex.TokenFrequencies {
	rv := make(bleveindex.TokenFrequencies)
	f.field.EachTerm(func(term blugeseg.FieldTerm) {
		termBytes := term.Term()
		tf := &bleveindex.TokenFreq{
			Term: append([]byte(nil), termBytes...),
		}
		tf.SetFrequency(term.Frequency())
		term.EachLocation(func(location blugeseg.Location) {
			fieldName := location.Field()
			if fieldName == "" {
				fieldName = f.field.Name()
			}
			tf.Locations = append(tf.Locations, &bleveindex.TokenLocation{
				Field:    fieldName,
				Start:    location.Start(),
				End:      location.End(),
				Position: location.Pos(),
			})
		})
		rv[string(termBytes)] = tf
	})
	return rv
}

func (f *fieldAdapter) NumPlainTextBytes() uint64 {
	return uint64(len(f.field.Value()))
}

func (f *fieldAdapter) hasLocations() bool {
	var hasLocations bool
	f.field.EachTerm(func(term blugeseg.FieldTerm) {
		term.EachLocation(func(location blugeseg.Location) {
			hasLocations = true
		})
	})
	return hasLocations
}

type segmentAdapter struct {
	inner scorchseg.Segment
}

func (s *segmentAdapter) Dictionary(field string) (blugeseg.Dictionary, error) {
	dict, err := s.inner.Dictionary(field)
	if err != nil {
		return nil, err
	}
	return &dictionaryAdapter{inner: dict}, nil
}

func (s *segmentAdapter) VisitStoredFields(num uint64, visitor blugeseg.StoredFieldVisitor) error {
	return s.inner.VisitStoredFields(num, func(field string, typ byte, value []byte, pos []uint64) bool {
		return visitor(field, value)
	})
}

func (s *segmentAdapter) Count() uint64 {
	return s.inner.Count()
}

func (s *segmentAdapter) DocsMatchingTerms(terms []blugeseg.Term) (*roaringv1.Bitmap, error) {
	rv := roaringv1.NewBitmap()
	for _, term := range terms {
		dict, err := s.inner.Dictionary(term.Field())
		if err != nil {
			return nil, err
		}
		postings, err := dict.PostingsList(term.Term(), nil, nil)
		if err != nil {
			return nil, err
		}
		itr := postings.Iterator(false, false, false, nil)
		for {
			next, err := itr.Next()
			if err != nil {
				return nil, err
			}
			if next == nil {
				break
			}
			rv.Add(uint32(next.Number()))
		}
	}
	return rv, nil
}

func (s *segmentAdapter) Fields() []string {
	return s.inner.Fields()
}

func (s *segmentAdapter) CollectionStats(field string) (blugeseg.CollectionStats, error) {
	statsProvider, ok := s.inner.(interface {
		FieldStats(string) (uint64, uint64, bool)
	})
	if !ok {
		return nil, fmt.Errorf("segment type %T does not expose field statistics", s.inner)
	}
	documentCount, sumTotalTermFrequency, ok := statsProvider.FieldStats(field)
	if !ok {
		return &collectionStats{}, nil
	}
	return &collectionStats{
		totalDocCount:    s.inner.Count(),
		docCount:         documentCount,
		sumTotalTermFreq: sumTotalTermFrequency,
	}, nil
}

func (s *segmentAdapter) Size() int {
	return s.inner.Size()
}

func (s *segmentAdapter) DocumentValueReader(fields []string) (blugeseg.DocumentValueReader, error) {
	visitable, _ := s.inner.(scorchseg.DocValueVisitable)
	return &documentValueReaderAdapter{inner: visitable, fields: fields}, nil
}

func (s *segmentAdapter) WriteTo(w io.Writer, closeCh chan struct{}) (int64, error) {
	if writerTo, ok := s.inner.(interface {
		WriteTo(io.Writer) (int64, error)
	}); ok {
		return writerTo.WriteTo(w)
	}
	return 0, fmt.Errorf("segment type %T cannot be written", s.inner)
}

func (s *segmentAdapter) Type() string {
	return Type
}

func (s *segmentAdapter) Version() uint32 {
	return Version
}

type collectionStats struct {
	totalDocCount    uint64
	docCount         uint64
	sumTotalTermFreq uint64
}

func (c *collectionStats) TotalDocumentCount() uint64 {
	return c.totalDocCount
}

func (c *collectionStats) DocumentCount() uint64 {
	return c.docCount
}

func (c *collectionStats) SumTotalTermFrequency() uint64 {
	return c.sumTotalTermFreq
}

func (c *collectionStats) Merge(other blugeseg.CollectionStats) {
	c.totalDocCount += other.TotalDocumentCount()
	c.docCount += other.DocumentCount()
	c.sumTotalTermFreq += other.SumTotalTermFrequency()
}

type dictionaryAdapter struct {
	inner scorchseg.TermDictionary
}

func (d *dictionaryAdapter) Contains(key []byte) (bool, error) {
	return d.inner.Contains(key)
}

func (d *dictionaryAdapter) Close() error {
	return nil
}

func (d *dictionaryAdapter) PostingsList(term []byte, except *roaringv1.Bitmap,
	prealloc blugeseg.PostingsList) (blugeseg.PostingsList, error) {
	var innerPrealloc scorchseg.PostingsList
	if p, ok := prealloc.(*postingsListAdapter); ok {
		innerPrealloc = p.inner
	}
	inner, err := d.inner.PostingsList(term, roaring1To2(except), innerPrealloc)
	if err != nil {
		return nil, err
	}
	return &postingsListAdapter{inner: inner}, nil
}

func (d *dictionaryAdapter) Iterator(a blugeseg.Automaton,
	startKeyInclusive, endKeyExclusive []byte) blugeseg.DictionaryIterator {
	return &dictionaryIteratorAdapter{
		inner: d.inner.AutomatonIterator(toScorchAutomaton(a), startKeyInclusive, endKeyExclusive),
	}
}

type dictionaryIteratorAdapter struct {
	inner scorchseg.DictionaryIterator
}

func (d *dictionaryIteratorAdapter) Next() (blugeseg.DictionaryEntry, error) {
	entry, err := d.inner.Next()
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	return dictionaryEntry{term: entry.Term, count: entry.Count}, nil
}

func (d *dictionaryIteratorAdapter) Close() error {
	return nil
}

type dictionaryEntry struct {
	term  string
	count uint64
}

func (d dictionaryEntry) Term() string {
	return d.term
}

func (d dictionaryEntry) Count() uint64 {
	return d.count
}

type postingsListAdapter struct {
	inner scorchseg.PostingsList
}

func (p *postingsListAdapter) Iterator(includeFreq, includeNorm, includeLocations bool,
	prealloc blugeseg.PostingsIterator) (blugeseg.PostingsIterator, error) {
	var innerPrealloc scorchseg.PostingsIterator
	if itr, ok := prealloc.(*postingsIteratorAdapter); ok {
		innerPrealloc = itr.inner
	}
	inner := p.inner.Iterator(includeFreq, includeNorm, includeLocations, innerPrealloc)
	return &postingsIteratorAdapter{inner: inner, count: p.inner.Count()}, nil
}

func (p *postingsListAdapter) Size() int {
	return p.inner.Size()
}

func (p *postingsListAdapter) Count() uint64 {
	return p.inner.Count()
}

type postingsIteratorAdapter struct {
	inner   scorchseg.PostingsIterator
	current postingAdapter
	count   uint64
}

func (p *postingsIteratorAdapter) Next() (blugeseg.Posting, error) {
	posting, err := p.inner.Next()
	if err != nil || posting == nil {
		return nil, err
	}
	p.current.inner = posting
	p.current.hasNumber = false
	return &p.current, nil
}

func (p *postingsIteratorAdapter) Advance(docNum uint64) (blugeseg.Posting, error) {
	posting, err := p.inner.Advance(docNum)
	if err != nil || posting == nil {
		return nil, err
	}
	p.current.inner = posting
	p.current.hasNumber = false
	return &p.current, nil
}

func (p *postingsIteratorAdapter) Size() int {
	return p.inner.Size()
}

func (p *postingsIteratorAdapter) Empty() bool {
	return p.count == 0
}

func (p *postingsIteratorAdapter) Count() uint64 {
	return p.count
}

func (p *postingsIteratorAdapter) Close() error {
	return nil
}

func (p *postingsIteratorAdapter) ActualBitmap() *roaringv1.Bitmap {
	optimizable, ok := p.inner.(scorchseg.OptimizablePostingsIterator)
	if !ok {
		return nil
	}
	return roaring2To1(optimizable.ActualBitmap())
}

func (p *postingsIteratorAdapter) DocNum1Hit() (uint64, bool) {
	optimizable, ok := p.inner.(scorchseg.OptimizablePostingsIterator)
	if !ok {
		return 0, false
	}
	return optimizable.DocNum1Hit()
}

func (p *postingsIteratorAdapter) ReplaceActual(actual *roaringv1.Bitmap) {
	optimizable, ok := p.inner.(scorchseg.OptimizablePostingsIterator)
	if !ok {
		return
	}
	optimizable.ReplaceActual(roaring1To2(actual))
}

type postingAdapter struct {
	inner     scorchseg.Posting
	number    uint64
	hasNumber bool
}

func (p *postingAdapter) Number() uint64 {
	if p.hasNumber {
		return p.number
	}
	return p.inner.Number()
}

func (p *postingAdapter) SetNumber(number uint64) {
	p.number = number
	p.hasNumber = true
}

func (p *postingAdapter) Frequency() int {
	return int(p.inner.Frequency())
}

func (p *postingAdapter) Norm() float64 {
	if normBits, ok := p.inner.(interface{ NormUint64() uint64 }); ok {
		return float64(math.Float32frombits(uint32(normBits.NormUint64())))
	}
	return p.inner.Norm()
}

func (p *postingAdapter) NormUint64() uint64 {
	if normBits, ok := p.inner.(interface{ NormUint64() uint64 }); ok {
		return normBits.NormUint64()
	}
	return uint64(math.Float32bits(float32(p.inner.Norm())))
}

func (p *postingAdapter) Locations() []blugeseg.Location {
	innerLocs := p.inner.Locations()
	if len(innerLocs) == 0 {
		return nil
	}
	rv := make([]blugeseg.Location, len(innerLocs))
	for i, loc := range innerLocs {
		rv[i] = locationAdapter{inner: loc}
	}
	return rv
}

func (p *postingAdapter) Size() int {
	return p.inner.Size()
}

type locationAdapter struct {
	inner scorchseg.Location
}

func (l locationAdapter) Field() string {
	return l.inner.Field()
}

func (l locationAdapter) Start() int {
	return int(l.inner.Start())
}

func (l locationAdapter) End() int {
	return int(l.inner.End())
}

func (l locationAdapter) Pos() int {
	return int(l.inner.Pos())
}

func (l locationAdapter) Size() int {
	return l.inner.Size()
}

type documentValueReaderAdapter struct {
	inner  scorchseg.DocValueVisitable
	fields []string
	state  scorchseg.DocVisitState
}

func (d *documentValueReaderAdapter) VisitDocumentValues(number uint64,
	visitor blugeseg.DocumentValueVisitor) error {
	if d.inner == nil {
		return nil
	}
	state, err := d.inner.VisitDocValues(number, d.fields, func(field string, term []byte) {
		visitor(field, term)
	}, d.state)
	d.state = state
	return err
}

type merger struct {
	segments    []scorchseg.Segment
	drops       []*roaringv2.Bitmap
	documentMap [][]uint64
}

func (m *merger) WriteTo(w io.Writer, closeCh chan struct{}) (int64, error) {
	for i, segment := range m.segments {
		if segment == nil {
			return 0, fmt.Errorf("cannot merge non-zapx segment at index %d", i)
		}
	}
	docNums, bytesWritten, err := zapxtext.MergeToWriter(m.segments, m.drops, w, closeCh)
	if err != nil {
		return 0, err
	}
	m.documentMap = docNums
	return int64(bytesWritten), nil
}

func (m *merger) DocumentNumbers() [][]uint64 {
	return m.documentMap
}

type automatonAdapter struct {
	inner blugeseg.Automaton
}

func (a automatonAdapter) Start() int {
	return a.inner.Start()
}

func (a automatonAdapter) IsMatch(state int) bool {
	return a.inner.IsMatch(state)
}

func (a automatonAdapter) CanMatch(state int) bool {
	return a.inner.CanMatch(state)
}

func (a automatonAdapter) WillAlwaysMatch(state int) bool {
	return a.inner.WillAlwaysMatch(state)
}

func (a automatonAdapter) Accept(state int, b byte) int {
	return a.inner.Accept(state, b)
}

func toScorchAutomaton(a blugeseg.Automaton) scorchseg.Automaton {
	if a == nil {
		return nil
	}
	if rv, ok := a.(scorchseg.Automaton); ok {
		return rv
	}
	return automatonAdapter{inner: a}
}

func roaring1To2(src *roaringv1.Bitmap) *roaringv2.Bitmap {
	if src == nil {
		return nil
	}
	rv := roaringv2.New()
	itr := src.Iterator()
	for itr.HasNext() {
		rv.Add(itr.Next())
	}
	return rv
}

func roaring2To1(src *roaringv2.Bitmap) *roaringv1.Bitmap {
	if src == nil {
		return nil
	}
	rv := roaringv1.NewBitmap()
	itr := src.Iterator()
	for itr.HasNext() {
		rv.Add(itr.Next())
	}
	return rv
}
