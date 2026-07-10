// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package zap

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// New creates an in-memory zapx text segment.
func New(results []index.Document) (seg.Segment, uint64, error) {
	return new(ZapPlugin).New(results)
}

// NewWithNormCalc creates a segment using the caller's field norm encoding.
func NewWithNormCalc(results []index.Document, normCalc func(string, int) float32) (
	seg.Segment, uint64, error) {
	return new(ZapPlugin).newWithChunkMode(results, DefaultChunkMode, nil, normCalc)
}

// LoadBytes opens a zapx segment from bytes owned by the caller.
func LoadBytes(data []byte) (seg.Segment, error) {
	return LoadBytesUsing(data, nil)
}

// LoadBytesUsing opens a zapx segment from bytes owned by the caller.
func LoadBytesUsing(data []byte, config map[string]interface{}) (seg.Segment, error) {
	if len(data) < FooterSize {
		return nil, fmt.Errorf("zap segment too small: %d", len(data))
	}

	crcOffset := len(data) - 4
	verOffset := crcOffset - 4
	chunkOffset := verOffset - 4
	sectionsIndexOffsetPos := chunkOffset - 8
	storedIndexOffsetPos := sectionsIndexOffsetPos - 8
	numDocsOffset := storedIndexOffsetPos - 8
	idLenOffset := numDocsOffset - 4
	if idLenOffset < 0 {
		return nil, fmt.Errorf("zap segment footer is truncated")
	}

	version := binary.BigEndian.Uint32(data[verOffset:crcOffset])
	if version != Version {
		return nil, fmt.Errorf("unsupported version %d != %d", version, Version)
	}

	idLen := int(binary.BigEndian.Uint32(data[idLenOffset:numDocsOffset]))
	idOffset := idLenOffset - idLen
	if idOffset < 0 {
		return nil, fmt.Errorf("zap segment callback id length %d exceeds file size", idLen)
	}

	fileReader, err := NewFileReader(string(data[idOffset:idLenOffset]), nil)
	if err != nil {
		return nil, err
	}

	sb := &SegmentBase{
		mem:                 data[:idOffset],
		memCRC:              binary.BigEndian.Uint32(data[crcOffset:]),
		chunkMode:           binary.BigEndian.Uint32(data[chunkOffset:verOffset]),
		numDocs:             binary.BigEndian.Uint64(data[numDocsOffset:storedIndexOffsetPos]),
		storedIndexOffset:   binary.BigEndian.Uint64(data[storedIndexOffsetPos:sectionsIndexOffsetPos]),
		sectionsIndexOffset: binary.BigEndian.Uint64(data[sectionsIndexOffsetPos:chunkOffset]),
		fieldDvReaders:      make([][]*docValueReader, len(segmentSections)),
		updatedFields:       make(map[string]*index.UpdateFieldInfo),
		invIndexCache:       newInvertedIndexCache(),
		vecIndexCache:       newVectorIndexCache(),
		synIndexCache:       newSynonymIndexCache(),
		nstIndexCache:       newNestedIndexCache(),
		fieldsMap:           make(map[string]uint16),
		fieldsOptions:       make(map[string]index.FieldIndexingOptions),
		fieldsInv:           make([]string, 0),
		fieldStats:          make([]fieldStats, 0),
		fileReader:          fileReader,
		config:              config,
	}

	err = sb.loadFields()
	if err != nil {
		return nil, err
	}

	err = sb.loadDvReaders()
	if err != nil {
		return nil, err
	}

	err = sb.nstIndexCache.initialize(sb.numDocs, sb.getEdgeListOffset(), sb.mem)
	if err != nil {
		return nil, err
	}

	sb.updateSize()
	return sb, nil
}

// MergeToWriter merges zapx segments and writes the new zapx segment to w.
func MergeToWriter(segments []seg.Segment, drops []*roaring.Bitmap, w io.Writer,
	closeCh chan struct{}) ([][]uint64, uint64, error) {
	return MergeToWriterUsing(segments, drops, w, closeCh, nil, nil)
}

// MergeToWriterUsing merges zapx segments and writes the new zapx segment to w.
func MergeToWriterUsing(segments []seg.Segment, drops []*roaring.Bitmap, w io.Writer,
	closeCh chan struct{}, stats seg.StatsReporter, config map[string]interface{}) (
	[][]uint64, uint64, error) {
	if w == nil {
		return nil, 0, fmt.Errorf("invalid writer found")
	}
	if len(drops) != len(segments) {
		return nil, 0, fmt.Errorf("drops length %d does not match segments length %d", len(drops), len(segments))
	}

	segmentBases := make([]*SegmentBase, len(segments))
	for i, segment := range segments {
		switch segmentx := segment.(type) {
		case *Segment:
			segmentBases[i] = &segmentx.SegmentBase
		case *SegmentBase:
			segmentBases[i] = segmentx
		default:
			return nil, 0, fmt.Errorf("unexpected segment type: %T", segment)
		}
	}

	bw := bufio.NewWriterSize(w, DefaultFileMergerBufferSize)
	cw := NewCountHashWriterWithStatsReporter(bw, stats)
	fw, err := NewFileWriter(cw, nil)
	if err != nil {
		return nil, 0, err
	}

	newDocNums, numDocs, storedIndexOffset, _, _, sectionsIndexOffset, err :=
		mergeToWriter(segmentBases, drops, DefaultChunkMode, fw, closeCh, config)
	if err != nil {
		return nil, 0, err
	}

	err = persistFooter(numDocs, storedIndexOffset, sectionsIndexOffset, DefaultChunkMode, cw.Sum32(), fw, fw.id)
	if err != nil {
		return nil, 0, err
	}

	err = bw.Flush()
	if err != nil {
		return nil, 0, err
	}

	return newDocNums, uint64(cw.Count()), nil
}
