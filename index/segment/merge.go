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
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/golang/snappy"
)

// Merger merges multiple ZIP segments into a single ZIP segment
type merger struct {
	segments   []*segment
	drops      []*roaring.Bitmap
	bufSize    int
	newDocNums [][]uint64
}

// Merge creates a merger for ZIP segments that outputs ZIP format.
// All input segments must be *segment; mixed segment types are not supported.
func Merge(segments []Segment, drops []*roaring.Bitmap, bufSize int) (Merger, error) {
	// Validate all segments are *segment
	zipSegs := make([]*segment, len(segments))
	for i, seg := range segments {
		zs, ok := seg.(*segment)
		if !ok {
			return nil, fmt.Errorf("Merge requires Segment, got %T at index %d", seg, i)
		}
		zipSegs[i] = zs
	}
	return &merger{
		segments: zipSegs,
		drops:    drops,
		bufSize:  bufSize,
	}, nil
}

// WriteTo merges the segments and writes the result to w.
// The closeCh can be used to signal cancellation.
func (m *merger) WriteTo(w io.Writer, closeCh chan struct{}) (int64, error) {
	if len(m.segments) == 0 {
		return 0, nil
	}

	// Compute unified field list and check if fields are the same
	fieldsSame, fieldsInv := mergeZipFields(m.segments)
	fieldsMap := mapFields(fieldsInv)

	// Compute new document count
	numDocs := computeZipNewDocCount(m.segments, m.drops)

	if isClosed(closeCh) {
		return 0, ErrClosed
	}

	// Create ZIP segment writer
	zw := newZipSegmentWriter()
	zw.SetFieldNames(fieldsInv)

	// Merge stored fields and build document number mapping
	var err error
	m.newDocNums, err = m.mergeZipStoredFields(zw, fieldsMap, fieldsInv, fieldsSame, numDocs, closeCh)
	if err != nil {
		return 0, err
	}

	if numDocs > 0 {
		// Merge fields (dictionary, postings, doc values)
		err = m.mergeZipFields(zw, fieldsInv, fieldsMap, numDocs, closeCh)
		if err != nil {
			return 0, err
		}
	}

	// Get chunk mode from first segment (all should be the same)
	chunkMode := m.segments[0].footer.chunkMode

	// Write the final ZIP
	zipData, err := zw.WriteZip(numDocs, Version, chunkMode)
	if err != nil {
		return 0, err
	}

	n, err := w.Write(zipData)
	return int64(n), err
}

// DocumentNumbers returns the mapping from old document numbers to new document numbers
// for each input segment. This is populated after WriteTo completes.
func (m *merger) DocumentNumbers() [][]uint64 {
	return m.newDocNums
}

// mergeZipFields computes the unified field list for ZIP segments
func mergeZipFields(segments []*segment) (same bool, fields []string) {
	same = true

	var segment0Fields []string
	if len(segments) > 0 {
		segment0Fields = segments[0].Fields()
	}

	fieldsExist := map[string]struct{}{}
	for _, seg := range segments {
		segFields := seg.Fields()
		for fieldi, field := range segFields {
			fieldsExist[field] = struct{}{}
			if len(segment0Fields) != len(segFields) || segment0Fields[fieldi] != field {
				same = false
			}
		}
	}

	fields = make([]string, 0, len(fieldsExist))
	// ensure _id stays first
	fields = append(fields, _idFieldName)
	for k := range fieldsExist {
		if k != _idFieldName {
			fields = append(fields, k)
		}
	}

	sort.Strings(fields[1:]) // leave _id as first

	return same, fields
}

// computeZipNewDocCount determines how many documents will be in the merged segment
func computeZipNewDocCount(segments []*segment, drops []*roaring.Bitmap) uint64 {
	var newDocCount uint64
	for segI, seg := range segments {
		newDocCount += seg.footer.numDocs
		if drops[segI] != nil {
			newDocCount -= drops[segI].GetCardinality()
		}
	}
	return newDocCount
}

// mergeZipStoredFields merges stored fields from all segments
func (m *merger) mergeZipStoredFields(zw *zipSegmentWriter, fieldsMap map[string]uint16,
	fieldsInv []string, fieldsSame bool, newSegDocCount uint64, closeCh chan struct{}) ([][]uint64, error) {

	newDocNums := make([][]uint64, len(m.segments))
	var newDocNum uint64

	storedData := zw.StoredData()
	storedIndex := zw.StoredIndex()

	var metaBuf bytes.Buffer
	varBuf := make([]byte, binary.MaxVarintLen64)
	metaEncode := func(val uint64) (int, error) {
		wb := binary.PutUvarint(varBuf, val)
		return metaBuf.Write(varBuf[:wb])
	}

	vals := make([][][]byte, len(fieldsInv))
	var data, compressed []byte

	// For each segment
	for segI, seg := range m.segments {
		if isClosed(closeCh) {
			return nil, ErrClosed
		}

		segNewDocNums := make([]uint64, seg.footer.numDocs)
		dropsI := m.drops[segI]

		// Optimize when fields are the same and no deletions
		if fieldsSame && (dropsI == nil || dropsI.GetCardinality() == 0) {
			err := m.copyZipStoredDocs(seg, newDocNum, storedData, storedIndex)
			if err != nil {
				return nil, err
			}

			for i := uint64(0); i < seg.footer.numDocs; i++ {
				segNewDocNums[i] = newDocNum
				newDocNum++
			}
			newDocNums[segI] = segNewDocNums
			continue
		}

		// For each document in segment
		for docNum := uint64(0); docNum < seg.footer.numDocs; docNum++ {
			if dropsI != nil && dropsI.Contains(uint32(docNum)) {
				segNewDocNums[docNum] = docDropped
				continue
			}

			segNewDocNums[docNum] = newDocNum

			// Reset for this document
			metaBuf.Reset()
			data = data[:0]
			for i := range vals {
				vals[i] = vals[i][:0]
			}

			// Visit stored fields and collect values
			err := seg.VisitStoredFields(docNum, func(field string, value []byte) bool {
				fieldID := int(fieldsMap[field]) - 1
				if fieldID >= 0 && fieldID < len(vals) {
					// Make a copy of value since it may be reused
					valueCopy := make([]byte, len(value))
					copy(valueCopy, value)
					vals[fieldID] = append(vals[fieldID], valueCopy)
				}
				return true
			})
			if err != nil {
				return nil, err
			}

			// Encode stored fields in field order
			curr := 0
			for fieldID := 0; fieldID < len(fieldsInv); fieldID++ {
				storedFieldValues := vals[fieldID]
				var err2 error
				curr, data, err2 = encodeStoredFieldValues(fieldID, storedFieldValues, curr, metaEncode, data)
				if err2 != nil {
					return nil, err2
				}
			}

			metaBytes := metaBuf.Bytes()
			compressed = snappy.Encode(compressed[:cap(compressed)], data)

			// Record offset in stored index
			offset := uint64(storedData.Count())
			err = binary.Write(storedIndex, binary.BigEndian, offset)
			if err != nil {
				return nil, err
			}

			// Write meta len, compressed len, meta, compressed
			err = writeUvarints(storedData, uint64(len(metaBytes)), uint64(len(compressed)))
			if err != nil {
				return nil, err
			}
			_, err = storedData.Write(metaBytes)
			if err != nil {
				return nil, err
			}
			_, err = storedData.Write(compressed)
			if err != nil {
				return nil, err
			}

			newDocNum++
		}

		newDocNums[segI] = segNewDocNums
	}

	return newDocNums, nil
}

// copyZipStoredDocs copies stored docs directly when fields are the same and no drops
func (m *merger) copyZipStoredDocs(seg *segment, startDocNum uint64,
	storedData, storedIndex *zipEntry) error {

	if seg.footer.numDocs == 0 {
		return nil
	}

	// Get the source stored data using Data abstraction for mmap support
	srcStoredData, err := seg.data.Read(int(seg.storedDataOffset), int(seg.storedDataOffset+seg.storedDataSize))
	if err != nil {
		return err
	}
	srcStoredIndex, err := seg.data.Read(int(seg.storedIndexOffset), int(seg.storedIndexOffset+seg.storedIndexSize))
	if err != nil {
		return err
	}

	// Get offset of first document in source
	firstDocOffset := binary.BigEndian.Uint64(srcStoredIndex[0:fileAddrWidth])

	// Calculate the base offset in the destination
	destBaseOffset := uint64(storedData.Count())

	// Copy all stored data
	_, err = storedData.Write(srcStoredData)
	if err != nil {
		return err
	}

	// Write remapped offsets to stored index
	for docNum := uint64(0); docNum < seg.footer.numDocs; docNum++ {
		indexPos := docNum * fileAddrWidth
		srcOffset := binary.BigEndian.Uint64(srcStoredIndex[indexPos : indexPos+fileAddrWidth])
		// Remap offset: new offset = destBaseOffset + (srcOffset - firstDocOffset)
		newOffset := destBaseOffset + (srcOffset - firstDocOffset)
		err = binary.Write(storedIndex, binary.BigEndian, newOffset)
		if err != nil {
			return err
		}
	}

	return nil
}

// mergeZipFields merges dictionary, postings, and doc values for all fields
func (m *merger) mergeZipFields(zw *zipSegmentWriter, fieldsInv []string,
	fieldsMap map[string]uint16, newSegDocCount uint64, closeCh chan struct{}) error {

	// Get chunk mode from first segment
	chunkMode := m.segments[0].footer.chunkMode

	var vellumBuf bytes.Buffer
	newVellum, err := vellum.New(&vellumBuf, nil)
	if err != nil {
		return err
	}

	newRoaring := roaring.NewBitmap()
	fieldDocTracking := roaring.NewBitmap()

	// Initialize encoders
	tfEncoder := newChunkedIntCoder(uint64(legacyChunkMode), newSegDocCount-1)
	locEncoder := newChunkedIntCoder(uint64(legacyChunkMode), newSegDocCount-1)

	bufMaxVarintLen64 := make([]byte, binary.MaxVarintLen64)

	// For each field
	for fieldID, fieldName := range fieldsInv {
		if isClosed(closeCh) {
			return ErrClosed
		}

		err = m.mergeZipFieldDictPostings(zw, fieldName, uint16(fieldID), fieldsMap,
			newSegDocCount, chunkMode, newRoaring, fieldDocTracking,
			tfEncoder, locEncoder, newVellum, &vellumBuf, bufMaxVarintLen64, closeCh)
		if err != nil {
			return err
		}

		// Reset vellum for next field
		vellumBuf.Reset()
		err = newVellum.Reset(&vellumBuf)
		if err != nil {
			return err
		}
	}

	return nil
}

// mergeZipFieldDictPostings merges dictionary and postings for a single field
func (m *merger) mergeZipFieldDictPostings(zw *zipSegmentWriter, fieldName string, fieldID uint16,
	fieldsMap map[string]uint16, newSegDocCount uint64, chunkMode uint32,
	newRoaring, fieldDocTracking *roaring.Bitmap,
	tfEncoder, locEncoder *chunkedIntCoder,
	newVellum *vellum.Builder, vellumBuf *bytes.Buffer,
	bufMaxVarintLen64 []byte, closeCh chan struct{}) error {

	// Collect dictionaries and iterators from segments that have this field
	var dicts []*dictionary
	var itrs []vellum.Iterator
	var newDocNums [][]uint64
	var drops []*roaring.Bitmap
	var segmentsInFocus []*segment

	for segI, seg := range m.segments {
		if isClosed(closeCh) {
			return ErrClosed
		}

		dict, err := seg.dictionary(fieldName)
		if err != nil {
			return err
		}
		if dict != nil && dict.fst != nil {
			itr, err := dict.fst.Iterator(nil, nil)
			if err != nil && err != vellum.ErrIteratorDone {
				return err
			}
			if itr != nil {
				dicts = append(dicts, dict)
				itrs = append(itrs, itr)
				newDocNums = append(newDocNums, m.newDocNums[segI])
				if m.drops[segI] != nil && !m.drops[segI].IsEmpty() {
					drops = append(drops, m.drops[segI])
				} else {
					drops = append(drops, nil)
				}
				segmentsInFocus = append(segmentsInFocus, seg)
			}
		}
	}

	if len(itrs) == 0 {
		// No data for this field, write empty meta
		metaEntry := zw.FieldMeta(fieldName)
		n := binary.PutUvarint(bufMaxVarintLen64, 0) // docCount = 0
		_, err := metaEntry.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return err
		}
		n = binary.PutUvarint(bufMaxVarintLen64, 0) // freqCount = 0
		_, err = metaEntry.Write(bufMaxVarintLen64[:n])
		return err
	}

	// Debug: verify dicts and itrs have same length
	if len(dicts) != len(itrs) {
		return fmt.Errorf("mergeZipFieldDictPostings: dicts length %d != itrs length %d for field %s", len(dicts), len(itrs), fieldName)
	}
	for i, d := range dicts {
		if d == nil {
			return fmt.Errorf("mergeZipFieldDictPostings: dicts[%d] is nil for field %s", i, fieldName)
		}
	}

	postingsEntry := zw.FieldPostings(fieldName)

	// Write a padding byte to ensure TF data never starts at offset 0
	// This avoids collision with termNotEncoded=0 sentinel value
	postingsEntry.Write([]byte{0})

	var prevTerm []byte
	var lastDocNum, lastFreq, lastNorm uint64
	var totalFreq uint64

	newRoaring.Clear()
	fieldDocTracking.Clear()

	var postings *postingsList
	var postItr *postingsIterator
	var bufLoc []uint64

	enumerator, err := newEnumerator(itrs)
	if err != nil && err != vellum.ErrIteratorDone {
		return err
	}

	for err == nil {
		term, itrI, postingsOffset := enumerator.Current()

		if !bytes.Equal(prevTerm, term) {
			if isClosed(closeCh) {
				return ErrClosed
			}

			// Write out previous term's data
			err = m.finishZipTerm(postingsEntry, newRoaring, tfEncoder, locEncoder,
				newVellum, bufMaxVarintLen64, prevTerm, &lastDocNum, &lastFreq, &lastNorm)
			if err != nil {
				return err
			}
		}

		if !bytes.Equal(prevTerm, term) || prevTerm == nil {
			// Prepare for new term
			var newCard uint64
			lowItrIdxs, lowItrVals := enumerator.GetLowIdxsAndValues()
			for i, idx := range lowItrIdxs {
				// Debug check
				if idx < 0 || idx >= len(dicts) {
					return fmt.Errorf("mergeZipFieldDictPostings: idx %d out of bounds for dicts (len=%d), field=%s, term=%s", idx, len(dicts), fieldName, string(term))
				}
				if dicts[idx] == nil {
					return fmt.Errorf("mergeZipFieldDictPostings: dicts[%d] is nil during enum, field=%s, term=%s", idx, fieldName, string(term))
				}
				var pl *postingsList
				pl, err = dicts[idx].postingsListFromOffset(lowItrVals[i], drops[idx], nil)
				if err != nil {
					return err
				}
				newCard += pl.Count()
				totalFreq += newCard
			}

			chunkSize, err := getChunkSize(chunkMode, newCard, newSegDocCount)
			if err != nil {
				return err
			}
			tfEncoder.SetChunkSize(chunkSize, newSegDocCount-1)
			locEncoder.SetChunkSize(chunkSize, newSegDocCount-1)
		}

		postings, err = dicts[itrI].postingsListFromOffset(postingsOffset, drops[itrI], postings)
		if err != nil {
			return err
		}

		postItr, err = postings.iterator(true, true, true, postItr)
		if err != nil {
			return err
		}

		lastDocNum, lastFreq, lastNorm, bufLoc, err = mergeTermFreqNormLocs(
			fieldsMap, postItr, newDocNums[itrI], newRoaring,
			tfEncoder, locEncoder, bufLoc, fieldDocTracking)
		if err != nil {
			return err
		}

		prevTerm = prevTerm[:0]
		prevTerm = append(prevTerm, term...)

		err = enumerator.Next()
	}

	if err != vellum.ErrIteratorDone {
		return err
	}

	// Write final term
	err = m.finishZipTerm(postingsEntry, newRoaring, tfEncoder, locEncoder,
		newVellum, bufMaxVarintLen64, prevTerm, &lastDocNum, &lastFreq, &lastNorm)
	if err != nil {
		return err
	}

	// Close vellum and write dictionary
	err = newVellum.Close()
	if err != nil {
		return err
	}

	vellumData := vellumBuf.Bytes()
	if len(vellumData) > 0 {
		dictEntry := zw.FieldDictionary(fieldName)
		// Write vellum length then data
		n := binary.PutUvarint(bufMaxVarintLen64, uint64(len(vellumData)))
		_, err = dictEntry.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return err
		}
		_, err = dictEntry.Write(vellumData)
		if err != nil {
			return err
		}
	}

	// Write field metadata
	metaEntry := zw.FieldMeta(fieldName)
	docCount := fieldDocTracking.GetCardinality()
	n := binary.PutUvarint(bufMaxVarintLen64, docCount)
	_, err = metaEntry.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return err
	}
	n = binary.PutUvarint(bufMaxVarintLen64, totalFreq)
	_, err = metaEntry.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return err
	}

	// Merge doc values
	err = m.mergeZipDocValues(zw, fieldName, fieldID, segmentsInFocus, newDocNums, newSegDocCount, closeCh)
	if err != nil {
		return err
	}

	return nil
}

// finishZipTerm writes out the collected data for a term
func (m *merger) finishZipTerm(postingsEntry *zipEntry, newRoaring *roaring.Bitmap,
	tfEncoder, locEncoder *chunkedIntCoder,
	newVellum *vellum.Builder, bufMaxVarintLen64, term []byte,
	lastDocNum, lastFreq, lastNorm *uint64) error {

	if len(term) == 0 {
		return nil
	}

	tfEncoder.Close()
	locEncoder.Close()

	// Determine if we can use 1-hit encoding
	use1HitEncoding := func(termCardinality uint64) (bool, uint64, uint64) {
		if termCardinality == 1 && locEncoder.FinalSize() <= 0 {
			docNum := uint64(newRoaring.Minimum())
			if under32Bits(docNum) && docNum == *lastDocNum && *lastFreq == 1 {
				return true, docNum, *lastNorm
			}
		}
		return false, 0, 0
	}

	postingsOffset, err := writeZipPostings(newRoaring, tfEncoder, locEncoder,
		use1HitEncoding, postingsEntry, bufMaxVarintLen64)
	if err != nil {
		return err
	}

	if postingsOffset > 0 {
		err = newVellum.Insert(term, postingsOffset)
		if err != nil {
			return err
		}
	}

	newRoaring.Clear()
	tfEncoder.Reset()
	locEncoder.Reset()

	*lastDocNum = 0
	*lastFreq = 0
	*lastNorm = 0

	return nil
}

// writeZipPostings writes postings data to a ZIP entry
// The format matches writePostingsZip in newzip.go:
// 1. TF data (chunked int encoding)
// 2. Loc data (chunked int encoding)
// 3. Postings header: tfOffset | locOffset | roaringLen | roaringData
func writeZipPostings(postings *roaring.Bitmap, tfEncoder, locEncoder *chunkedIntCoder,
	use1HitEncoding func(uint64) (bool, uint64, uint64),
	w *zipEntry, bufMaxVarintLen64 []byte) (uint64, error) {

	termCardinality := postings.GetCardinality()
	if termCardinality <= 0 {
		return 0, nil
	}

	if use1HitEncoding != nil {
		encodeAs1Hit, docNum1Hit, normBits1Hit := use1HitEncoding(termCardinality)
		if encodeAs1Hit {
			return fSTValEncode1Hit(docNum1Hit, normBits1Hit), nil
		}
	}

	// Write TF data first, record where it starts
	tfOffset := uint64(w.Count())
	_, err := tfEncoder.Write(w)
	if err != nil {
		return 0, err
	}

	// Write loc data, record where it starts
	locOffset := uint64(w.Count())
	_, err = locEncoder.Write(w)
	if err != nil {
		return 0, err
	}

	// Now write the postings header
	postingsOffset := uint64(w.Count())

	// Write TF offset
	n := binary.PutUvarint(bufMaxVarintLen64, tfOffset)
	_, err = w.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return 0, err
	}

	// Write loc offset
	n = binary.PutUvarint(bufMaxVarintLen64, locOffset)
	_, err = w.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return 0, err
	}

	// Write roaring bitmap
	roaringBytes, err := postings.ToBytes()
	if err != nil {
		return 0, err
	}
	n = binary.PutUvarint(bufMaxVarintLen64, uint64(len(roaringBytes)))
	_, err = w.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return 0, err
	}
	_, err = w.Write(roaringBytes)
	if err != nil {
		return 0, err
	}

	return postingsOffset, nil
}

// mergeZipDocValues merges doc values for a field
func (m *merger) mergeZipDocValues(zw *zipSegmentWriter, fieldName string, fieldID uint16,
	segmentsInFocus []*segment, newDocNums [][]uint64, newSegDocCount uint64,
	closeCh chan struct{}) error {

	dvEntry := zw.FieldDocValues(fieldName)

	// NOTE: doc values continue to use legacy chunk mode
	chunkSize, err := getChunkSize(legacyChunkMode, 0, 0)
	if err != nil {
		return err
	}

	fdvEncoder := newChunkedContentCoder(chunkSize, newSegDocCount-1, dvEntry, true)

	fdvReadersAvailable := false
	var dvIterClone *docValueReader

	for segI, seg := range segmentsInFocus {
		if isClosed(closeCh) {
			return ErrClosed
		}

		fieldIDPlus1 := seg.fieldsMap[fieldName]
		if fieldIDPlus1 == 0 {
			continue
		}

		fieldIDLocal := fieldIDPlus1 - 1
		if dvIter, exists := seg.fieldDvReaders[fieldIDLocal]; exists && dvIter != nil {
			fdvReadersAvailable = true
			dvIterClone = dvIter.cloneInto(dvIterClone)

			// Get the doc values data for this field using Data abstraction for mmap support
			off := seg.fieldOffsets[fieldIDLocal]
			if off.dvSize == 0 {
				continue
			}
			dvData, err := seg.data.Read(int(off.dvOffset), int(off.dvOffset+off.dvSize))
			if err != nil {
				return err
			}

			err = dvIterClone.iterateAllDocValuesFromData(dvData, func(docNum uint64, terms []byte) error {
				if newDocNums[segI][docNum] == docDropped {
					return nil
				}
				return fdvEncoder.Add(newDocNums[segI][docNum], terms)
			})
			if err != nil {
				return err
			}
		}
	}

	if fdvReadersAvailable {
		err = fdvEncoder.Close()
		if err != nil {
			return err
		}

		_, err = fdvEncoder.Write()
		if err != nil {
			return err
		}
	}

	return nil
}
