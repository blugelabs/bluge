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
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/golang/snappy"
)

// New creates a segment from the source documents.
// This function creates ZIP format segments and returns a usable Segment
// directly without re-parsing the ZIP data.
func New(results []Document, normCalc func(string, int) float32) (
	Segment, uint64, error) {
	return newWithChunkMode(results, normCalc, defaultChunkMode)
}

// newWithChunkMode creates a segment with a specific chunk mode
func newWithChunkMode(results []Document, normCalc func(string, int) float32,
	chunkMode uint32) (Segment, uint64, error) {
	// Build the ZIP data and collect metadata
	zipData, offsets, fieldsMap, fieldsInv, fieldDocs, fieldFreqs, err :=
		buildZipWithMetadata(results, normCalc, chunkMode)
	if err != nil {
		return nil, 0, err
	}

	// Create segment directly from construction metadata
	seg, err := newSegmentFromConstruction(
		zipData,
		offsets,
		fieldsMap,
		fieldsInv,
		fieldDocs,
		fieldFreqs,
		uint64(len(results)),
		Version,
		chunkMode,
	)
	if err != nil {
		return nil, 0, err
	}

	return seg, uint64(len(zipData)), nil
}

var interimPool = sync.Pool{New: func() interface{} { return &interim{} }}

// interim holds temporary working data used while converting from
// the source operations to an encoded segment
type interim struct {
	results []Document

	chunkMode uint32

	w *countHashWriter

	// FieldsMap adds 1 to field id to avoid zero value issues
	//  name -> field id + 1
	FieldsMap map[string]uint16

	// FieldsInv is the inverse of FieldsMap
	//  field id -> name
	FieldsInv []string

	// FieldDocs tracks how many documents have at least one value
	// for each field
	FieldDocs map[uint16]uint64

	// FieldFreqs tracks how many total tokens there are in a field
	// across all documents
	FieldFreqs map[uint16]uint64

	// Term dictionaries for each field
	//  field id -> term -> postings list id + 1
	Dicts []map[string]uint64

	// Terms for each field, where terms are sorted ascending
	//  field id -> []term
	DictKeys [][]string

	// Fields whose IncludeDocValues is true
	//  field id -> bool
	IncludeDocValues []bool

	// postings id -> bitmap of docNums
	Postings []*roaring.Bitmap

	// postings id -> freq/norm's, one for each docNum in postings
	FreqNorms        [][]interimFreqNorm
	freqNormsBacking []interimFreqNorm

	// postings id -> locs, one for each freq
	Locs        [][]interimLoc
	locsBacking []interimLoc

	numTermsPerPostingsList []int // key is postings list id
	numLocsPerPostingsList  []int // key is postings list id

	builder    *vellum.Builder
	builderBuf bytes.Buffer

	metaBuf bytes.Buffer

	tmp0 []byte
	tmp1 []byte

	lastNumDocs int
	lastOutSize int

	normCalc func(string, int) float32
}

func (s *interim) reset() (err error) {
	s.results = nil
	s.chunkMode = 0
	s.w = nil
	s.FieldsMap = nil
	s.FieldsInv = nil
	for i := range s.Dicts {
		s.Dicts[i] = nil
	}
	s.Dicts = s.Dicts[:0]
	for i := range s.DictKeys {
		s.DictKeys[i] = s.DictKeys[i][:0]
	}
	s.DictKeys = s.DictKeys[:0]
	for i := range s.IncludeDocValues {
		s.IncludeDocValues[i] = false
	}
	s.IncludeDocValues = s.IncludeDocValues[:0]
	for _, idn := range s.Postings {
		idn.Clear()
	}
	s.Postings = s.Postings[:0]
	s.FreqNorms = s.FreqNorms[:0]
	for i := range s.freqNormsBacking {
		s.freqNormsBacking[i] = interimFreqNorm{}
	}
	s.freqNormsBacking = s.freqNormsBacking[:0]
	s.Locs = s.Locs[:0]
	for i := range s.locsBacking {
		s.locsBacking[i] = interimLoc{}
	}
	s.locsBacking = s.locsBacking[:0]
	s.numTermsPerPostingsList = s.numTermsPerPostingsList[:0]
	s.numLocsPerPostingsList = s.numLocsPerPostingsList[:0]
	s.builderBuf.Reset()
	if s.builder != nil {
		err = s.builder.Reset(&s.builderBuf)
	}
	s.metaBuf.Reset()
	s.tmp0 = s.tmp0[:0]
	s.tmp1 = s.tmp1[:0]
	s.lastNumDocs = 0
	s.lastOutSize = 0

	return err
}

func (s *interim) grabBuf(size int) []byte {
	buf := s.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		s.tmp0 = buf
	}
	return buf[0:size]
}

type interimStoredField struct {
	vals [][]byte
}

type interimFreqNorm struct {
	freq    uint64
	norm    float32
	numLocs int
}

type interimLoc struct {
	fieldID uint16
	pos     uint64
	start   uint64
	end     uint64
}

func (s *interim) getOrDefineField(fieldName string) int {
	fieldIDPlus1, exists := s.FieldsMap[fieldName]
	if !exists {
		fieldIDPlus1 = uint16(len(s.FieldsInv) + 1)
		s.FieldsMap[fieldName] = fieldIDPlus1
		s.FieldsInv = append(s.FieldsInv, fieldName)

		s.Dicts = append(s.Dicts, make(map[string]uint64))

		n := len(s.DictKeys)
		if n < cap(s.DictKeys) {
			s.DictKeys = s.DictKeys[:n+1]
			s.DictKeys[n] = s.DictKeys[n][:0]
		} else {
			s.DictKeys = append(s.DictKeys, []string(nil))
		}
	}

	return int(fieldIDPlus1 - 1)
}

// fill Dicts and DictKeys from analysis results
func (s *interim) prepareDicts() {
	var pidNext int

	var totTFs int
	var totLocs int

	for _, result := range s.results {
		pidNext, totLocs, totTFs = s.prepareDictsForDocument(result, pidNext, totLocs, totTFs)
	}

	numPostingsLists := pidNext

	if cap(s.Postings) >= numPostingsLists {
		s.Postings = s.Postings[:numPostingsLists]
	} else {
		postings := make([]*roaring.Bitmap, numPostingsLists)
		copy(postings, s.Postings[:cap(s.Postings)])
		for i := 0; i < numPostingsLists; i++ {
			if postings[i] == nil {
				postings[i] = roaring.New()
			}
		}
		s.Postings = postings
	}

	if cap(s.FreqNorms) >= numPostingsLists {
		s.FreqNorms = s.FreqNorms[:numPostingsLists]
	} else {
		s.FreqNorms = make([][]interimFreqNorm, numPostingsLists)
	}

	if cap(s.freqNormsBacking) >= totTFs {
		s.freqNormsBacking = s.freqNormsBacking[:totTFs]
	} else {
		s.freqNormsBacking = make([]interimFreqNorm, totTFs)
	}

	freqNormsBacking := s.freqNormsBacking
	for pid, numTerms := range s.numTermsPerPostingsList {
		s.FreqNorms[pid] = freqNormsBacking[0:0]
		freqNormsBacking = freqNormsBacking[numTerms:]
	}

	if cap(s.Locs) >= numPostingsLists {
		s.Locs = s.Locs[:numPostingsLists]
	} else {
		s.Locs = make([][]interimLoc, numPostingsLists)
	}

	if cap(s.locsBacking) >= totLocs {
		s.locsBacking = s.locsBacking[:totLocs]
	} else {
		s.locsBacking = make([]interimLoc, totLocs)
	}

	locsBacking := s.locsBacking
	for pid, numLocs := range s.numLocsPerPostingsList {
		s.Locs[pid] = locsBacking[0:0]
		locsBacking = locsBacking[numLocs:]
	}
}

func (s *interim) prepareDictsForDocument(result Document, pidNext, totLocs, totTFs int) (
	pidNextOut, totLocsOut, totTFsOut int) {
	fieldsSeen := map[uint16]struct{}{}
	result.EachField(func(field Field) {
		fieldID := uint16(s.getOrDefineField(field.Name()))

		fieldsSeen[fieldID] = struct{}{}
		s.FieldFreqs[fieldID] += uint64(field.Length())

		dict := s.Dicts[fieldID]
		dictKeys := s.DictKeys[fieldID]

		var numTerms int
		field.EachTerm(func(term FieldTerm) {
			numTerms++
			termStr := string(term.Term())
			pidPlus1, exists := dict[termStr]
			if !exists {
				pidNext++
				pidPlus1 = uint64(pidNext)

				dict[termStr] = pidPlus1
				dictKeys = append(dictKeys, termStr)

				s.numTermsPerPostingsList = append(s.numTermsPerPostingsList, 0)
				s.numLocsPerPostingsList = append(s.numLocsPerPostingsList, 0)
			}

			pid := pidPlus1 - 1

			s.numTermsPerPostingsList[pid]++

			var numLocations int
			term.EachLocation(func(_ Location) {
				numLocations++
			})
			s.numLocsPerPostingsList[pid] += numLocations

			totLocs += numLocations
		})

		totTFs += numTerms

		s.DictKeys[fieldID] = dictKeys
	})
	// record fields seen by this doc
	for k := range fieldsSeen {
		s.FieldDocs[k]++
	}
	return pidNext, totLocs, totTFs
}

func (s *interim) processDocuments() {
	numFields := len(s.FieldsInv)
	reuseFieldLens := make([]int, numFields)
	reuseFieldTFs := make([]tokenFrequencies, numFields)

	for docNum, result := range s.results {
		for i := 0; i < numFields; i++ { // clear these for reuse
			reuseFieldLens[i] = 0
			reuseFieldTFs[i] = nil
		}

		s.processDocument(uint64(docNum), result,
			reuseFieldLens, reuseFieldTFs)
	}
}

func (s *interim) processDocument(docNum uint64,
	result Document,
	fieldLens []int, fieldTFs []tokenFrequencies) {
	visitField := func(field Field) {
		fieldID := uint16(s.getOrDefineField(field.Name()))
		fieldLens[fieldID] += field.Length()

		if existingFreqs := fieldTFs[fieldID]; existingFreqs == nil {
			fieldTFs[fieldID] = make(map[string]*tokenFreq)
		}

		existingFreqs := fieldTFs[fieldID]
		field.EachTerm(func(term FieldTerm) {
			tfk := string(term.Term())
			existingTf, exists := existingFreqs[tfk]
			if exists {
				term.EachLocation(func(location Location) {
					existingTf.Locations = append(existingTf.Locations,
						&tokenLocation{
							FieldVal:    field.Name(),
							StartVal:    location.Start(),
							EndVal:      location.End(),
							PositionVal: location.Pos(),
						})
				})
				existingTf.frequency += term.Frequency()
			} else {
				newTf := &tokenFreq{
					TermVal:   term.Term(),
					frequency: term.Frequency(),
				}
				term.EachLocation(func(location Location) {
					newTf.Locations = append(newTf.Locations,
						&tokenLocation{
							FieldVal:    location.Field(),
							StartVal:    location.Start(),
							EndVal:      location.End(),
							PositionVal: location.Pos(),
						})
				})
				existingFreqs[tfk] = newTf
			}
		})
	}

	result.EachField(visitField)

	// now that it's been rolled up into fieldTFs, walk that
	for fieldID, tfs := range fieldTFs {
		dict := s.Dicts[fieldID]
		norm := s.normCalc(s.FieldsInv[fieldID], fieldLens[fieldID])

		for term, tf := range tfs {
			pid := dict[term] - 1
			bs := s.Postings[pid]
			bs.Add(uint32(docNum))

			s.FreqNorms[pid] = append(s.FreqNorms[pid],
				interimFreqNorm{
					freq:    uint64(tf.Frequency()),
					norm:    norm,
					numLocs: len(tf.Locations),
				})

			if len(tf.Locations) > 0 {
				locs := s.Locs[pid]

				for _, loc := range tf.Locations {
					var locf = uint16(fieldID)
					if loc.FieldVal != "" {
						locf = uint16(s.getOrDefineField(loc.FieldVal))
					}
					locs = append(locs, interimLoc{
						fieldID: locf,
						pos:     uint64(loc.PositionVal),
						start:   uint64(loc.StartVal),
						end:     uint64(loc.EndVal),
					})
				}

				s.Locs[pid] = locs
			}
		}
	}
}

// ============================================================================
// ZIP Format Functions
// ============================================================================

// NewZip creates a ZIP-based segment from documents
func NewZip(results []Document, normCalc func(string, int) float32) (
	[]byte, uint64, error) {
	return newZipWithChunkMode(results, normCalc, defaultChunkMode)
}

func newZipWithChunkMode(results []Document, normCalc func(string, int) float32,
	chunkMode uint32) ([]byte, uint64, error) {
	zipData, _, _, _, _, _, err := buildZipWithMetadata(results, normCalc, chunkMode)
	if err != nil {
		return nil, 0, err
	}
	return zipData, uint64(len(zipData)), nil
}

// buildZipWithMetadata builds a ZIP segment and returns all metadata needed
// to create a Segment without re-parsing the ZIP data.
func buildZipWithMetadata(results []Document, normCalc func(string, int) float32,
	chunkMode uint32) (
	zipData []byte,
	offsets *SegmentOffsets,
	fieldsMap map[string]uint16,
	fieldsInv []string,
	fieldDocs map[uint16]uint64,
	fieldFreqs map[uint16]uint64,
	err error,
) {
	s := interimPool.Get().(*interim)
	defer func() {
		if s.reset() == nil {
			interimPool.Put(s)
		}
	}()

	s.normCalc = normCalc
	s.results = results
	s.chunkMode = chunkMode

	// Use a dummy writer for offset calculations (we'll track per-entry)
	var dummyBuf bytes.Buffer
	s.w = newCountHashWriter(&dummyBuf)

	// Initialize field maps
	s.FieldsMap = map[string]uint16{}
	s.FieldDocs = map[uint16]uint64{}
	s.FieldFreqs = map[uint16]uint64{}

	// Define _id field first
	s.getOrDefineField(_idFieldName)

	for _, result := range results {
		result.EachField(func(field Field) {
			s.getOrDefineField(field.Name())
		})
	}

	sort.Strings(s.FieldsInv[1:]) // keep _id as first field

	for fieldID, fieldName := range s.FieldsInv {
		s.FieldsMap[fieldName] = uint16(fieldID + 1)
	}

	if cap(s.IncludeDocValues) >= len(s.FieldsInv) {
		s.IncludeDocValues = s.IncludeDocValues[:len(s.FieldsInv)]
	} else {
		s.IncludeDocValues = make([]bool, len(s.FieldsInv))
	}

	s.prepareDicts()

	for _, dict := range s.DictKeys {
		sort.Strings(dict)
	}

	s.processDocuments()

	// Create ZIP writer
	zw := newZipSegmentWriter()
	zw.SetFieldNames(s.FieldsInv)

	// Write stored fields
	err = s.writeStoredFieldsZip(zw)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	// Write dictionaries, postings, and doc values per field
	if len(results) > 0 {
		err = s.writeDictsZip(zw)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}
	}

	// Write field metadata entries
	err = s.writeFieldMetaZip(zw)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	// Build the ZIP file
	zipData, err = zw.WriteZip(uint64(len(results)), Version, chunkMode)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	// Get tracked offsets
	offsets = zw.GetOffsets()

	// Copy field metadata (make copies since interim will be reset)
	fieldsMap = make(map[string]uint16, len(s.FieldsMap))
	for k, v := range s.FieldsMap {
		fieldsMap[k] = v
	}

	fieldsInv = make([]string, len(s.FieldsInv))
	copy(fieldsInv, s.FieldsInv)

	fieldDocs = make(map[uint16]uint64, len(s.FieldDocs))
	for k, v := range s.FieldDocs {
		fieldDocs[k] = v
	}

	fieldFreqs = make(map[uint16]uint64, len(s.FieldFreqs))
	for k, v := range s.FieldFreqs {
		fieldFreqs[k] = v
	}

	return zipData, offsets, fieldsMap, fieldsInv, fieldDocs, fieldFreqs, nil
}

// writeStoredFieldsZip writes stored fields to ZIP entries
func (s *interim) writeStoredFieldsZip(zw *zipSegmentWriter) error {
	varBuf := make([]byte, binary.MaxVarintLen64)
	metaEncode := func(val uint64) (int, error) {
		wb := binary.PutUvarint(varBuf, val)
		return s.metaBuf.Write(varBuf[:wb])
	}

	data, compressed := s.tmp0[:0], s.tmp1[:0]
	defer func() { s.tmp0, s.tmp1 = data, compressed }()

	storedDataEntry := zw.StoredData()
	storedIndexEntry := zw.StoredIndex()

	// Track per-document offsets within the data entry
	docStoredOffsets := make([]uint64, len(s.results))

	// Keyed by fieldID, for the current doc in the loop
	docStoredFields := map[uint16]interimStoredField{}

	for docNum, result := range s.results {
		for fieldID := range docStoredFields {
			delete(docStoredFields, fieldID)
		}

		result.EachField(func(field Field) {
			fieldID := uint16(s.getOrDefineField(field.Name()))

			if field.Store() {
				isf := docStoredFields[fieldID]
				isf.vals = append(isf.vals, field.Value())
				docStoredFields[fieldID] = isf
			}

			if field.IndexDocValues() {
				s.IncludeDocValues[fieldID] = true
			}
		})

		var curr int
		s.metaBuf.Reset()
		data = data[:0]

		// Handle fields
		for fieldID := 0; fieldID < len(s.FieldsInv); fieldID++ {
			isf, exists := docStoredFields[uint16(fieldID)]
			if exists {
				var err error
				curr, data, err = encodeStoredFieldValues(
					fieldID, isf.vals,
					curr, metaEncode, data)
				if err != nil {
					return err
				}
			}
		}

		metaBytes := s.metaBuf.Bytes()
		compressed = snappy.Encode(compressed[:cap(compressed)], data)

		// Record offset within data entry
		docStoredOffsets[docNum] = uint64(storedDataEntry.Count())

		// Write meta length and compressed length
		err := writeUvarintsTo(storedDataEntry,
			uint64(len(metaBytes)),
			uint64(len(compressed)))
		if err != nil {
			return err
		}

		_, err = storedDataEntry.Write(metaBytes)
		if err != nil {
			return err
		}

		_, err = storedDataEntry.Write(compressed)
		if err != nil {
			return err
		}
	}

	// Write the offset index to the separate index entry
	for _, docStoredOffset := range docStoredOffsets {
		err := binary.Write(storedIndexEntry, binary.BigEndian, docStoredOffset)
		if err != nil {
			return err
		}
	}

	return nil
}

// writeDictsZip writes dictionaries and postings per field
func (s *interim) writeDictsZip(zw *zipSegmentWriter) error {
	buf := s.grabBuf(binary.MaxVarintLen64)

	// These int coders are initialized with chunk size 1024
	tfEncoder := newChunkedIntCoder(uint64(legacyChunkMode), uint64(len(s.results)-1))
	locEncoder := newChunkedIntCoder(uint64(legacyChunkMode), uint64(len(s.results)-1))

	var docTermMap [][]byte

	if s.builder == nil {
		var err error
		s.builder, err = vellum.New(&s.builderBuf, nil)
		if err != nil {
			return err
		}
	}

	for fieldID, terms := range s.DictKeys {
		fieldName := s.FieldsInv[fieldID]
		err := s.writeDictsFieldZip(zw, fieldName, docTermMap, fieldID, terms, tfEncoder, locEncoder, buf)
		if err != nil {
			return err
		}
	}

	return nil
}

// writeDictsFieldZip writes a single field's dictionary, postings, and doc values
func (s *interim) writeDictsFieldZip(zw *zipSegmentWriter, fieldName string, docTermMap [][]byte, fieldID int, terms []string,
	tfEncoder, locEncoder *chunkedIntCoder, buf []byte) error {

	if cap(docTermMap) < len(s.results) {
		docTermMap = make([][]byte, len(s.results))
	} else {
		docTermMap = docTermMap[0:len(s.results)]
		for docNum := range docTermMap {
			docTermMap[docNum] = docTermMap[docNum][:0]
		}
	}

	dict := s.Dicts[fieldID]
	postingsEntry := zw.FieldPostings(fieldName)

	// Write a padding byte to ensure TF data never starts at offset 0
	// This avoids collision with termNotEncoded=0 sentinel value
	postingsEntry.Write([]byte{0})

	for _, term := range terms {
		err := s.writeDictsTermFieldZip(postingsEntry, docTermMap, dict, term, tfEncoder, locEncoder, buf)
		if err != nil {
			return err
		}
	}

	err := s.builder.Close()
	if err != nil {
		return err
	}

	// Write dictionary to its entry
	dictEntry := zw.FieldDictionary(fieldName)
	vellumData := s.builderBuf.Bytes()

	// Write out the length of the vellum data
	n := binary.PutUvarint(buf, uint64(len(vellumData)))
	_, err = dictEntry.Write(buf[:n])
	if err != nil {
		return err
	}

	// Write the vellum data
	_, err = dictEntry.Write(vellumData)
	if err != nil {
		return err
	}

	// Reset vellum for reuse
	s.builderBuf.Reset()
	err = s.builder.Reset(&s.builderBuf)
	if err != nil {
		return err
	}

	// Write doc values if this field has them
	if s.IncludeDocValues[fieldID] {
		chunkSize, err := getChunkSize(legacyChunkMode, 0, 0)
		if err != nil {
			return err
		}

		dvEntry := zw.FieldDocValues(fieldName)
		fdvEncoder := newChunkedContentCoder(chunkSize, uint64(len(s.results)-1), dvEntry, false)

		for docNum, docTerms := range docTermMap {
			if len(docTerms) > 0 {
				err = fdvEncoder.Add(uint64(docNum), docTerms)
				if err != nil {
					return err
				}
			}
		}

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

// writeDictsTermFieldZip writes postings for a single term
func (s *interim) writeDictsTermFieldZip(postingsEntry *zipEntry, docTermMap [][]byte, dict map[string]uint64, term string,
	tfEncoder, locEncoder *chunkedIntCoder, buf []byte) error {

	pid := dict[term] - 1
	postingsBS := s.Postings[pid]

	freqNorms := s.FreqNorms[pid]
	freqNormOffset := 0

	locs := s.Locs[pid]
	locOffset := 0

	chunkSize, err := getChunkSize(s.chunkMode, postingsBS.GetCardinality(), uint64(len(s.results)))
	if err != nil {
		return err
	}
	tfEncoder.SetChunkSize(chunkSize, uint64(len(s.results)-1))
	locEncoder.SetChunkSize(chunkSize, uint64(len(s.results)-1))

	postingsItr := postingsBS.Iterator()
	for postingsItr.HasNext() {
		docNum := uint64(postingsItr.Next())

		freqNorm := freqNorms[freqNormOffset]

		err = tfEncoder.Add(docNum,
			encodeFreqHasLocs(freqNorm.freq, freqNorm.numLocs > 0),
			uint64(math.Float32bits(freqNorm.norm)))
		if err != nil {
			return err
		}

		if freqNorm.numLocs > 0 {
			numBytesLocs := 0
			for _, loc := range locs[locOffset : locOffset+freqNorm.numLocs] {
				numBytesLocs += totalUvarintBytes(
					uint64(loc.fieldID), loc.pos, loc.start, loc.end)
			}

			err = locEncoder.Add(docNum, uint64(numBytesLocs))
			if err != nil {
				return err
			}

			for _, loc := range locs[locOffset : locOffset+freqNorm.numLocs] {
				err = locEncoder.Add(docNum,
					uint64(loc.fieldID), loc.pos, loc.start, loc.end)
				if err != nil {
					return err
				}
			}

			locOffset += freqNorm.numLocs
		}

		freqNormOffset++

		docTermMap[docNum] = append(
			append(docTermMap[docNum], term...),
			termSeparator)
	}

	tfEncoder.Close()
	locEncoder.Close()

	var postingsOffset uint64
	postingsOffset, err = writePostingsZip(postingsBS, tfEncoder, locEncoder, postingsEntry, buf)
	if err != nil {
		return err
	}

	if postingsOffset > uint64(0) {
		err = s.builder.Insert([]byte(term), postingsOffset)
		if err != nil {
			return err
		}
	}

	tfEncoder.Reset()
	locEncoder.Reset()
	return nil
}

// writePostingsZip writes postings to the field's postings entry
func writePostingsZip(postings *roaring.Bitmap, tfEncoder, locEncoder *chunkedIntCoder,
	w *zipEntry, bufMaxVarintLen64 []byte) (uint64, error) {

	termCardinality := postings.GetCardinality()
	if termCardinality <= 0 {
		return 0, nil
	}

	// Note: 1-hit encoding is not used for new segments (same as original code)

	var tfOffset uint64
	tfOffset, err := tfEncoder.writeAtEntry(w)
	if err != nil {
		return 0, err
	}

	var locOffset uint64
	locOffset, err = locEncoder.writeAtEntry(w)
	if err != nil {
		return 0, err
	}

	postingsOffset := uint64(w.Count())

	n := binary.PutUvarint(bufMaxVarintLen64, tfOffset)
	_, err = w.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return 0, err
	}

	n = binary.PutUvarint(bufMaxVarintLen64, locOffset)
	_, err = w.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return 0, err
	}

	_, err = writeRoaringWithLenEntry(postings, w, bufMaxVarintLen64)
	if err != nil {
		return 0, err
	}

	return postingsOffset, nil
}

// writeRoaringWithLenEntry writes a roaring bitmap to a ZIP entry
func writeRoaringWithLenEntry(r *roaring.Bitmap, w *zipEntry, reuseBufVarint []byte) (int, error) {
	buf, err := r.ToBytes()
	if err != nil {
		return 0, err
	}

	var tw int

	n := binary.PutUvarint(reuseBufVarint, uint64(len(buf)))
	nw, err := w.Write(reuseBufVarint[:n])
	tw += nw
	if err != nil {
		return tw, err
	}

	nw, err = w.Write(buf)
	tw += nw
	if err != nil {
		return tw, err
	}

	return tw, nil
}

// writeFieldMetaZip writes per-field metadata entries
func (s *interim) writeFieldMetaZip(zw *zipSegmentWriter) error {
	buf := make([]byte, binary.MaxVarintLen64)

	for fieldID, fieldName := range s.FieldsInv {
		metaEntry := zw.FieldMeta(fieldName)

		// Write doc count
		n := binary.PutUvarint(buf, s.FieldDocs[uint16(fieldID)])
		_, err := metaEntry.Write(buf[:n])
		if err != nil {
			return err
		}

		// Write freq count
		n = binary.PutUvarint(buf, s.FieldFreqs[uint16(fieldID)])
		_, err = metaEntry.Write(buf[:n])
		if err != nil {
			return err
		}
	}

	return nil
}

// writeUvarintsTo writes multiple varints to a writer
func writeUvarintsTo(w *zipEntry, vals ...uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	for _, val := range vals {
		n := binary.PutUvarint(buf, val)
		_, err := w.Write(buf[:n])
		if err != nil {
			return err
		}
	}
	return nil
}
