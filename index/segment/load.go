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
	"math"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/golang/snappy"
)

// ZIP format constants (little-endian)
const (
	zipEOCDSignature   = 0x06054b50
	zipCDirSignature   = 0x02014b50
	zipEOCDSize        = 22 // minimum EOCD size without comment
	zipCDirHeaderSize  = 46 // central directory file header size
	zipLocalHeaderSize = 30 // local file header size
)

// footer represents the parsed _footer entry from a segment
type footer struct {
	numDocs   uint64
	version   uint32
	chunkMode uint32
}

// fieldOffsets holds the byte offsets for a field's ZIP entries
type fieldOffsets struct {
	metaOffset uint64
	metaSize   uint64
	dictOffset uint64
	dictSize   uint64
	postOffset uint64
	postSize   uint64
	dvOffset   uint64
	dvSize     uint64
}

// Segment is a segment backed by a ZIP file format
type segment struct {
	data *Data // backing store (supports mmap and heap allocation)

	footer *footer

	// Entry offsets for non-field entries
	storedDataOffset  uint64
	storedDataSize    uint64
	storedIndexOffset uint64
	storedIndexSize   uint64

	// Field data
	fieldsMap    map[string]uint16 // fieldName -> fieldID+1
	fieldsInv    []string          // fieldID -> fieldName
	fieldDocs    map[uint16]uint64 // fieldID -> # docs with value in field
	fieldFreqs   map[uint16]uint64 // fieldID -> # total tokens in field
	fieldOffsets []fieldOffsets    // fieldID -> entry offsets

	fieldDvReaders map[uint16]*docValueReader
	fieldDvNames   []string

	fieldFSTs map[uint16]*vellum.FST
	fstMu     sync.Mutex

	size uint64
}

// Compile-time check that Segment implements Segment
var _ Segment = (*segment)(nil)

// Load loads a segment from Data (supports mmap and heap allocation)
func Load(data *Data) (*segment, error) {
	zs := &segment{
		data:           data,
		fieldsMap:      make(map[string]uint16),
		fieldDocs:      make(map[uint16]uint64),
		fieldFreqs:     make(map[uint16]uint64),
		fieldDvReaders: make(map[uint16]*docValueReader),
		fieldFSTs:      make(map[uint16]*vellum.FST),
	}

	// Read full data for ZIP parsing (zero-copy for mmap)
	zipBytes, err := data.Read(0, data.Size())
	if err != nil {
		return nil, fmt.Errorf("error reading segment data: %w", err)
	}

	// Parse ZIP central directory directly into segment structures
	if err := zs.parseZipDirectoryInto(zipBytes); err != nil {
		return nil, fmt.Errorf("error parsing ZIP directory: %w", err)
	}

	// Load doc value readers
	if err := zs.loadDvReadersFromZip(); err != nil {
		return nil, fmt.Errorf("error loading doc value readers: %w", err)
	}

	zs.updateSize()

	return zs, nil
}

// LoadBytes is a convenience function that loads a segment from a byte slice.
// It wraps the bytes in a Data and calls Load.
func LoadBytes(data []byte) (*segment, error) {
	return Load(NewDataBytes(data))
}

// Open loads a segment from a byte slice and returns it as the Segment interface.
// This is the primary function for external packages to load segments.
func Open(data []byte) (Segment, error) {
	return LoadBytes(data)
}

// newSegmentFromConstruction creates a Segment directly from construction metadata,
// avoiding the need to re-parse the ZIP data. This is used by New() for efficiency.
func newSegmentFromConstruction(
	data []byte,
	offsets *SegmentOffsets,
	fieldsMap map[string]uint16,
	fieldsInv []string,
	fieldDocs map[uint16]uint64,
	fieldFreqs map[uint16]uint64,
	numDocs uint64,
	version uint32,
	chunkMode uint32,
) (*segment, error) {
	// Convert SegmentOffsets to fieldOffsets
	fieldOffs := make([]fieldOffsets, len(offsets.FieldOffsets))
	for i, fo := range offsets.FieldOffsets {
		fieldOffs[i] = fieldOffsets{
			metaOffset: fo.MetaOffset,
			metaSize:   fo.MetaSize,
			dictOffset: fo.DictOffset,
			dictSize:   fo.DictSize,
			postOffset: fo.PostOffset,
			postSize:   fo.PostSize,
			dvOffset:   fo.DvOffset,
			dvSize:     fo.DvSize,
		}
	}

	zs := &segment{
		data: NewDataBytes(data),
		footer: &footer{
			numDocs:   numDocs,
			version:   version,
			chunkMode: chunkMode,
		},
		storedDataOffset:  offsets.StoredDataOffset,
		storedDataSize:    offsets.StoredDataSize,
		storedIndexOffset: offsets.StoredIndexOffset,
		storedIndexSize:   offsets.StoredIndexSize,
		fieldsMap:         fieldsMap,
		fieldsInv:         fieldsInv,
		fieldDocs:         fieldDocs,
		fieldFreqs:        fieldFreqs,
		fieldOffsets:      fieldOffs,
		fieldDvReaders:    make(map[uint16]*docValueReader),
		fieldFSTs:         make(map[uint16]*vellum.FST),
	}

	// Load doc value readers
	if err := zs.loadDvReadersFromZip(); err != nil {
		return nil, fmt.Errorf("error loading doc value readers: %w", err)
	}

	zs.updateSize()

	return zs, nil
}

// parseZipDirectoryInto parses the ZIP central directory directly into the segment structures
func (zs *segment) parseZipDirectoryInto(data []byte) error {
	dataLen := len(data)
	if dataLen < zipEOCDSize {
		return fmt.Errorf("data too small for ZIP: %d bytes", dataLen)
	}

	// Find End of Central Directory record
	eocdPos := -1
	for i := dataLen - zipEOCDSize; i >= 0 && i >= dataLen-65557; i-- {
		if binary.LittleEndian.Uint32(data[i:]) == zipEOCDSignature {
			eocdPos = i
			break
		}
	}
	if eocdPos < 0 {
		return fmt.Errorf("ZIP end of central directory not found")
	}

	totalEntries := int(binary.LittleEndian.Uint16(data[eocdPos+10:]))
	cdirOffset := int(binary.LittleEndian.Uint32(data[eocdPos+16:]))

	if cdirOffset < 0 || cdirOffset >= dataLen {
		return fmt.Errorf("invalid central directory offset: %d", cdirOffset)
	}

	// Byte slices for suffix matching (avoid string conversions)
	metaSuffixBytes := []byte(fieldMetaSuffix)
	dictSuffixBytes := []byte(fieldDictionarySuffix)
	postSuffixBytes := []byte(fieldPostingsSuffix)
	dvSuffixBytes := []byte(fieldDocValuesSuffix)
	footerBytes := []byte(footerEntry)
	storedDataBytes := []byte(storedDataEntry)
	storedIndexBytes := []byte(storedIndexEntry)
	idFieldBytes := []byte(_idFieldName)

	// First pass: count fields and find footer
	var footerOffset, footerSize uint64
	fieldCount := 0
	hasID := false

	pos := cdirOffset
	for i := 0; i < totalEntries; i++ {
		if pos+zipCDirHeaderSize > dataLen {
			return fmt.Errorf("truncated central directory at entry %d", i)
		}

		uncompressedSize := binary.LittleEndian.Uint32(data[pos+24:])
		fileNameLen := int(binary.LittleEndian.Uint16(data[pos+28:]))
		extraFieldLen := int(binary.LittleEndian.Uint16(data[pos+30:]))
		fileCommentLen := int(binary.LittleEndian.Uint16(data[pos+32:]))
		localHeaderOffset := int(binary.LittleEndian.Uint32(data[pos+42:]))

		nameStart := pos + zipCDirHeaderSize
		nameBytes := data[nameStart : nameStart+fileNameLen]

		// Check for footer
		if bytes.Equal(nameBytes, footerBytes) {
			localFileNameLen := int(binary.LittleEndian.Uint16(data[localHeaderOffset+26:]))
			localExtraLen := int(binary.LittleEndian.Uint16(data[localHeaderOffset+28:]))
			footerOffset = uint64(localHeaderOffset + zipLocalHeaderSize + localFileNameLen + localExtraLen)
			footerSize = uint64(uncompressedSize)
		}

		// Count fields and check for _id
		if bytes.HasSuffix(nameBytes, metaSuffixBytes) {
			fieldCount++
			fieldNameBytes := nameBytes[:len(nameBytes)-len(metaSuffixBytes)]
			if bytes.Equal(fieldNameBytes, idFieldBytes) {
				hasID = true
			}
		}

		pos += zipCDirHeaderSize + fileNameLen + extraFieldLen + fileCommentLen
	}

	// Parse footer
	if footerSize == 0 {
		return fmt.Errorf("_footer entry not found")
	}
	var err error
	zs.footer, err = parseZipFooter(data[footerOffset : footerOffset+footerSize])
	if err != nil {
		return fmt.Errorf("error parsing footer: %w", err)
	}

	// Pre-allocate field structures
	zs.fieldsInv = make([]string, 0, fieldCount)
	zs.fieldOffsets = make([]fieldOffsets, fieldCount)

	// Add _id first if present
	if hasID {
		zs.fieldsInv = append(zs.fieldsInv, _idFieldName)
		zs.fieldsMap[_idFieldName] = 1
	}

	// Second pass: add all fields to fieldsMap from /meta entries
	pos = cdirOffset
	for i := 0; i < totalEntries; i++ {
		fileNameLen := int(binary.LittleEndian.Uint16(data[pos+28:]))
		extraFieldLen := int(binary.LittleEndian.Uint16(data[pos+30:]))
		fileCommentLen := int(binary.LittleEndian.Uint16(data[pos+32:]))

		nameStart := pos + zipCDirHeaderSize
		nameBytes := data[nameStart : nameStart+fileNameLen]

		// Add non-_id fields to fieldsInv (only on /meta entries)
		if bytes.HasSuffix(nameBytes, metaSuffixBytes) {
			fieldNameBytes := nameBytes[:len(nameBytes)-len(metaSuffixBytes)]
			if !bytes.Equal(fieldNameBytes, idFieldBytes) {
				fieldName := string(fieldNameBytes)
				zs.fieldsInv = append(zs.fieldsInv, fieldName)
				zs.fieldsMap[fieldName] = uint16(len(zs.fieldsInv))
			}
		}

		pos += zipCDirHeaderSize + fileNameLen + extraFieldLen + fileCommentLen
	}

	// Third pass: store all offsets now that fieldsMap is complete
	pos = cdirOffset
	for i := 0; i < totalEntries; i++ {
		uncompressedSize := binary.LittleEndian.Uint32(data[pos+24:])
		fileNameLen := int(binary.LittleEndian.Uint16(data[pos+28:]))
		extraFieldLen := int(binary.LittleEndian.Uint16(data[pos+30:]))
		fileCommentLen := int(binary.LittleEndian.Uint16(data[pos+32:]))
		localHeaderOffset := int(binary.LittleEndian.Uint32(data[pos+42:]))

		nameStart := pos + zipCDirHeaderSize
		nameBytes := data[nameStart : nameStart+fileNameLen]

		// Calculate data offset
		localFileNameLen := int(binary.LittleEndian.Uint16(data[localHeaderOffset+26:]))
		localExtraLen := int(binary.LittleEndian.Uint16(data[localHeaderOffset+28:]))
		dataOffset := uint64(localHeaderOffset + zipLocalHeaderSize + localFileNameLen + localExtraLen)
		dataSize := uint64(uncompressedSize)

		pos += zipCDirHeaderSize + fileNameLen + extraFieldLen + fileCommentLen

		// Store offsets based on entry type
		switch {
		case bytes.Equal(nameBytes, storedDataBytes):
			zs.storedDataOffset, zs.storedDataSize = dataOffset, dataSize
		case bytes.Equal(nameBytes, storedIndexBytes):
			zs.storedIndexOffset, zs.storedIndexSize = dataOffset, dataSize
		case bytes.HasSuffix(nameBytes, metaSuffixBytes):
			fieldNameBytes := nameBytes[:len(nameBytes)-len(metaSuffixBytes)]
			if fieldID, ok := zs.fieldsMap[string(fieldNameBytes)]; ok {
				zs.fieldOffsets[fieldID-1].metaOffset = dataOffset
				zs.fieldOffsets[fieldID-1].metaSize = dataSize
			}
		case bytes.HasSuffix(nameBytes, dictSuffixBytes):
			fieldNameBytes := nameBytes[:len(nameBytes)-len(dictSuffixBytes)]
			if fieldID, ok := zs.fieldsMap[string(fieldNameBytes)]; ok {
				zs.fieldOffsets[fieldID-1].dictOffset = dataOffset
				zs.fieldOffsets[fieldID-1].dictSize = dataSize
			}
		case bytes.HasSuffix(nameBytes, postSuffixBytes):
			fieldNameBytes := nameBytes[:len(nameBytes)-len(postSuffixBytes)]
			if fieldID, ok := zs.fieldsMap[string(fieldNameBytes)]; ok {
				zs.fieldOffsets[fieldID-1].postOffset = dataOffset
				zs.fieldOffsets[fieldID-1].postSize = dataSize
			}
		case bytes.HasSuffix(nameBytes, dvSuffixBytes):
			fieldNameBytes := nameBytes[:len(nameBytes)-len(dvSuffixBytes)]
			if fieldID, ok := zs.fieldsMap[string(fieldNameBytes)]; ok {
				zs.fieldOffsets[fieldID-1].dvOffset = dataOffset
				zs.fieldOffsets[fieldID-1].dvSize = dataSize
			}
		}
	}

	// Load field metadata (docCount, freqCount)
	for fieldID, fieldName := range zs.fieldsInv {
		off := zs.fieldOffsets[fieldID]
		if off.metaSize == 0 {
			continue
		}
		metaData := data[off.metaOffset : off.metaOffset+off.metaSize]
		docCount, n := binary.Uvarint(metaData)
		if n <= 0 {
			return fmt.Errorf("error reading docCount for field %s", fieldName)
		}
		freqCount, _ := binary.Uvarint(metaData[n:])
		zs.fieldDocs[uint16(fieldID)] = docCount
		zs.fieldFreqs[uint16(fieldID)] = freqCount
	}

	return nil
}

// parseZipFooter parses the _footer entry content
func parseZipFooter(data []byte) (*footer, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("footer too short: %d bytes", len(data))
	}

	footer := &footer{}
	footer.numDocs = binary.BigEndian.Uint64(data[0:8])
	footer.version = binary.BigEndian.Uint32(data[8:12])
	if footer.version != Version {
		return nil, fmt.Errorf("unsupported version %d", footer.version)
	}
	footer.chunkMode = binary.BigEndian.Uint32(data[12:16])

	return footer, nil
}

// loadDvReadersFromZip loads doc value readers for fields that have them
func (zs *segment) loadDvReadersFromZip() error {
	for fieldID, fieldName := range zs.fieldsInv {
		off := zs.fieldOffsets[fieldID]
		if off.dvSize == 0 {
			continue
		}

		dvData, err := zs.data.Read(int(off.dvOffset), int(off.dvOffset+off.dvSize))
		if err != nil {
			return fmt.Errorf("error reading docvalue data for %s: %w", fieldName, err)
		}

		if len(dvData) < 16 {
			continue
		}

		dvReader, err := loadZipFieldDocValueReader(fieldName, dvData)
		if err != nil {
			return fmt.Errorf("error loading docvalue reader for %s: %w", fieldName, err)
		}

		if dvReader != nil {
			zs.fieldDvReaders[uint16(fieldID)] = dvReader
			zs.fieldDvNames = append(zs.fieldDvNames, fieldName)
		}
	}
	return nil
}

// loadZipFieldDocValueReader creates a docValueReader from ZIP entry data
// The data format is:
// | ..... chunk data ..... | chunk offsets (varints) | chunk offsets len (uint64) | num chunks (uint64) |
func loadZipFieldDocValueReader(field string, data []byte) (*docValueReader, error) {
	dataLen := uint64(len(data))

	// Read number of chunks (last 8 bytes)
	numChunks := binary.BigEndian.Uint64(data[dataLen-8:])

	// Read chunk offsets length (8 bytes before numChunks)
	chunkOffsetsLen := binary.BigEndian.Uint64(data[dataLen-16 : dataLen-8])

	// Calculate position of chunk offsets
	chunkOffsetsPos := dataLen - 16 - chunkOffsetsLen

	// Read chunk offsets
	chunkOffsets := make([]uint64, numChunks)
	pos := chunkOffsetsPos
	for i := uint64(0); i < numChunks; i++ {
		offset, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("corrupted chunk offset at position %d", i)
		}
		chunkOffsets[i] = offset
		pos += uint64(n)
	}

	return &docValueReader{
		field:        field,
		curChunkNum:  math.MaxInt64,
		chunkOffsets: chunkOffsets,
		dvDataLoc:    0, // data starts at beginning for ZIP entries
	}, nil
}

func (zs *segment) updateSize() {
	sizeInBytes := zs.data.Size()

	for k := range zs.fieldsMap {
		sizeInBytes += len(k) + sizeOfString + sizeOfUint16
	}

	for _, entry := range zs.fieldsInv {
		sizeInBytes += len(entry) + sizeOfString
	}

	for _, v := range zs.fieldDvReaders {
		sizeInBytes += sizeOfUint16 + sizeOfPtr
		if v != nil {
			sizeInBytes += v.size()
		}
	}

	zs.size = uint64(sizeInBytes)
}

// Implement Segment interface

func (zs *segment) Type() string {
	return Type
}

func (zs *segment) Version() uint32 {
	return zs.footer.version
}

func (zs *segment) Size() int {
	return int(zs.size)
}

func (zs *segment) Count() uint64 {
	return zs.footer.numDocs
}

func (zs *segment) Fields() []string {
	return zs.fieldsInv
}

func (zs *segment) CollectionStats(field string) (CollectionStats, error) {
	rv := &collectionStats{}
	fieldIDPlus1 := zs.fieldsMap[field]
	if fieldIDPlus1 > 0 {
		rv.totalDocCount = zs.footer.numDocs
		rv.docCount = zs.fieldDocs[fieldIDPlus1-1]
		rv.sumTotalTermFreq = zs.fieldFreqs[fieldIDPlus1-1]
	}
	return rv, nil
}

func (zs *segment) Dictionary(field string) (Dictionary, error) {
	dict, err := zs.dictionary(field)
	if err == nil && dict == nil {
		return emptyDictionary, nil
	}
	return dict, err
}

func (zs *segment) dictionary(field string) (*dictionary, error) {
	fieldIDPlus1 := zs.fieldsMap[field]
	if fieldIDPlus1 == 0 {
		return nil, nil
	}

	fieldID := fieldIDPlus1 - 1
	rv := &dictionary{
		sb:      zs,
		field:   field,
		fieldID: fieldID,
	}

	// Load FST if we have a dictionary entry
	off := zs.fieldOffsets[fieldID]
	if off.dictSize == 0 {
		return rv, nil
	}

	zs.fstMu.Lock()
	defer zs.fstMu.Unlock()

	if fst, ok := zs.fieldFSTs[fieldID]; ok {
		rv.fst = fst
	} else {
		dictData, err := zs.data.Read(int(off.dictOffset), int(off.dictOffset+off.dictSize))
		if err != nil {
			return nil, fmt.Errorf("error reading dictionary data for %s: %w", field, err)
		}

		if len(dictData) > 0 {
			// Parse vellum length and data
			vellumLen, n := binary.Uvarint(dictData)
			if n <= 0 {
				return nil, fmt.Errorf("error reading vellum length for %s", field)
			}

			fstBytes := dictData[n : n+int(vellumLen)]
			rv.fst, err = vellum.Load(fstBytes)
			if err != nil {
				return nil, fmt.Errorf("error loading vellum for %s: %w", field, err)
			}

			zs.fieldFSTs[fieldID] = rv.fst
		}
	}

	if rv.fst != nil {
		var err error
		rv.fstReader, err = rv.fst.Reader()
		if err != nil {
			return nil, fmt.Errorf("error creating vellum reader for %s: %w", field, err)
		}
	}

	return rv, nil
}

// zipVisitCtx holds reusable data structures for VisitStoredFields
type zipVisitCtx struct {
	buf    []byte
	reader bytes.Reader
}

var zipVisitCtxPool = sync.Pool{
	New: func() interface{} {
		return &zipVisitCtx{}
	},
}

func (zs *segment) VisitStoredFields(num uint64, visitor StoredFieldVisitor) error {
	if num >= zs.footer.numDocs {
		return nil
	}

	vdc := zipVisitCtxPool.Get().(*zipVisitCtx)
	defer zipVisitCtxPool.Put(vdc)

	// Read stored data and index via Data abstraction
	storedData, err := zs.data.Read(int(zs.storedDataOffset), int(zs.storedDataOffset+zs.storedDataSize))
	if err != nil {
		return fmt.Errorf("error reading stored data: %w", err)
	}
	storedIndex, err := zs.data.Read(int(zs.storedIndexOffset), int(zs.storedIndexOffset+zs.storedIndexSize))
	if err != nil {
		return fmt.Errorf("error reading stored index: %w", err)
	}

	// Get offset for this document
	indexOffset := num * fileAddrWidth
	if indexOffset+fileAddrWidth > uint64(len(storedIndex)) {
		return fmt.Errorf("stored index out of bounds for doc %d", num)
	}

	docOffset := binary.BigEndian.Uint64(storedIndex[indexOffset : indexOffset+fileAddrWidth])

	// Read meta length and compressed length
	metaLen, n := binary.Uvarint(storedData[docOffset:])
	if n <= 0 {
		return fmt.Errorf("error reading meta length for doc %d", num)
	}
	pos := docOffset + uint64(n)

	compressedLen, n := binary.Uvarint(storedData[pos:])
	if n <= 0 {
		return fmt.Errorf("error reading compressed length for doc %d", num)
	}
	pos += uint64(n)

	metaBytes := storedData[pos : pos+metaLen]
	pos += metaLen

	compressed := storedData[pos : pos+compressedLen]

	// Decompress using reusable buffer
	uncompressed, err := snappy.Decode(vdc.buf[:cap(vdc.buf)], compressed)
	if err != nil {
		return fmt.Errorf("error decompressing stored fields for doc %d: %w", num, err)
	}
	vdc.buf = uncompressed // save potentially grown buffer for reuse

	// Parse metadata using reusable reader
	vdc.reader.Reset(metaBytes)
	for {
		field, err := binary.ReadUvarint(&vdc.reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading field ID: %w", err)
		}

		offset, err := binary.ReadUvarint(&vdc.reader)
		if err != nil {
			return fmt.Errorf("error reading field offset: %w", err)
		}

		length, err := binary.ReadUvarint(&vdc.reader)
		if err != nil {
			return fmt.Errorf("error reading field length: %w", err)
		}

		value := uncompressed[offset : offset+length]
		if !visitor(zs.fieldsInv[field], value) {
			break
		}
	}

	return nil
}

func (zs *segment) DocsMatchingTerms(terms []Term) (*roaring.Bitmap, error) {
	rv := roaring.New()

	if len(zs.fieldsMap) == 0 {
		return rv, nil
	}

	var lastField string
	var dict *dictionary
	for _, term := range terms {
		thisField := term.Field()
		if thisField != lastField {
			var err error
			dict, err = zs.dictionary(thisField)
			if err != nil {
				return nil, err
			}
			lastField = thisField
		}
		if dict == nil {
			continue
		}

		postingsList := emptyPostingsList
		postingsList, err := dict.postingsList(term.Term(), nil, postingsList)
		if err != nil {
			return nil, err
		}
		postingsList.OrInto(rv)
	}

	return rv, nil
}

func (zs *segment) WriteTo(w io.Writer, _ chan struct{}) (int64, error) {
	return zs.data.WriteTo(w)
}

// CRC returns the CRC - for ZIP segments this is 0 (per-entry CRCs are used)
func (zs *segment) CRC() uint32 {
	return 0
}

func (zs *segment) ChunkMode() uint32 {
	return zs.footer.chunkMode
}

// NumDocs returns the number of documents
func (zs *segment) NumDocs() uint64 {
	return zs.footer.numDocs
}

// Dictionary represents a term dictionary for a field in a segment
type dictionary struct {
	sb        *segment
	field     string
	fieldID   uint16
	fst       *vellum.FST
	fstReader *vellum.Reader
}

// represents an immutable, empty dictionary
var emptyDictionary = &dictionary{}

// represents an immutable, empty dictionary iterator
var emptyDictionaryIterator = &dictionaryIterator{}

func (d *dictionary) PostingsList(term []byte, except *roaring.Bitmap, prealloc PostingsList) (PostingsList, error) {
	return d.postingsList(term, except, prealloc)
}

func (d *dictionary) postingsList(term []byte, except *roaring.Bitmap, prealloc PostingsList) (*postingsList, error) {
	var preallocPL *postingsList
	if prealloc != nil {
		preallocPL, _ = prealloc.(*postingsList)
	}
	// Create a new PostingsList if prealloc is nil or the global emptyPostingsList
	// to avoid corrupting the shared empty instance
	if preallocPL == nil || preallocPL == emptyPostingsList {
		preallocPL = &postingsList{}
	}
	rv := preallocPL
	rv.sb = nil
	rv.postingsOffset = 0
	rv.freqOffset = 0
	rv.locOffset = 0
	rv.postings = nil
	rv.except = except
	rv.docNum1Hit = 0
	rv.normBits1Hit = 0
	rv.chunkSize = 0

	if d.fst == nil || d.fstReader == nil {
		return rv, nil
	}

	postingsOffset, exists, err := d.fstReader.Get(term)
	if err != nil {
		return nil, fmt.Errorf("vellum get err: %v", err)
	}
	if !exists {
		return rv, nil
	}

	return d.postingsListFromOffset(postingsOffset, except, rv)
}

func (d *dictionary) postingsListFromOffset(postingsOffset uint64, except *roaring.Bitmap, rv *postingsList) (*postingsList, error) {
	// Initialize rv if nil (similar to flat segment's postingsListInit)
	if rv == nil || rv == emptyPostingsList {
		rv = &postingsList{}
	} else {
		postings := rv.postings
		if postings != nil {
			postings.Clear()
		}
		*rv = postingsList{} // clear the struct
		rv.postings = postings
	}
	rv.except = except

	// Check for 1-hit encoding
	if postingsOffset&fSTValEncodingMask == fSTValEncoding1Hit {
		docNum, normBits := fSTValDecode1Hit(postingsOffset)
		rv.docNum1Hit = docNum
		rv.normBits1Hit = normBits
		rv.postings = nil
		return rv, nil
	}

	// Get the base offset for this field's postings entry
	off := d.sb.fieldOffsets[d.fieldID]
	if off.postSize == 0 {
		return rv, nil
	}
	baseOffset := off.postOffset

	// Use segment directly for PostingsList iteration
	rv.sb = d.sb

	// Parse the postings at the given offset (relative to field's postings entry)
	// Convert to absolute offset
	absPostingsOffset := baseOffset + postingsOffset
	rv.postingsOffset = absPostingsOffset

	// Read the postings data from absPostingsOffset to end of postings entry
	postingsEndOffset := off.postOffset + off.postSize
	postingsData, err := d.sb.data.Read(int(absPostingsOffset), int(postingsEndOffset))
	if err != nil {
		return nil, fmt.Errorf("error reading postings data: %w", err)
	}

	// Read TF offset (stored as relative, convert to absolute if not sentinel)
	tfOffset, n := binary.Uvarint(postingsData)
	if n <= 0 {
		return nil, fmt.Errorf("error reading TF offset")
	}
	// termNotEncoded (0) is a sentinel value meaning "no TF data" - don't adjust it
	if tfOffset == termNotEncoded {
		rv.freqOffset = termNotEncoded
	} else {
		rv.freqOffset = baseOffset + tfOffset
	}
	localPos := uint64(n)

	// Read loc offset (stored as relative, convert to absolute if not sentinel)
	locOffset, n := binary.Uvarint(postingsData[localPos:])
	if n <= 0 {
		return nil, fmt.Errorf("error reading loc offset")
	}
	// termNotEncoded (0) is a sentinel value meaning "no loc data" - don't adjust it
	if locOffset == termNotEncoded {
		rv.locOffset = termNotEncoded
	} else {
		rv.locOffset = baseOffset + locOffset
	}
	localPos += uint64(n)

	// Read roaring bitmap length and data
	roaringLen, n := binary.Uvarint(postingsData[localPos:])
	if n <= 0 {
		return nil, fmt.Errorf("error reading roaring length")
	}
	localPos += uint64(n)

	roaringBytes := postingsData[localPos : localPos+roaringLen]

	if rv.postings == nil {
		rv.postings = roaring.New()
	}
	_, err = rv.postings.FromBuffer(roaringBytes)
	if err != nil {
		return nil, fmt.Errorf("error loading roaring bitmap: %w", err)
	}

	// Get chunk size
	rv.chunkSize, err = getChunkSize(d.sb.footer.chunkMode, rv.postings.GetCardinality(), d.sb.footer.numDocs)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (d *dictionary) Iterator(a Automaton, startKeyInclusive, endKeyExclusive []byte) DictionaryIterator {
	if d.fst != nil {
		rv := &dictionaryIterator{
			d: d,
		}

		itr, err := d.fst.Search(a, startKeyInclusive, endKeyExclusive)
		if err == nil {
			rv.itr = itr
		} else if err != vellum.ErrIteratorDone {
			rv.err = err
		}

		return rv
	}
	return emptyDictionaryIterator
}

func (d *dictionary) Contains(key []byte) (bool, error) {
	if d.fst == nil {
		return false, nil
	}
	return d.fst.Contains(key)
}

func (d *dictionary) Close() error {
	return nil
}

// dictionaryIterator iterates through terms in a ZIP dictionary
type dictionaryIterator struct {
	d     *dictionary
	itr   vellum.Iterator
	err   error
	tmp   postingsList
	entry DictEntry
}

func (i *dictionaryIterator) Next() (DictionaryEntry, error) {
	if i.err != nil && i.err != vellum.ErrIteratorDone {
		return nil, i.err
	} else if i.itr == nil || i.err == vellum.ErrIteratorDone {
		return nil, nil
	}

	term, postingsOffset := i.itr.Current()
	i.entry.term = string(term)

	// Read postings to get count
	pl, err := i.d.postingsListFromOffset(postingsOffset, nil, &i.tmp)
	if err != nil {
		return nil, err
	}
	i.entry.count = pl.Count()

	i.err = i.itr.Next()
	return &i.entry, nil
}

func (i *dictionaryIterator) Close() error {
	return nil
}

// zipDocVisitState holds state for visiting document values in a ZIP segment
type zipDocVisitState struct {
	dvrs    map[uint16]*docValueReader
	segment *segment
}

// visitDocumentFieldTerms visits document values for the specified fields
func (zs *segment) visitDocumentFieldTerms(localDocNum uint64, fields []string,
	visitor DocumentValueVisitor, dvs *zipDocVisitState) (
	*zipDocVisitState, error) {
	if dvs == nil {
		dvs = &zipDocVisitState{}
	} else if dvs.segment != zs {
		dvs.segment = zs
		dvs.dvrs = nil
	}

	if dvs.dvrs == nil {
		var ok bool
		var fieldIDPlus1 uint16
		dvs.dvrs = make(map[uint16]*docValueReader, len(fields))
		for _, field := range fields {
			if fieldIDPlus1, ok = zs.fieldsMap[field]; !ok {
				continue
			}
			fieldID := fieldIDPlus1 - 1
			if dvIter, exists := zs.fieldDvReaders[fieldID]; exists &&
				dvIter != nil {
				dvs.dvrs[fieldID] = dvIter.cloneInto(dvs.dvrs[fieldID])
			}
		}
	}

	// find the chunkNumber where the docValues are stored
	// NOTE: doc values continue to use legacy chunk mode
	chunkFactor, err := getChunkSize(legacyChunkMode, 0, 0)
	if err != nil {
		return nil, err
	}
	docInChunk := localDocNum / chunkFactor

	var dvr *docValueReader
	for _, field := range fields {
		var ok bool
		var fieldIDPlus1 uint16
		if fieldIDPlus1, ok = zs.fieldsMap[field]; !ok {
			continue
		}
		fieldID := fieldIDPlus1 - 1
		if dvr, ok = dvs.dvrs[fieldID]; ok && dvr != nil {
			// check if the chunk is already loaded
			if docInChunk != dvr.curChunkNumber() {
				// Read the docvalues data for this field
				off := zs.fieldOffsets[fieldID]
				if off.dvSize == 0 {
					continue
				}
				dvData, err := zs.data.Read(int(off.dvOffset), int(off.dvOffset+off.dvSize))
				if err != nil {
					return dvs, fmt.Errorf("error reading docvalue data: %w", err)
				}
				err = dvr.loadDvChunkFromData(docInChunk, dvData)
				if err != nil {
					return dvs, err
				}
			}

			_ = dvr.visitDocValues(localDocNum, visitor)
		}
	}
	return dvs, nil
}

// zipDocValueReader implements DocumentValueReader for ZIP segments
type zipDocValueReader struct {
	fields  []string
	state   *zipDocVisitState
	segment *segment
}

// VisitDocumentValues visits document values for a specific document
func (d *zipDocValueReader) VisitDocumentValues(number uint64, visitor DocumentValueVisitor) error {
	state, err := d.segment.visitDocumentFieldTerms(number, d.fields, visitor, d.state)
	if err != nil {
		return err
	}
	d.state = state
	return nil
}

// DocumentValueReader returns a reader for visiting document values
func (zs *segment) DocumentValueReader(fields []string) (DocumentValueReader, error) {
	return &zipDocValueReader{
		fields:  fields,
		segment: zs,
	}, nil
}

// ZipEntry represents a single entry in the ZIP archive
type ZipEntry struct {
	Name             string
	CompressedSize   uint64
	UncompressedSize uint64
}

// ZipEntries returns a list of all entries in the ZIP archive
func (zs *segment) ZipEntries() ([]ZipEntry, error) {
	data, err := zs.data.Read(0, zs.data.Size())
	if err != nil {
		return nil, fmt.Errorf("error reading segment data: %w", err)
	}
	dataLen := len(data)

	if dataLen < zipEOCDSize {
		return nil, fmt.Errorf("data too small for ZIP: %d bytes", dataLen)
	}

	// Find End of Central Directory record
	eocdPos := -1
	for i := dataLen - zipEOCDSize; i >= 0 && i >= dataLen-65557; i-- {
		if binary.LittleEndian.Uint32(data[i:]) == zipEOCDSignature {
			eocdPos = i
			break
		}
	}
	if eocdPos < 0 {
		return nil, fmt.Errorf("ZIP end of central directory not found")
	}

	totalEntries := int(binary.LittleEndian.Uint16(data[eocdPos+10:]))
	cdirOffset := int(binary.LittleEndian.Uint32(data[eocdPos+16:]))

	if cdirOffset < 0 || cdirOffset >= dataLen {
		return nil, fmt.Errorf("invalid central directory offset: %d", cdirOffset)
	}

	entries := make([]ZipEntry, 0, totalEntries)

	pos := cdirOffset
	for i := 0; i < totalEntries; i++ {
		if pos+zipCDirHeaderSize > dataLen {
			return nil, fmt.Errorf("truncated central directory at entry %d", i)
		}

		compressedSize := binary.LittleEndian.Uint32(data[pos+20:])
		uncompressedSize := binary.LittleEndian.Uint32(data[pos+24:])
		fileNameLen := int(binary.LittleEndian.Uint16(data[pos+28:]))
		extraFieldLen := int(binary.LittleEndian.Uint16(data[pos+30:]))
		fileCommentLen := int(binary.LittleEndian.Uint16(data[pos+32:]))

		nameStart := pos + zipCDirHeaderSize
		name := string(data[nameStart : nameStart+fileNameLen])

		entries = append(entries, ZipEntry{
			Name:             name,
			CompressedSize:   uint64(compressedSize),
			UncompressedSize: uint64(uncompressedSize),
		})

		pos += zipCDirHeaderSize + fileNameLen + extraFieldLen + fileCommentLen
	}

	return entries, nil
}
