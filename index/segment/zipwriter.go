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
	"hash/crc32"
	"io"
)

// ZIP entry names
const (
	storedDataEntry  = "_stored/data"
	storedIndexEntry = "_stored/index"
	footerEntry      = "_footer"
)

// Per-field entry suffixes
const (
	fieldMetaSuffix       = "/meta"
	fieldDictionarySuffix = "/dictionary"
	fieldPostingsSuffix   = "/postings"
	fieldDocValuesSuffix  = "/docvalues"
)

// zipEntry represents a single entry in the ZIP segment
type zipEntry struct {
	name string
	buf  bytes.Buffer
	crc  uint32
}

// Write writes to the entry buffer and updates CRC
func (e *zipEntry) Write(b []byte) (int, error) {
	n, err := e.buf.Write(b)
	e.crc = crc32.Update(e.crc, crc32.IEEETable, b[:n])
	return n, err
}

// Count returns current size of entry
func (e *zipEntry) Count() int {
	return e.buf.Len()
}

// Bytes returns the entry data
func (e *zipEntry) Bytes() []byte {
	return e.buf.Bytes()
}

// Reset resets the entry for reuse
func (e *zipEntry) Reset(name string) {
	e.name = name
	e.buf.Reset()
	e.crc = 0
}

// ZIP local file header size (fixed portion)
const zipLocalHeaderFixedSize = 30
const zipDataDescriptorSize = 16 // signature (4) + CRC (4) + compressed size (4) + uncompressed size (4)

// zipSegmentWriter manages writing a ZIP-based segment
type zipSegmentWriter struct {
	entries    map[string]*zipEntry
	entryOrder []string // track order for deterministic output
	fieldNames []string // field names in order
	currentCRC uint32   // running CRC for compatibility

	// Streaming ZIP output
	zipBuf        bytes.Buffer
	zw            *zip.Writer
	manifest      []manifestEntry
	streamErr     error  // store any error during streaming
	runningOffset uint64 // tracks current offset in final ZIP

	// Reusable entry buffers for field processing (reduces allocations)
	fieldMetaEntry   zipEntry
	fieldDictEntry   zipEntry
	fieldPostEntry   zipEntry
	fieldDvEntry     zipEntry
	currentFieldName string // track which field is using the reusable entries
}

// newZipSegmentWriter creates a new ZIP segment writer
func newZipSegmentWriter() *zipSegmentWriter {
	return &zipSegmentWriter{
		entries: make(map[string]*zipEntry),
	}
}

// getEntry returns the entry for the given name, creating if needed
func (w *zipSegmentWriter) getEntry(name string) *zipEntry {
	if e, ok := w.entries[name]; ok {
		return e
	}
	e := &zipEntry{name: name}
	w.entries[name] = e
	w.entryOrder = append(w.entryOrder, name)
	return e
}

// StoredData returns the writer for stored field data
func (w *zipSegmentWriter) StoredData() *zipEntry {
	return w.getEntry(storedDataEntry)
}

// StoredIndex returns the writer for stored field index
func (w *zipSegmentWriter) StoredIndex() *zipEntry {
	return w.getEntry(storedIndexEntry)
}

// FieldMeta returns the writer for field metadata (uses reusable buffer)
func (w *zipSegmentWriter) FieldMeta(fieldName string) *zipEntry {
	w.prepareFieldEntry(fieldName)
	if w.fieldMetaEntry.name == "" {
		w.fieldMetaEntry.Reset(fieldName + fieldMetaSuffix)
	}
	return &w.fieldMetaEntry
}

// FieldDictionary returns the writer for field dictionary (uses reusable buffer)
func (w *zipSegmentWriter) FieldDictionary(fieldName string) *zipEntry {
	w.prepareFieldEntry(fieldName)
	if w.fieldDictEntry.name == "" {
		w.fieldDictEntry.Reset(fieldName + fieldDictionarySuffix)
	}
	return &w.fieldDictEntry
}

// FieldPostings returns the writer for field postings (uses reusable buffer)
func (w *zipSegmentWriter) FieldPostings(fieldName string) *zipEntry {
	w.prepareFieldEntry(fieldName)
	if w.fieldPostEntry.name == "" {
		w.fieldPostEntry.Reset(fieldName + fieldPostingsSuffix)
	}
	return &w.fieldPostEntry
}

// FieldDocValues returns the writer for field doc values (uses reusable buffer)
func (w *zipSegmentWriter) FieldDocValues(fieldName string) *zipEntry {
	w.prepareFieldEntry(fieldName)
	if w.fieldDvEntry.name == "" {
		w.fieldDvEntry.Reset(fieldName + fieldDocValuesSuffix)
	}
	return &w.fieldDvEntry
}

// ensureZipStarted lazily initializes the zip.Writer and writes stored entries
func (w *zipSegmentWriter) ensureZipStarted() {
	if w.zw != nil || w.streamErr != nil {
		return
	}
	w.zw = zip.NewWriter(&w.zipBuf)

	// Write any stored entries first (stored data/index)
	for _, name := range w.entryOrder {
		entry := w.entries[name]
		if entry.buf.Len() > 0 {
			if err := w.writeEntryToZip(entry); err != nil {
				w.streamErr = err
				return
			}
		}
	}
}

// prepareFieldEntry ensures we're working on the expected field
// If switching to a new field, finalizes the previous field first
func (w *zipSegmentWriter) prepareFieldEntry(fieldName string) {
	if w.streamErr != nil {
		return
	}
	if w.currentFieldName != "" && w.currentFieldName != fieldName {
		// Switching fields - finalize the previous one
		w.finalizeFieldInternal()
	}
	w.currentFieldName = fieldName
}

// FinalizeField writes current field entries to ZIP and resets buffers for reuse
func (w *zipSegmentWriter) FinalizeField() {
	w.finalizeFieldInternal()
}

// finalizeFieldInternal does the actual finalization work
func (w *zipSegmentWriter) finalizeFieldInternal() {
	if w.currentFieldName == "" || w.streamErr != nil {
		return
	}

	// Ensure ZIP is started before writing field entries
	w.ensureZipStarted()
	if w.streamErr != nil {
		return
	}

	// Write each non-empty field entry directly to ZIP
	entries := []*zipEntry{&w.fieldMetaEntry, &w.fieldDictEntry, &w.fieldPostEntry, &w.fieldDvEntry}
	for _, entry := range entries {
		if entry.buf.Len() > 0 {
			if err := w.writeEntryToZip(entry); err != nil {
				w.streamErr = err
				return
			}
		}
	}

	// Reset entries for reuse (keeps underlying buffer capacity)
	w.fieldMetaEntry.Reset("")
	w.fieldDictEntry.Reset("")
	w.fieldPostEntry.Reset("")
	w.fieldDvEntry.Reset("")
	w.currentFieldName = ""
}

// writeEntryToZip writes a single entry to the ZIP stream
func (w *zipSegmentWriter) writeEntryToZip(entry *zipEntry) error {
	header := &zip.FileHeader{
		Name:   entry.name,
		Method: zip.Store,
	}
	header.SetMode(0644)

	ew, err := w.zw.CreateHeader(header)
	if err != nil {
		return err
	}

	// Calculate offset where entry data starts
	// ZIP local file header = 30 bytes fixed + filename length (no extra field)
	dataOffset := w.runningOffset + zipLocalHeaderFixedSize + uint64(len(entry.name))

	data := entry.Bytes()
	_, err = ew.Write(data)
	if err != nil {
		return err
	}

	w.manifest = append(w.manifest, manifestEntry{
		Name:   entry.name,
		Offset: dataOffset,
		Size:   uint64(len(data)),
		CRC:    entry.crc,
	})

	// Update running offset: header + data + data descriptor
	// Go's archive/zip adds a data descriptor after each file when streaming
	w.runningOffset = dataOffset + uint64(len(data)) + zipDataDescriptorSize

	return nil
}

// SetFieldNames sets the ordered list of field names
func (w *zipSegmentWriter) SetFieldNames(names []string) {
	w.fieldNames = names
}

// manifestEntry represents an entry in the footer manifest
type manifestEntry struct {
	Name   string
	Offset uint64 // byte offset of entry data in final ZIP
	Size   uint64
	CRC    uint32
}

// SegmentOffsets contains the byte offsets needed to create a Segment
// without re-parsing the ZIP data
type SegmentOffsets struct {
	StoredDataOffset  uint64
	StoredDataSize    uint64
	StoredIndexOffset uint64
	StoredIndexSize   uint64
	FieldOffsets      []FieldEntryOffsets // indexed by fieldID
}

// FieldEntryOffsets contains offsets for a single field's ZIP entries
type FieldEntryOffsets struct {
	MetaOffset uint64
	MetaSize   uint64
	DictOffset uint64
	DictSize   uint64
	PostOffset uint64
	PostSize   uint64
	DvOffset   uint64
	DvSize     uint64
}

// GetOffsets returns the tracked offsets for all entries
// Must be called after WriteZip()
func (w *zipSegmentWriter) GetOffsets() *SegmentOffsets {
	offsets := &SegmentOffsets{
		FieldOffsets: make([]FieldEntryOffsets, len(w.fieldNames)),
	}

	// Build a map for quick lookup
	entryMap := make(map[string]manifestEntry, len(w.manifest))
	for _, entry := range w.manifest {
		entryMap[entry.Name] = entry
	}

	// Get stored entry offsets
	if entry, ok := entryMap[storedDataEntry]; ok {
		offsets.StoredDataOffset = entry.Offset
		offsets.StoredDataSize = entry.Size
	}
	if entry, ok := entryMap[storedIndexEntry]; ok {
		offsets.StoredIndexOffset = entry.Offset
		offsets.StoredIndexSize = entry.Size
	}

	// Get per-field offsets
	for fieldID, fieldName := range w.fieldNames {
		if entry, ok := entryMap[fieldName+fieldMetaSuffix]; ok {
			offsets.FieldOffsets[fieldID].MetaOffset = entry.Offset
			offsets.FieldOffsets[fieldID].MetaSize = entry.Size
		}
		if entry, ok := entryMap[fieldName+fieldDictionarySuffix]; ok {
			offsets.FieldOffsets[fieldID].DictOffset = entry.Offset
			offsets.FieldOffsets[fieldID].DictSize = entry.Size
		}
		if entry, ok := entryMap[fieldName+fieldPostingsSuffix]; ok {
			offsets.FieldOffsets[fieldID].PostOffset = entry.Offset
			offsets.FieldOffsets[fieldID].PostSize = entry.Size
		}
		if entry, ok := entryMap[fieldName+fieldDocValuesSuffix]; ok {
			offsets.FieldOffsets[fieldID].DvOffset = entry.Offset
			offsets.FieldOffsets[fieldID].DvSize = entry.Size
		}
	}

	return offsets
}

// WriteZip finalizes the ZIP and returns the bytes
func (w *zipSegmentWriter) WriteZip(numDocs uint64, version uint32, chunkMode uint32) ([]byte, error) {
	// Finalize any remaining field entries
	w.FinalizeField()

	// Check for any streaming errors
	if w.streamErr != nil {
		return nil, w.streamErr
	}

	// Ensure ZIP is started (handles case with no field entries)
	w.ensureZipStarted()
	if w.streamErr != nil {
		return nil, w.streamErr
	}

	// Write footer entry
	footerData, err := w.buildFooter(numDocs, version, chunkMode, w.manifest)
	if err != nil {
		return nil, err
	}

	header := &zip.FileHeader{
		Name:   footerEntry,
		Method: zip.Store,
	}
	header.SetMode(0644)

	ew, err := w.zw.CreateHeader(header)
	if err != nil {
		return nil, err
	}

	_, err = ew.Write(footerData)
	if err != nil {
		return nil, err
	}

	err = w.zw.Close()
	if err != nil {
		return nil, err
	}

	return w.zipBuf.Bytes(), nil
}

// buildFooter builds the _footer entry content
// Format:
//
//	[numDocs: uint64]
//	[version: uint32]
//	[chunkMode: uint32]
//	[entryCount: varint]
//	[entries: repeated]
//	  [nameLen: varint]
//	  [name: bytes]
//	  [size: uint64]
//	  [crc: uint32]
func (w *zipSegmentWriter) buildFooter(numDocs uint64, version uint32, chunkMode uint32, manifest []manifestEntry) ([]byte, error) {
	var buf bytes.Buffer

	// Write numDocs
	err := binary.Write(&buf, binary.BigEndian, numDocs)
	if err != nil {
		return nil, err
	}

	// Write version
	err = binary.Write(&buf, binary.BigEndian, version)
	if err != nil {
		return nil, err
	}

	// Write chunkMode
	err = binary.Write(&buf, binary.BigEndian, chunkMode)
	if err != nil {
		return nil, err
	}

	// Write entry count
	err = writeUvarintTo(&buf, uint64(len(manifest)))
	if err != nil {
		return nil, err
	}

	// Write each entry
	for _, entry := range manifest {
		// Write name length
		err = writeUvarintTo(&buf, uint64(len(entry.Name)))
		if err != nil {
			return nil, err
		}

		// Write name
		_, err = buf.WriteString(entry.Name)
		if err != nil {
			return nil, err
		}

		// Write size
		err = binary.Write(&buf, binary.BigEndian, entry.Size)
		if err != nil {
			return nil, err
		}

		// Write CRC
		err = binary.Write(&buf, binary.BigEndian, entry.CRC)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// writeUvarintTo writes a varint to a writer
func writeUvarintTo(w io.Writer, val uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, val)
	_, err := w.Write(buf[:n])
	return err
}
