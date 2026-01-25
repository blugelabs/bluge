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
	"fmt"
	"math"
	"sort"

	"github.com/golang/snappy"
)

type docNumTermsVisitor func(docNum uint64, terms []byte) error

type docValueReader struct {
	field          string
	curChunkNum    uint64
	chunkOffsets   []uint64
	dvDataLoc      uint64
	curChunkHeader []metaData
	curChunkData   []byte // compressed data cache
	uncompressed   []byte // temp buf for snappy decompression
}

func (di *docValueReader) size() int {
	return reflectStaticSizedocValueReader + sizeOfPtr +
		len(di.field) +
		len(di.chunkOffsets)*sizeOfUint64 +
		len(di.curChunkHeader)*reflectStaticSizeMetaData +
		len(di.curChunkData)
}

func (di *docValueReader) cloneInto(rv *docValueReader) *docValueReader {
	if rv == nil {
		rv = &docValueReader{}
	}

	rv.field = di.field
	rv.curChunkNum = math.MaxInt64
	rv.chunkOffsets = di.chunkOffsets // immutable, so it's sharable
	rv.dvDataLoc = di.dvDataLoc
	rv.curChunkHeader = rv.curChunkHeader[:0]
	rv.curChunkData = nil
	rv.uncompressed = rv.uncompressed[:0]

	return rv
}

func (di *docValueReader) curChunkNumber() uint64 {
	return di.curChunkNum
}

// loadDvChunkFromData loads a doc value chunk from raw []byte data
// Used by ZIP segments where data is available as []byte
func (di *docValueReader) loadDvChunkFromData(chunkNumber uint64, data []byte) error {
	destChunkDataLoc, curChunkEnd := di.dvDataLoc, di.dvDataLoc
	start, end := readChunkBoundary(int(chunkNumber), di.chunkOffsets)
	if start >= end {
		di.curChunkHeader = di.curChunkHeader[:0]
		di.curChunkData = nil
		di.curChunkNum = chunkNumber
		di.uncompressed = di.uncompressed[:0]
		return nil
	}

	destChunkDataLoc += start
	curChunkEnd += end

	// read the number of docs reside in the chunk
	numDocs, read := binary.Uvarint(data[destChunkDataLoc:])
	if read <= 0 {
		return fmt.Errorf("failed to read the chunk")
	}
	chunkMetaLoc := destChunkDataLoc + uint64(read)

	offset := uint64(0)
	if cap(di.curChunkHeader) < int(numDocs) {
		di.curChunkHeader = make([]metaData, int(numDocs))
	} else {
		di.curChunkHeader = di.curChunkHeader[:int(numDocs)]
	}
	for i := 0; i < int(numDocs); i++ {
		di.curChunkHeader[i].DocNum, read = binary.Uvarint(data[chunkMetaLoc+offset:])
		if read <= 0 {
			return fmt.Errorf("failed to read doc num")
		}
		offset += uint64(read)
		di.curChunkHeader[i].DocDvOffset, read = binary.Uvarint(data[chunkMetaLoc+offset:])
		if read <= 0 {
			return fmt.Errorf("failed to read doc dv offset")
		}
		offset += uint64(read)
	}

	compressedDataLoc := chunkMetaLoc + offset
	dataLength := curChunkEnd - compressedDataLoc
	di.curChunkData = data[compressedDataLoc : compressedDataLoc+dataLength]
	di.curChunkNum = chunkNumber
	di.uncompressed = di.uncompressed[:0]
	return nil
}

// iterateAllDocValuesFromData iterates through all doc values using raw []byte data.
// This is used by ZIP segments during merge where data is available as []byte.
func (di *docValueReader) iterateAllDocValuesFromData(data []byte, visitor docNumTermsVisitor) error {
	for i := 0; i < len(di.chunkOffsets); i++ {
		err := di.loadDvChunkFromData(uint64(i), data)
		if err != nil {
			return err
		}
		if di.curChunkData == nil || len(di.curChunkHeader) == 0 {
			continue
		}

		// uncompress the already loaded data
		uncompressed, err := snappy.Decode(di.uncompressed[:cap(di.uncompressed)], di.curChunkData)
		if err != nil {
			return err
		}
		di.uncompressed = uncompressed

		start := uint64(0)
		for _, entry := range di.curChunkHeader {
			err = visitor(entry.DocNum, uncompressed[start:entry.DocDvOffset])
			if err != nil {
				return err
			}

			start = entry.DocDvOffset
		}
	}

	return nil
}

func (di *docValueReader) visitDocValues(docNum uint64,
	visitor DocumentValueVisitor) error {
	// binary search the term locations for the docNum
	start, end := di.getDocValueLocs(docNum)
	if start == math.MaxUint64 || end == math.MaxUint64 || start == end {
		return nil
	}

	var uncompressed []byte
	var err error
	// use the uncompressed copy if available
	if len(di.uncompressed) > 0 {
		uncompressed = di.uncompressed
	} else {
		// uncompress the already loaded data
		uncompressed, err = snappy.Decode(di.uncompressed[:cap(di.uncompressed)], di.curChunkData)
		if err != nil {
			return err
		}
		di.uncompressed = uncompressed
	}

	// pick the terms for the given docNum
	uncompressed = uncompressed[start:end]
	for {
		i := bytes.Index(uncompressed, termSeparatorSplitSlice)
		if i < 0 {
			break
		}

		visitor(di.field, uncompressed[0:i])
		uncompressed = uncompressed[i+1:]
	}

	return nil
}

func (di *docValueReader) getDocValueLocs(docNum uint64) (start, end uint64) {
	i := sort.Search(len(di.curChunkHeader), func(i int) bool {
		return di.curChunkHeader[i].DocNum >= docNum
	})
	if i < len(di.curChunkHeader) && di.curChunkHeader[i].DocNum == docNum {
		return readDocValueBoundary(i, di.curChunkHeader)
	}
	return math.MaxUint64, math.MaxUint64
}
