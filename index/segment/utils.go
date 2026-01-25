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
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/RoaringBitmap/roaring"
)

// fieldNotUninverted is a sentinel value indicating no doc values
const fieldNotUninverted = math.MaxUint64

// docDropped is a sentinel docNum to represent a deleted doc
const docDropped = math.MaxInt64

// _idFieldName is the name of the internal ID field
const _idFieldName = "_id"

// fileAddrWidth is the width of file address entries (8 bytes for uint64)
const fileAddrWidth = 8

// numUintsLocation is the number of uint64s used to encode a location
const numUintsLocation = 4

// mapFields takes the fieldsInv list and returns a map of fieldName
// to fieldID+1
func mapFields(fields []string) map[string]uint16 {
	rv := make(map[string]uint16, len(fields))
	for i, fieldName := range fields {
		rv[fieldName] = uint16(i) + 1
	}
	return rv
}

// isClosed checks if the channel is closed
func isClosed(closeCh chan struct{}) bool {
	select {
	case <-closeCh:
		return true
	default:
		return false
	}
}

// varintEncoder is a function type for encoding varints
type varintEncoder func(uint64) (int, error)

// encodeStoredFieldValues encodes stored field values into the provided data slice
func encodeStoredFieldValues(fieldID int,
	storedFieldValues [][]byte,
	curr int, metaEncode varintEncoder, data []byte) (
	newCurr int, newData []byte, err error) {
	for i := 0; i < len(storedFieldValues); i++ {
		// encode field
		_, err := metaEncode(uint64(fieldID))
		if err != nil {
			return 0, nil, err
		}
		// encode start offset
		_, err = metaEncode(uint64(curr))
		if err != nil {
			return 0, nil, err
		}
		// end len
		_, err = metaEncode(uint64(len(storedFieldValues[i])))
		if err != nil {
			return 0, nil, err
		}

		data = append(data, storedFieldValues[i]...)
		curr += len(storedFieldValues[i])
	}

	return curr, data, nil
}

// totalUvarintBytes returns the total # of bytes needed to encode the given uint64's
// into binary.PutUVarint() encoding
func totalUvarintBytes(a, b, c, d uint64) (n int) {
	n = numUvarintBytes(a)
	n += numUvarintBytes(b)
	n += numUvarintBytes(c)
	n += numUvarintBytes(d)
	return n
}

// numUvarintBytes returns # of bytes needed to encode x in binary.PutUvarint() encoding
func numUvarintBytes(x uint64) (n int) {
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n + 1
}

// writeUvarints writes multiple uint64 values as varints to the writer
func writeUvarints(w io.Writer, vals ...uint64) (err error) {
	buf := make([]byte, binary.MaxVarintLen64)
	for _, val := range vals {
		n := binary.PutUvarint(buf, val)
		_, err = w.Write(buf[:n])
		if err != nil {
			return err
		}
	}
	return err
}

// mergeTermFreqNormLocs merges term frequency, norm, and location data.
// NOTE: This is a temporary function for backwards compatibility during
// the flat-to-ZIP format transition. It will be removed when mergezip.go
// is updated.
func mergeTermFreqNormLocs(fieldsMap map[string]uint16, postItr *postingsIterator,
	newDocNums []uint64, newRoaring *roaring.Bitmap,
	tfEncoder, locEncoder *chunkedIntCoder, bufLoc []uint64, docTracking *roaring.Bitmap) (
	lastDocNum, lastFreq, lastNorm uint64, bufLocOut []uint64, err error) {
	next, err := postItr.Next()
	for next != nil && err == nil {
		hitNewDocNum := newDocNums[next.Number()]
		if hitNewDocNum == docDropped {
			return 0, 0, 0, nil, fmt.Errorf("see hit with dropped docNum")
		}

		newRoaring.Add(uint32(hitNewDocNum))
		docTracking.Add(uint32(hitNewDocNum))

		nextFreq := next.Frequency()
		nextNorm := uint64(math.Float32bits(float32(next.Norm())))

		locs := next.Locations()

		err = tfEncoder.Add(hitNewDocNum,
			encodeFreqHasLocs(uint64(nextFreq), len(locs) > 0), nextNorm)
		if err != nil {
			return 0, 0, 0, nil, err
		}

		if len(locs) > 0 {
			numBytesLocs := 0
			for _, loc := range locs {
				numBytesLocs += totalUvarintBytes(uint64(fieldsMap[loc.Field()]-1),
					uint64(loc.Pos()), uint64(loc.Start()), uint64(loc.End()))
			}

			err = locEncoder.Add(hitNewDocNum, uint64(numBytesLocs))
			if err != nil {
				return 0, 0, 0, nil, err
			}

			for _, loc := range locs {
				if cap(bufLoc) < numUintsLocation {
					bufLoc = make([]uint64, 0, numUintsLocation)
				}
				args := bufLoc[0:4]
				args[0] = uint64(fieldsMap[loc.Field()] - 1)
				args[1] = uint64(loc.Pos())
				args[2] = uint64(loc.Start())
				args[3] = uint64(loc.End())
				err = locEncoder.Add(hitNewDocNum, args...)
				if err != nil {
					return 0, 0, 0, nil, err
				}
			}
		}

		lastDocNum = hitNewDocNum
		lastFreq = uint64(nextFreq)
		lastNorm = nextNorm

		next, err = postItr.Next()
	}

	return lastDocNum, lastFreq, lastNorm, bufLoc, err
}
