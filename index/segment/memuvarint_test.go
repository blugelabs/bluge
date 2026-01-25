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
	"math"
	"testing"
)

func BenchmarkUvarint(b *testing.B) {
	n, buf := generateCommonUvarints(64, 512)

	reader := bytes.NewReader(buf)
	seen := 0

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if seen >= n {
			reader.Reset(buf)
			seen = 0
		}

		_, _ = binary.ReadUvarint(reader)
		seen++
	}
}

func BenchmarkMemUvarintReader(b *testing.B) {
	n, buf := generateCommonUvarints(64, 512)

	reader := &memUvarintReader{S: buf}
	seen := 0

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if seen >= n {
			reader.Reset(buf)
			seen = 0
		}

		_, _ = reader.ReadUvarint()
		seen++
	}
}

// generate some common, encoded uvarint's that we might see as
// freq-norm's or locations.
func generateCommonUvarints(maxFreq, maxFieldLen int) (n int, rv []byte) {
	buf := make([]byte, binary.MaxVarintLen64)

	var out bytes.Buffer

	encode := func(val uint64) {
		bufLen := binary.PutUvarint(buf, val)
		out.Write(buf[:bufLen])
		n++
	}

	for i := 1; i < maxFreq; i *= 2 { // Common freqHasLoc's.
		freqHasLocs := uint64(i << 1)
		encode(freqHasLocs)
		encode(freqHasLocs | 0x01) // 0'th LSB encodes whether there are locations.
	}

	encodeNorm := func(fieldLen int) {
		norm := float32(1.0 / math.Sqrt(float64(fieldLen)))
		normUint64 := uint64(math.Float32bits(norm))
		encode(normUint64)
	}

	for i := 1; i < maxFieldLen; i *= 2 { // Common norm's.
		encodeNorm(i)
	}

	return n, out.Bytes()
}
