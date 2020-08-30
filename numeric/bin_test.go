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

package numeric

import "testing"

func TestInterleaveDeinterleave(t *testing.T) {
	tests := []struct {
		v1 uint64
		v2 uint64
	}{
		{0, 0},
		{1, 1},
		{27, 39},
		{1<<32 - 1, 1<<32 - 1}, // largest that should still work
	}

	for _, test := range tests {
		i := Interleave(test.v1, test.v2)
		gotv1 := Deinterleave(i)
		gotv2 := Deinterleave(i >> 1)
		if gotv1 != test.v1 {
			t.Errorf("expected v1: %d, got %d, interleaved was %x", test.v1, gotv1, i)
		}
		if gotv2 != test.v2 {
			t.Errorf("expected v2: %d, got %d, interleaved was %x", test.v2, gotv2, i)
		}
	}
}
