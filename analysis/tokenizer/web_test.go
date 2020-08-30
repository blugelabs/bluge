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

package tokenizer

import (
	"reflect"
	"testing"

	"github.com/blugelabs/bluge/analysis"
)

func TestWeb(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		{
			[]byte("Hello info@blugelabs.com"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      5,
					Term:     []byte("Hello"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    6,
					End:      24,
					Term:     []byte("info@blugelabs.com"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("That http://blugelabs.com"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      4,
					Term:     []byte("That"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    5,
					End:      25,
					Term:     []byte("http://blugelabs.com"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("Hey @blugelabs"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      3,
					Term:     []byte("Hey"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    4,
					End:      14,
					Term:     []byte("@blugelabs"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("This #bluge"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      4,
					Term:     []byte("This"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    5,
					End:      11,
					Term:     []byte("#bluge"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("What about @blugelabs?"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      4,
					Term:     []byte("What"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    5,
					End:      10,
					Term:     []byte("about"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    11,
					End:      21,
					Term:     []byte("@blugelabs"),
					Position: 3,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
	}

	tokenizer := NewWebTokenizer()
	for _, test := range tests {
		actual := tokenizer.Tokenize(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("Expected %v, got %v for %s", test.output, actual, string(test.input))
		}
	}
}
