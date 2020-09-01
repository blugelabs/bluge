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

package char

import (
	"reflect"
	"regexp"
	"testing"
)

func TestRegexpCharFilter(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		replacement []byte
		input       []byte
		output      []byte
	}{
		{
			name:        "html",
			pattern:     `</?[!\w]+((\s+\w+(\s*=\s*(?:".*?"|'.*?'|[^'">\s]+))?)+\s*|\s*)/?>`,
			replacement: []byte{' '},
			input:       []byte(`<html>test</html>`),
			output:      []byte(` test `),
		},
		{
			name:        "zero width non-joiner",
			pattern:     `\x{200C}`,
			replacement: []byte{' '},
			input:       []byte("water\u200Cunder\u200Cthe\u200Cbridge"),
			output:      []byte("water under the bridge"),
		},
		{
			name:        "pattern replacement",
			pattern:     `([a-z])\s+(\d)`,
			replacement: []byte(`$1-$2`),
			input:       []byte("temp 1"),
			output:      []byte("temp-1"),
		},

		{
			name:        "pattern replacement2",
			pattern:     `([a-z])\s+(\d)`,
			replacement: []byte(`$1-$2`),
			input:       []byte(`temp 1`),
			output:      []byte(`temp-1`),
		},
		{
			name:        "pattern replacement3",
			pattern:     `foo.?`,
			replacement: []byte(`X`),
			input:       []byte(`seafood, fool`),
			output:      []byte(`seaX, X`),
		},
		{
			name:        "pattern replacement4",
			pattern:     `def`,
			replacement: []byte(`_`),
			input:       []byte(`abcdefghi`),
			output:      []byte(`abc_ghi`),
		},
		{
			name:        "pattern replacement5",
			pattern:     `456`,
			replacement: []byte(`000000`),
			input:       []byte(`123456789`),
			output:      []byte(`123000000789`),
		},
		{
			pattern:     `“|”`,
			replacement: []byte(`"`),
			input:       []byte(`“hello”`),
			output:      []byte(`"hello"`),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			patternRegexp := regexp.MustCompile(test.pattern)
			filter := NewRegexpCharFilter(patternRegexp, test.replacement)
			output := filter.Filter(test.input)
			if !reflect.DeepEqual(output, test.output) {
				t.Errorf("Expected:\n`%s`\ngot:\n`%s`\nfor:\n`%s`\n", string(test.output), string(output), string(test.input))
			}
		})
	}
}
