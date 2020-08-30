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
		name    string
		pattern string
		input   []byte
		output  []byte
	}{
		{
			name:    "html",
			pattern: `</?[!\w]+((\s+\w+(\s*=\s*(?:".*?"|'.*?'|[^'">\s]+))?)+\s*|\s*)/?>`,
			input:   []byte(`<html>test</html>`),
			output:  []byte(`      test       `),
		},
		{
			name:    "zero width non-joiner",
			pattern: `\x{200C}`,
			input:   []byte("water\u200Cunder\u200Cthe\u200Cbridge"),
			output:  []byte("water   under   the   bridge"),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			patternRegexp := regexp.MustCompile(test.pattern)
			filter := NewRegexpCharFilter(patternRegexp, []byte{' '})
			output := filter.Filter(test.input)
			if !reflect.DeepEqual(output, test.output) {
				t.Errorf("Expected:\n`%s`\ngot:\n`%s`\nfor:\n`%s`\n", string(test.output), string(output), string(test.input))
			}
		})
	}
}
