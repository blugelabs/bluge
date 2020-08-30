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

package search

import (
	"reflect"
	"testing"
)

func TestLocationsDedupe(t *testing.T) {
	a := &Location{}
	b := &Location{Pos: 1}
	c := &Location{Pos: 2}

	tests := []struct {
		input  Locations
		expect Locations
	}{
		{Locations{}, Locations{}},
		{Locations{a}, Locations{a}},
		{Locations{a, b, c}, Locations{a, b, c}},
		{Locations{a, a}, Locations{a}},
		{Locations{a, a, a}, Locations{a}},
		{Locations{a, b}, Locations{a, b}},
		{Locations{b, a}, Locations{a, b}},
		{Locations{c, b, a, c, b, a, c, b, a}, Locations{a, b, c}},
	}

	for testi, test := range tests {
		res := test.input.Dedupe()
		if !reflect.DeepEqual(res, test.expect) {
			t.Errorf("testi: %d, test: %+v, res: %+v", testi, test, res)
		}
	}
}
