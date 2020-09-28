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

package collector

import (
	"context"
	"testing"

	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
)

func TestAllCollector(t *testing.T) {
	matches := makeMatches(99, 11)
	searcher := &stubSearcher{
		matches: matches,
	}

	aggs := make(search.Aggregations)
	aggs.Add("count", aggregations.CountMatches())

	collector := NewAllCollector()
	dmi, err := collector.Collect(context.Background(), aggs, searcher)
	if err != nil {
		t.Fatal(err)
	}

	var count uint64
	next, err := dmi.Next()
	for err == nil && next != nil {
		count++

		// test that we can see aggregations while iterating with this collector
		if dmi.Aggregations().Count() != count {
			t.Errorf("expected aggregations count to match running count, %d != %d",
				count, dmi.Aggregations().Count())
		}

		next, err = dmi.Next()
	}
	if err != nil {
		t.Fatalf("error iterator matches: %v", err)
	}

	if count != 99 {
		t.Errorf("expected to see 99 hits, saw: %d", count)
	}
}
