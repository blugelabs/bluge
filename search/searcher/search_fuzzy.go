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

package searcher

import (
	"fmt"

	segment "github.com/blugelabs/bluge_segment_api"

	"github.com/blugelabs/bluge/search"
	"github.com/couchbase/vellum/levenshtein"
)

// reusable, thread-safe levenshtein builders
var levAutomatonBuilders map[int]*levenshtein.LevenshteinAutomatonBuilder

func init() {
	levAutomatonBuilders = map[int]*levenshtein.LevenshteinAutomatonBuilder{}
	supportedFuzziness := []int{1, 2}
	for _, fuzziness := range supportedFuzziness {
		lb, err := levenshtein.NewLevenshteinAutomatonBuilder(uint8(fuzziness), true)
		if err != nil {
			panic(fmt.Errorf("levenshtein automaton ed1 builder err: %v", err))
		}
		levAutomatonBuilders[fuzziness] = lb
	}
}

var MaxFuzziness = 2

func NewFuzzySearcher(indexReader search.Reader, term string,
	prefix, fuzziness int, field string, boost float64, scorer search.Scorer,
	options search.SearcherOptions) (search.Searcher, error) {
	if fuzziness > MaxFuzziness {
		return nil, fmt.Errorf("fuzziness exceeds max (%d)", MaxFuzziness)
	}

	if fuzziness < 0 {
		return nil, fmt.Errorf("invalid fuzziness, negative")
	}

	// Note: we don't byte slice the term for a prefix because of runes.
	prefixTerm := ""
	for i, r := range term {
		if i < prefix {
			prefixTerm += string(r)
		} else {
			break
		}
	}
	candidateTerms, err := findFuzzyCandidateTerms(indexReader, term, fuzziness,
		field, prefixTerm)
	if err != nil {
		return nil, err
	}

	return NewMultiTermSearcher(indexReader, candidateTerms, field,
		boost, scorer, options, true)
}

func findFuzzyCandidateTerms(indexReader search.Reader, term string,
	fuzziness int, field, prefixTerm string) (rv []string, err error) {
	rv = make([]string, 0)

	a, err := getLevAutomaton(term, fuzziness)
	if err != nil {
		return nil, err
	}

	var prefixBeg, prefixEnd []byte
	if prefixTerm != "" {
		prefixBeg = []byte(prefixTerm)
		prefixEnd = incrementBytes(prefixBeg)
	}

	fieldDict, err := indexReader.DictionaryIterator(field, a, prefixBeg, prefixEnd)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := fieldDict.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	tfd, err := fieldDict.Next()
	for err == nil && tfd != nil {
		rv = append(rv, tfd.Term())
		if tooManyClauses(len(rv)) {
			return nil, tooManyClausesErr(field, len(rv))
		}
		tfd, err = fieldDict.Next()
	}
	return rv, err
}

func getLevAutomaton(term string, fuzziness int) (segment.Automaton, error) {
	if levAutomatonBuilder, ok := levAutomatonBuilders[fuzziness]; ok {
		return levAutomatonBuilder.BuildDfa(term, uint8(fuzziness))
	}
	return nil, fmt.Errorf("unsupported fuzziness: %d", fuzziness)
}
