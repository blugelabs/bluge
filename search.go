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

package bluge

import (
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/blugelabs/bluge/search/collector"
)

type SearchRequest interface {
	Collector() search.Collector
	Searcher(i search.Reader, config Config) (search.Searcher, error)
	AddAggregation(name string, aggregation search.Aggregation)
	Aggregations() search.Aggregations
}

type SearchOptions struct {
	ExplainScores    bool
	IncludeLocations bool
}

type BaseSearch struct {
	query        Query
	options      SearchOptions
	aggregations search.Aggregations
}

func (b BaseSearch) Query() Query {
	return b.query
}

func (b BaseSearch) Options() SearchOptions {
	return b.options
}

func (b BaseSearch) Aggregations() search.Aggregations {
	return b.aggregations
}

func (b BaseSearch) Searcher(i search.Reader, config Config) (search.Searcher, error) {
	return b.query.Searcher(i, searchOptionsFromConfig(config, b.options))
}

type TopNSearch struct {
	BaseSearch
	n        int
	from     int
	sort     search.SortOrder
	after    [][]byte
	reversed bool
}

func NewTopNSearch(n int, q Query) *TopNSearch {
	return &TopNSearch{
		BaseSearch: BaseSearch{
			query:        q,
			aggregations: make(search.Aggregations),
		},
		n: n,
		sort: search.SortOrder{
			search.SortBy(search.DocumentScore()).Desc(),
		},
	}
}

var standardAggs = search.Aggregations{
	"count":     aggregations.CountMatches(),
	"max_score": aggregations.Max(search.DocumentScore()),
}

func (s *TopNSearch) WithStandardAggregations() *TopNSearch {
	for name, agg := range standardAggs {
		s.AddAggregation(name, agg)
	}
	return s
}

func (s *TopNSearch) Size() int {
	return s.n
}

func (s *TopNSearch) SetFrom(from int) *TopNSearch {
	s.from = from
	return s
}

func (s *TopNSearch) From() int {
	return s.from
}

func (s *TopNSearch) After(after [][]byte) *TopNSearch {
	s.after = after
	return s
}

func (s *TopNSearch) Before(before [][]byte) *TopNSearch {
	s.after = before
	s.reversed = true
	return s
}

func (s *TopNSearch) SortBy(order []string) *TopNSearch {
	s.sort = search.ParseSortOrderStrings(order)
	return s
}

func (s *TopNSearch) SortByCustom(order search.SortOrder) *TopNSearch {
	s.sort = order
	return s
}

func (s *TopNSearch) SortOrder() search.SortOrder {
	return s.sort
}

func (s *TopNSearch) ExplainScores() *TopNSearch {
	s.options.ExplainScores = true
	return s
}

func (s *TopNSearch) IncludeLocations() *TopNSearch {
	s.options.IncludeLocations = true
	return s
}

func (s *TopNSearch) Collector() search.Collector {
	if s.after != nil {
		collectorSort := s.sort
		if s.reversed {
			// preserve original sort order in the request
			collectorSort = s.sort.Copy()
			collectorSort.Reverse()
		}
		rv := collector.NewTopNCollectorAfter(s.n, collectorSort, s.after, s.reversed)
		return rv
	}
	return collector.NewTopNCollector(s.n, s.from, s.sort)
}

func searchOptionsFromConfig(config Config, options SearchOptions) search.SearcherOptions {
	return search.SearcherOptions{
		SimilarityForField: func(field string) search.Similarity {
			if pfs, ok := config.PerFieldSimilarity[field]; ok {
				return pfs
			}
			return config.DefaultSimilarity
		},
		DefaultSearchField: config.DefaultSearchField,
		DefaultAnalyzer:    config.DefaultSearchAnalyzer,
		Explain:            options.ExplainScores,
		IncludeTermVectors: options.IncludeLocations,
	}
}

func (s *TopNSearch) AddAggregation(name string, aggregation search.Aggregation) {
	s.aggregations.Add(name, aggregation)
}

type AllMatches struct {
	BaseSearch
}

func NewAllMatches(q Query) *AllMatches {
	return &AllMatches{
		BaseSearch: BaseSearch{
			query: q,
		},
	}
}

func (s *AllMatches) WithStandardAggregations() *AllMatches {
	for name, agg := range standardAggs {
		s.AddAggregation(name, agg)
	}
	return s
}

func (s *AllMatches) AddAggregation(name string, aggregation search.Aggregation) {
	s.aggregations.Add(name, aggregation)
}

func (s *AllMatches) ExplainScores() *AllMatches {
	s.options.ExplainScores = true
	return s
}

func (s *AllMatches) IncludeLocations() *AllMatches {
	s.options.IncludeLocations = true
	return s
}

func (s *AllMatches) Collector() search.Collector {
	return collector.NewAllCollector()
}

func (s *TopNSearch) AllMatches(i search.Reader, config Config) (search.Searcher, error) {
	return s.query.Searcher(i, search.SearcherOptions{
		DefaultSearchField: config.DefaultSearchField,
		Explain:            s.options.ExplainScores,
		IncludeTermVectors: s.options.IncludeLocations,
	})
}

// memNeededForSearch is a helper function that returns an estimate of RAM
// needed to execute a search request.
func memNeededForSearch(
	searcher search.Searcher,
	coll search.Collector) uint64 {
	numDocMatches := coll.BackingSize() + searcher.DocumentMatchPoolSize()

	estimate := 0

	// overhead, size in bytes from collector
	estimate += coll.Size()

	// pre-allocing DocumentMatchPool
	estimate += searchContextEmptySize + numDocMatches*documentMatchEmptySize

	// searcher overhead
	estimate += searcher.Size()

	// overhead from results, lowestMatchOutsideResults
	estimate += (numDocMatches + 1) * documentMatchEmptySize

	return uint64(estimate)
}
