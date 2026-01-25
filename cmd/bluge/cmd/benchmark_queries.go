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

package cmd

import (
	"time"

	"github.com/blugelabs/bluge"
)

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// QueryTestCase represents a benchmark query test case.
type QueryTestCase struct {
	Name        string
	Category    string
	Description string
	Query       bluge.Query
}

// GetQueryTestCases returns all benchmark query test cases.
// These queries use vocabulary from the document generator to ensure matches.
func GetQueryTestCases() []QueryTestCase {
	return []QueryTestCase{
		// Term queries - Low frequency (rare terms, ~50-100 matches)
		{
			Name:        "term_low_1",
			Category:    "term_low",
			Description: "Rare term in body field",
			Query:       bluge.NewTermQuery("xenomorphic0").SetField("body"),
		},
		{
			Name:        "term_low_2",
			Category:    "term_low",
			Description: "Another rare term in body field",
			Query:       bluge.NewTermQuery("protoplastic0").SetField("body"),
		},

		// Term queries - Medium frequency (~1k-5k matches)
		{
			Name:        "term_med_1",
			Category:    "term_med",
			Description: "Medium frequency term in body",
			Query:       bluge.NewTermQuery("technology").SetField("body"),
		},
		{
			Name:        "term_med_2",
			Category:    "term_med",
			Description: "Another medium frequency term",
			Query:       bluge.NewTermQuery("university").SetField("body"),
		},

		// Term queries - High frequency (~30k-50k matches)
		{
			Name:        "term_high_1",
			Category:    "term_high",
			Description: "High frequency term in body",
			Query:       bluge.NewTermQuery("information").SetField("body"),
		},
		{
			Name:        "term_high_2",
			Category:    "term_high",
			Description: "Another high frequency term",
			Query:       bluge.NewTermQuery("system").SetField("body"),
		},

		// Fuzzy queries
		{
			Name:        "fuzzy_edit1",
			Category:    "fuzzy",
			Description: "Fuzzy query with edit distance 1",
			Query:       bluge.NewFuzzyQuery("informaton").SetField("body").SetFuzziness(1), // misspelling
		},
		{
			Name:        "fuzzy_edit2",
			Category:    "fuzzy",
			Description: "Fuzzy query with edit distance 2",
			Query:       bluge.NewFuzzyQuery("tehnology").SetField("body").SetFuzziness(2), // misspelling
		},

		// Phrase queries
		{
			Name:        "phrase_2term",
			Category:    "phrase",
			Description: "2-word phrase query",
			Query:       bluge.NewMatchPhraseQuery("information system").SetField("body"),
		},
		{
			Name:        "phrase_3term",
			Category:    "phrase",
			Description: "3-word phrase query",
			Query:       bluge.NewMatchPhraseQuery("united states government").SetField("body"),
		},

		// Boolean queries
		{
			Name:        "bool_must_2",
			Category:    "boolean",
			Description: "Boolean AND with 2 terms",
			Query: bluge.NewBooleanQuery().
				AddMust(bluge.NewTermQuery("information").SetField("body")).
				AddMust(bluge.NewTermQuery("technology").SetField("body")),
		},
		{
			Name:        "bool_should_3",
			Category:    "boolean",
			Description: "Boolean OR with 3 terms",
			Query: bluge.NewBooleanQuery().
				AddShould(bluge.NewTermQuery("science").SetField("body")).
				AddShould(bluge.NewTermQuery("research").SetField("body")).
				AddShould(bluge.NewTermQuery("university").SetField("body")),
		},
		{
			Name:        "bool_complex",
			Category:    "boolean",
			Description: "Complex boolean with must, should, must_not",
			Query: bluge.NewBooleanQuery().
				AddMust(bluge.NewTermQuery("system").SetField("body")).
				AddShould(bluge.NewTermQuery("computer").SetField("body")).
				AddShould(bluge.NewTermQuery("software").SetField("body")).
				AddMustNot(bluge.NewTermQuery("military").SetField("body")),
		},

		// Numeric range query
		{
			Name:        "numeric_range",
			Category:    "numeric",
			Description: "Numeric range query on viewCount (100k-500k)",
			Query:       bluge.NewNumericRangeQuery(100000, 500000).SetField("viewCount"),
		},

		// Prefix query
		{
			Name:        "prefix_short",
			Category:    "prefix",
			Description: "3-character prefix query",
			Query:       bluge.NewPrefixQuery("inf").SetField("body"),
		},

		// Wildcard query
		{
			Name:        "wildcard_suffix",
			Category:    "wildcard",
			Description: "Wildcard suffix query",
			Query:       bluge.NewWildcardQuery("*ology").SetField("body"),
		},

		// Match query (uses analyzer)
		{
			Name:        "match_single",
			Category:    "match",
			Description: "Match query single term",
			Query:       bluge.NewMatchQuery("INFORMATION").SetField("body"), // uppercase to test analyzer
		},

		// Date range query
		{
			Name:        "date_range",
			Category:    "date",
			Description: "Date range query (2010-2015)",
			Query: bluge.NewDateRangeQuery(
				mustParseTime("2010-01-01T00:00:00Z"),
				mustParseTime("2015-12-31T23:59:59Z"),
			).SetField("timestamp"),
		},

		// Keyword/exact match on category
		{
			Name:        "keyword_category",
			Category:    "keyword",
			Description: "Exact match on category field",
			Query:       bluge.NewTermQuery("Science").SetField("category"),
		},
	}
}

// GetConcurrentTestQuery returns a representative query for concurrent stress testing.
func GetConcurrentTestQuery() bluge.Query {
	// Use a medium-frequency term query for representative load
	return bluge.NewTermQuery("technology").SetField("body")
}
