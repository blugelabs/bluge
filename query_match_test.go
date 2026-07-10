// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package bluge

import (
	"context"
	"math"
	"strings"
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"

	"github.com/blugelabs/bluge/search"
)

func TestMatchQueryBoostAppliedOnce(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	config := DefaultConfig(tmpIndexPath)
	writer, err := OpenWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Insert(NewDocument("1").AddField(NewTextField("body", "alpha beta"))); err != nil {
		t.Fatal(err)
	}
	reader, err := writer.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()

	tests := []struct {
		name      string
		text      string
		operator  MatchQueryOperator
		fuzziness int
	}{
		{name: "single", text: "alpha", operator: MatchQueryOperatorOr},
		{name: "or", text: "alpha beta", operator: MatchQueryOperatorOr},
		{name: "and", text: "alpha beta", operator: MatchQueryOperatorAnd},
		{name: "fuzzy", text: "alpha beta", operator: MatchQueryOperatorOr, fuzziness: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := matchQueryScore(t, reader, test.text, test.operator, test.fuzziness, 1)
			boosted := matchQueryScore(t, reader, test.text, test.operator, test.fuzziness, 3)
			if math.Abs(boosted-base*3) > 1e-12 {
				t.Fatalf("boosted score: got %v want %v", boosted, base*3)
			}
		})
	}
}

func TestMatchQueryValidatesOperatorBeforeSingleTermFastPath(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	config := DefaultConfig(tmpIndexPath)
	writer, err := OpenWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err = writer.Insert(NewDocument("1").AddField(NewTextField("body", "alpha"))); err != nil {
		t.Fatal(err)
	}
	reader, err := writer.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	query := NewMatchQuery("alpha").SetField("body").SetOperator(MatchQueryOperator(99))
	_, err = reader.Search(context.Background(), NewTopNSearch(1, query))
	if err == nil || !strings.Contains(err.Error(), "unhandled operator 99") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDefaultSimilarityCustomNormPersists(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	config := DefaultConfig(tmpIndexPath)
	config.DefaultSimilarity = normEchoSimilarity{}
	writer, err := OpenWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Insert(NewDocument("1").AddField(NewTextField("body", "alpha beta"))); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenReader(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	iterator, err := reader.Search(context.Background(), NewTopNSearch(1,
		NewTermQuery("alpha").SetField("body")).ExplainScores())
	if err != nil {
		t.Fatal(err)
	}
	match, err := iterator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if match == nil {
		t.Fatal("expected match")
	}
	if got, want := match.Score, 42.5; got != want {
		t.Fatalf("score from persisted custom norm: got %v want %v", got, want)
	}
	if match.Explanation == nil || match.Explanation.Value != 42.5 {
		t.Fatalf("unexpected explanation: %#v", match.Explanation)
	}
}

func matchQueryScore(t *testing.T, reader *Reader, text string, operator MatchQueryOperator,
	fuzziness int, boost float64) float64 {
	t.Helper()
	query := NewMatchQuery(text).
		SetField("body").
		SetOperator(operator).
		SetFuzziness(fuzziness).
		SetBoost(boost)
	iterator, err := reader.Search(context.Background(), NewTopNSearch(1, query))
	if err != nil {
		t.Fatal(err)
	}
	match, err := iterator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if match == nil {
		t.Fatal("expected match")
	}
	return match.Score
}

type normEchoSimilarity struct{}

func (normEchoSimilarity) ComputeNorm(int) float32 {
	return 42.5
}

func (normEchoSimilarity) Scorer(float64, segment.CollectionStats, segment.TermStats) search.Scorer {
	return normEchoScorer{}
}

type normEchoScorer struct{}

func (normEchoScorer) Score(_ int, norm float64) float64 {
	return norm
}

func (normEchoScorer) Explain(_ int, norm float64) *search.Explanation {
	return search.NewExplanation(norm, "persisted norm")
}
