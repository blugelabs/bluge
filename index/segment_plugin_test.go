// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package index

import (
	"strconv"
	"strings"
	"testing"
)

func TestDefaultIndexSegmentsAreZapxBlugeV1(t *testing.T) {
	cfg, cleanup := CreateConfig("TestDefaultIndexSegmentsAreZapxBlugeV1")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "zap", false, false, true),
	}
	batch := NewBatch()
	batch.Update(testIdentifier("1"), doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	if len(reader.segment) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(reader.segment))
	}
	seg := reader.segment[0].segment
	if seg.Type() != "zapx-bluge" {
		t.Fatalf("expected segment type zapx-bluge, got %s", seg.Type())
	}
	if seg.Version() != 1 {
		t.Fatalf("expected segment version 1, got %d", seg.Version())
	}
}

func TestZapxBlugeCollectionStatsPersisted(t *testing.T) {
	cfg, cleanup := CreateConfig("TestZapxBlugeCollectionStatsPersisted")
	defer func() {
		if err := cleanup(); err != nil {
			t.Log(err)
		}
	}()

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	batch := NewBatch()
	batch.Update(testIdentifier("1"), &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("body", "alpha alpha beta", false, false, true),
		NewFakeField("body", "gamma gamma", false, false, true),
	})
	batch.Update(testIdentifier("2"), &FakeDocument{
		NewFakeField("_id", "2", true, false, false),
		NewFakeField("body", "alpha", false, false, true),
	})
	batch.Update(testIdentifier("3"), &FakeDocument{
		NewFakeField("_id", "3", true, false, false),
		NewFakeField("other", "delta", false, false, true),
	})
	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenReader(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	stats, err := reader.CollectionStats("body")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stats.TotalDocumentCount(), uint64(3); got != want {
		t.Fatalf("total document count: got %d want %d", got, want)
	}
	if got, want := stats.DocumentCount(), uint64(2); got != want {
		t.Fatalf("field document count: got %d want %d", got, want)
	}
	if got, want := stats.SumTotalTermFrequency(), uint64(6); got != want {
		t.Fatalf("total term frequency: got %d want %d", got, want)
	}

	missing, err := reader.CollectionStats("missing")
	if err != nil {
		t.Fatal(err)
	}
	if missing.TotalDocumentCount() != 0 || missing.DocumentCount() != 0 ||
		missing.SumTotalTermFrequency() != 0 {
		t.Fatalf("unexpected missing field stats: total=%d docs=%d freq=%d",
			missing.TotalDocumentCount(), missing.DocumentCount(), missing.SumTotalTermFrequency())
	}
}

func TestZapxBlugePersistsCustomNorm(t *testing.T) {
	cfg, cleanup := CreateConfig("TestZapxBlugePersistsCustomNorm")
	defer func() {
		if err := cleanup(); err != nil {
			t.Log(err)
		}
	}()
	cfg = cfg.WithNormCalc(func(field string, length int) float32 {
		if field == "body" {
			return float32(length * 10)
		}
		return float32(length)
	})

	idx, err := OpenWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	batch := NewBatch()
	batch.Update(testIdentifier("1"), &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("body", "one two", false, false, false),
		NewFakeField("body", "three", false, false, false),
	})
	if err = idx.Batch(batch); err != nil {
		t.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenReader(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	postings, err := reader.PostingsIterator([]byte("one"), "body", true, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer postings.Close()
	posting, err := postings.Next()
	if err != nil {
		t.Fatal(err)
	}
	if posting == nil {
		t.Fatal("expected posting")
	}
	if got, want := posting.Norm(), 30.0; got != want {
		t.Fatalf("persisted norm: got %v want %v", got, want)
	}
}

func TestLegacyIceSegmentPluginError(t *testing.T) {
	_, err := loadSegmentPlugin(defaultConfig().supportedSegmentPlugins, "ice", 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `unsupported legacy segment type "ice"; migrate the index offline`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExperimentalZapSegmentPluginError(t *testing.T) {
	_, err := loadSegmentPlugin(defaultConfig().supportedSegmentPlugins, "zap", 17)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `unsupported experimental segment type "zap" version 17; rebuild the index with zapx-bluge`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func BenchmarkZapxBlugeCollectionStats(b *testing.B) {
	cfg, cleanup := CreateConfig("BenchmarkZapxBlugeCollectionStats")
	terms := make([]string, 10000)
	for i := range terms {
		terms[i] = "term" + strconv.Itoa(i)
	}

	idx, err := OpenWriter(cfg)
	if err != nil {
		b.Fatal(err)
	}
	batch := NewBatch()
	batch.Update(testIdentifier("1"), &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("body", strings.Join(terms, " "), false, false, false),
	})
	if err = idx.Batch(batch); err != nil {
		b.Fatal(err)
	}
	if err = idx.Close(); err != nil {
		b.Fatal(err)
	}
	reader, err := OpenReader(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = reader.Close()
		_ = cleanup()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats, err := reader.CollectionStats("body")
		if err != nil {
			b.Fatal(err)
		}
		if stats.DocumentCount() != 1 || stats.SumTotalTermFrequency() != uint64(len(terms)) {
			b.Fatal("unexpected collection statistics")
		}
	}
}
