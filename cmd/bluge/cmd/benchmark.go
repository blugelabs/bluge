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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/spf13/cobra"
)

// Benchmark flags
var (
	benchmarkDocs       int
	benchmarkPath       string
	benchmarkOutput     string
	benchmarkSeed       int64
	benchmarkQueryIters int
	benchmarkWarmup     int
)

// benchmarkCmd represents the benchmark command
var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Run comprehensive performance benchmarks",
	Long: `Run comprehensive performance benchmarks including:
- Index build with detailed metrics
- Varied query benchmarks with latency percentiles
- Concurrent query stress testing
- Result verification via hash comparison`,
	RunE: runBenchmark,
}

func init() {
	RootCmd.AddCommand(benchmarkCmd)

	benchmarkCmd.Flags().IntVar(&benchmarkDocs, "docs", 100000, "Number of documents to index")
	benchmarkCmd.Flags().StringVar(&benchmarkPath, "path", "", "Index path (default: temp directory)")
	benchmarkCmd.Flags().StringVar(&benchmarkOutput, "output", "", "JSON results file (default: stdout)")
	benchmarkCmd.Flags().Int64Var(&benchmarkSeed, "seed", 42, "Random seed for reproducibility")
	benchmarkCmd.Flags().IntVar(&benchmarkQueryIters, "query-iters", 100, "Query iterations")
	benchmarkCmd.Flags().IntVar(&benchmarkWarmup, "warmup", 10, "Warmup iterations")
}

// Result structures for JSON output
type BenchmarkResults struct {
	Metadata          BenchmarkMetadata  `json:"metadata"`
	IndexBuild        IndexBuildResult   `json:"index_build"`
	QueryResults      []QueryResult      `json:"query_results"`
	ConcurrentResults []ConcurrentResult `json:"concurrent_results"`
}

type BenchmarkMetadata struct {
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	Timestamp  string `json:"timestamp"`
	GoVersion  string `json:"go_version"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	DocCount   int    `json:"doc_count"`
	RandomSeed int64  `json:"random_seed"`
}

type IndexBuildResult struct {
	DurationMs   int64  `json:"duration_ms"`
	DocsIndexed  int    `json:"docs_indexed"`
	BatchCount   int    `json:"batch_count"`
	SizeBytes    uint64 `json:"size_bytes"`
	SegmentCount uint64 `json:"segment_count"`
	AllocsTotal  uint64 `json:"allocs_total"`
	AllocBytes   uint64 `json:"alloc_bytes"`
}

type QueryResult struct {
	Name        string `json:"name"`
	Category    string `json:"category"`
	Iterations  int    `json:"iterations"`
	MinUs       int64  `json:"min_us"`
	MaxUs       int64  `json:"max_us"`
	MeanUs      int64  `json:"mean_us"`
	P50Us       int64  `json:"p50_us"`
	P95Us       int64  `json:"p95_us"`
	P99Us       int64  `json:"p99_us"`
	ResultCount int    `json:"result_count"`
	ResultHash  string `json:"result_hash"`
}

type ConcurrentResult struct {
	Concurrency int     `json:"concurrency"`
	DurationSec int     `json:"duration_sec"`
	TotalQuries int64   `json:"total_queries"`
	QPS         float64 `json:"qps"`
	P50Us       int64   `json:"p50_us"`
	P95Us       int64   `json:"p95_us"`
	P99Us       int64   `json:"p99_us"`
	Errors      int64   `json:"errors"`
}

func runBenchmark(cmd *cobra.Command, args []string) error {
	// Setup temp directory if no path specified
	indexPath := benchmarkPath
	if indexPath == "" {
		tmpDir, err := os.MkdirTemp("", "bluge-benchmark-*")
		if err != nil {
			return fmt.Errorf("error creating temp directory: %w", err)
		}
		indexPath = tmpDir
		defer os.RemoveAll(tmpDir)
	}

	fmt.Fprintf(os.Stderr, "Starting benchmark with %d documents...\n", benchmarkDocs)
	fmt.Fprintf(os.Stderr, "Index path: %s\n", indexPath)
	fmt.Fprintf(os.Stderr, "Random seed: %d\n", benchmarkSeed)

	// Initialize results
	results := &BenchmarkResults{
		Metadata: collectMetadata(),
	}

	// Phase 1: Index Build
	fmt.Fprintf(os.Stderr, "\n=== Phase 1: Index Build ===\n")
	writer, buildResult, err := runIndexBuild(indexPath)
	if err != nil {
		return fmt.Errorf("index build failed: %w", err)
	}
	results.IndexBuild = buildResult

	// Phase 2: Wait for merges
	fmt.Fprintf(os.Stderr, "\n=== Phase 2: Waiting for Merges ===\n")
	if err := waitForMerges(writer, 5*time.Minute); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: merge wait timeout: %v\n", err)
	}

	// Collect final stats
	stats := writer.Stats()
	results.IndexBuild.SizeBytes = stats.CurOnDiskBytes
	results.IndexBuild.SegmentCount = stats.TotFileSegmentsAtRoot
	fmt.Fprintf(os.Stderr, "Final index size: %d bytes\n", results.IndexBuild.SizeBytes)
	fmt.Fprintf(os.Stderr, "Final segment count: %d\n", results.IndexBuild.SegmentCount)

	// Get reader for queries
	reader, err := writer.Reader()
	if err != nil {
		return fmt.Errorf("error getting reader: %w", err)
	}
	defer reader.Close()

	// Phase 3: Query Benchmarks
	fmt.Fprintf(os.Stderr, "\n=== Phase 3: Query Benchmarks ===\n")
	queryResults, err := runQueryBenchmarks(reader)
	if err != nil {
		return fmt.Errorf("query benchmarks failed: %w", err)
	}
	results.QueryResults = queryResults

	// Phase 4: Concurrent Stress Test
	fmt.Fprintf(os.Stderr, "\n=== Phase 4: Concurrent Stress Test ===\n")
	concurrentResults, err := runConcurrentTest(writer)
	if err != nil {
		return fmt.Errorf("concurrent test failed: %w", err)
	}
	results.ConcurrentResults = concurrentResults

	// Close writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %w", err)
	}

	// Output results
	output, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling results: %w", err)
	}

	if benchmarkOutput != "" {
		if err := os.WriteFile(benchmarkOutput, output, 0644); err != nil {
			return fmt.Errorf("error writing output file: %w", err)
		}
		fmt.Fprintf(os.Stderr, "\nResults written to: %s\n", benchmarkOutput)
	} else {
		fmt.Println(string(output))
	}

	return nil
}

func collectMetadata() BenchmarkMetadata {
	branch := getGitBranch()
	commit := getGitCommit()

	return BenchmarkMetadata{
		Branch:     branch,
		Commit:     commit,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		GoVersion:  runtime.Version(),
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		DocCount:   benchmarkDocs,
		RandomSeed: benchmarkSeed,
	}
}

func getGitBranch() string {
	cmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func getGitCommit() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func runIndexBuild(indexPath string) (*bluge.Writer, IndexBuildResult, error) {
	config := bluge.DefaultConfig(indexPath)
	writer, err := bluge.OpenWriter(config)
	if err != nil {
		return nil, IndexBuildResult{}, fmt.Errorf("error opening writer: %w", err)
	}

	generator := NewDocGenerator(benchmarkSeed)
	batchSize := 1000
	batch := bluge.NewBatch()

	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	startTime := time.Now()
	batchCount := 0
	docsInCurrentBatch := 0

	for i := 0; i < benchmarkDocs; i++ {
		doc := generator.Generate()
		batch.Insert(doc.ToDocument())
		docsInCurrentBatch++

		if docsInCurrentBatch >= batchSize {
			if err := writer.Batch(batch); err != nil {
				return nil, IndexBuildResult{}, fmt.Errorf("error indexing batch: %w", err)
			}
			batch.Reset()
			batchCount++
			docsInCurrentBatch = 0

			if (i+1)%10000 == 0 {
				fmt.Fprintf(os.Stderr, "Indexed %d documents...\n", i+1)
			}
		}
	}

	// Final batch
	if docsInCurrentBatch > 0 {
		if err := writer.Batch(batch); err != nil {
			return nil, IndexBuildResult{}, fmt.Errorf("error indexing final batch: %w", err)
		}
		batchCount++
	}

	duration := time.Since(startTime)
	runtime.ReadMemStats(&memStatsAfter)

	result := IndexBuildResult{
		DurationMs:  duration.Milliseconds(),
		DocsIndexed: benchmarkDocs,
		BatchCount:  batchCount,
		AllocsTotal: memStatsAfter.Mallocs - memStatsBefore.Mallocs,
		AllocBytes:  memStatsAfter.TotalAlloc - memStatsBefore.TotalAlloc,
	}

	fmt.Fprintf(os.Stderr, "Index build completed in %v\n", duration)
	fmt.Fprintf(os.Stderr, "Batches: %d, Memory allocated: %d MB\n",
		batchCount, result.AllocBytes/(1024*1024))

	return writer, result, nil
}

func waitForMerges(writer *bluge.Writer, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastSegments := uint64(0)
	stableCount := 0

	for time.Now().Before(deadline) {
		stats := writer.Stats()
		segments := stats.TotFileSegmentsAtRoot

		fmt.Fprintf(os.Stderr, "Segments: %d, Persisted: %d, Merged: %d\n",
			segments, stats.LastPersistedEpoch, stats.LastMergedEpoch)

		// Consider stable if segment count hasn't changed for 3 checks
		if segments == lastSegments {
			stableCount++
			if stableCount >= 3 && segments > 0 {
				fmt.Fprintf(os.Stderr, "Merges appear complete (stable at %d segments)\n", segments)
				return nil
			}
		} else {
			stableCount = 0
		}
		lastSegments = segments

		// If down to 1 segment, we're definitely done
		if segments == 1 {
			fmt.Fprintf(os.Stderr, "Merges complete (1 segment)\n")
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	return errors.New("merge timeout")
}

func runQueryBenchmarks(reader *bluge.Reader) ([]QueryResult, error) {
	testCases := GetQueryTestCases()
	results := make([]QueryResult, 0, len(testCases))

	for _, tc := range testCases {
		fmt.Fprintf(os.Stderr, "Running query: %s (%s)...\n", tc.Name, tc.Description)

		result, err := benchmarkQuery(reader, tc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Warning: %s failed: %v\n", tc.Name, err)
			continue
		}

		results = append(results, result)
		fmt.Fprintf(os.Stderr, "  Results: %d, p50: %dμs, p99: %dμs\n",
			result.ResultCount, result.P50Us, result.P99Us)
	}

	return results, nil
}

func benchmarkQuery(reader *bluge.Reader, tc QueryTestCase) (QueryResult, error) {
	request := bluge.NewTopNSearch(1000000, tc.Query) // Large N to get all results

	// Warmup
	for i := 0; i < benchmarkWarmup; i++ {
		iter, err := reader.Search(context.Background(), request)
		if err != nil {
			return QueryResult{}, err
		}
		for {
			match, err := iter.Next()
			if err != nil || match == nil {
				break
			}
		}
	}

	// Collect results and timing
	latencies := make([]int64, benchmarkQueryIters)
	var docIDs []string
	var resultCount int

	for i := 0; i < benchmarkQueryIters; i++ {
		start := time.Now()
		iter, err := reader.Search(context.Background(), request)
		if err != nil {
			return QueryResult{}, err
		}

		if i == 0 {
			// Collect doc IDs on first iteration
			docIDs = make([]string, 0)
		}

		count := 0
		for {
			match, err := iter.Next()
			if err != nil || match == nil {
				break
			}
			count++
			if i == 0 {
				// Collect ID for hash
				err = match.VisitStoredFields(func(field string, value []byte) bool {
					if field == "_id" {
						docIDs = append(docIDs, string(value))
					}
					return true
				})
				if err != nil {
					return QueryResult{}, err
				}
			}
		}

		latencies[i] = time.Since(start).Microseconds()
		if i == 0 {
			resultCount = count
		}
	}

	// Calculate statistics
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var sum int64
	for _, l := range latencies {
		sum += l
	}

	// Calculate result hash
	sort.Strings(docIDs)
	hash := sha256.Sum256([]byte(strings.Join(docIDs, ",")))

	return QueryResult{
		Name:        tc.Name,
		Category:    tc.Category,
		Iterations:  benchmarkQueryIters,
		MinUs:       latencies[0],
		MaxUs:       latencies[len(latencies)-1],
		MeanUs:      sum / int64(len(latencies)),
		P50Us:       percentile(latencies, 50),
		P95Us:       percentile(latencies, 95),
		P99Us:       percentile(latencies, 99),
		ResultCount: resultCount,
		ResultHash:  hex.EncodeToString(hash[:]),
	}, nil
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted) * p) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func runConcurrentTest(writer *bluge.Writer) ([]ConcurrentResult, error) {
	concurrencies := []int{1, 4, 8, 16}
	duration := 10 * time.Second
	query := GetConcurrentTestQuery()
	results := make([]ConcurrentResult, 0, len(concurrencies))

	for _, conc := range concurrencies {
		fmt.Fprintf(os.Stderr, "Running concurrent test with %d goroutines...\n", conc)

		result, err := runConcurrentLevel(writer, query, conc, duration)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		fmt.Fprintf(os.Stderr, "  QPS: %.1f, p50: %dμs, p99: %dμs, errors: %d\n",
			result.QPS, result.P50Us, result.P99Us, result.Errors)
	}

	return results, nil
}

func runConcurrentLevel(writer *bluge.Writer, query bluge.Query, concurrency int, duration time.Duration) (ConcurrentResult, error) {
	var totalQueries int64
	var totalErrors int64
	latencies := make([]int64, 0, 10000)
	var latencyMu sync.Mutex

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	request := bluge.NewTopNSearch(100, query)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each goroutine gets its own reader
			reader, err := writer.Reader()
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				return
			}
			defer reader.Close()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				start := time.Now()
				iter, err := reader.Search(context.Background(), request)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// Drain results
				for {
					match, err := iter.Next()
					if err != nil || match == nil {
						break
					}
				}

				latency := time.Since(start).Microseconds()
				atomic.AddInt64(&totalQueries, 1)

				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Calculate statistics
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	qps := float64(totalQueries) / duration.Seconds()

	return ConcurrentResult{
		Concurrency: concurrency,
		DurationSec: int(duration.Seconds()),
		TotalQuries: totalQueries,
		QPS:         qps,
		P50Us:       percentile(latencies, 50),
		P95Us:       percentile(latencies, 95),
		P99Us:       percentile(latencies, 99),
		Errors:      totalErrors,
	}, nil
}

// Ensure we use search package (for type checking)
var _ search.DocumentMatch
