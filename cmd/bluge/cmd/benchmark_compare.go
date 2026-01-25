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
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var benchmarkCompareCmd = &cobra.Command{
	Use:   "compare <file1.json> <file2.json>",
	Short: "Compare two benchmark result files",
	Long:  `Compare two benchmark result files and display the differences in a formatted table.`,
	Args:  cobra.ExactArgs(2),
	RunE:  runBenchmarkCompare,
}

func init() {
	benchmarkCmd.AddCommand(benchmarkCompareCmd)
}

func runBenchmarkCompare(cmd *cobra.Command, args []string) error {
	// Load both result files
	results1, err := loadBenchmarkResults(args[0])
	if err != nil {
		return fmt.Errorf("error loading %s: %w", args[0], err)
	}

	results2, err := loadBenchmarkResults(args[1])
	if err != nil {
		return fmt.Errorf("error loading %s: %w", args[1], err)
	}

	// Print comparison
	printComparison(results1, results2)

	return nil
}

func loadBenchmarkResults(path string) (*BenchmarkResults, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var results BenchmarkResults
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, err
	}

	return &results, nil
}

func printComparison(a, b *BenchmarkResults) {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("BLUGE PERFORMANCE COMPARISON")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Branch A: %s (%s)\n", a.Metadata.Branch, truncateCommit(a.Metadata.Commit))
	fmt.Printf("Branch B: %s (%s)\n", b.Metadata.Branch, truncateCommit(b.Metadata.Commit))
	fmt.Printf("Documents: %d | Seed: %d\n", a.Metadata.DocCount, a.Metadata.RandomSeed)
	fmt.Println()

	// Index Build comparison
	fmt.Println("INDEX BUILD")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-25s %15s %15s %12s %10s\n", "Metric", a.Metadata.Branch, b.Metadata.Branch, "Delta", "Change")
	fmt.Println(strings.Repeat("-", 80))

	printMetricRow("Duration",
		formatDuration(a.IndexBuild.DurationMs),
		formatDuration(b.IndexBuild.DurationMs),
		int64(a.IndexBuild.DurationMs),
		int64(b.IndexBuild.DurationMs),
		true)

	printMetricRow("Size on Disk",
		formatBytes(a.IndexBuild.SizeBytes),
		formatBytes(b.IndexBuild.SizeBytes),
		int64(a.IndexBuild.SizeBytes),
		int64(b.IndexBuild.SizeBytes),
		true)

	printMetricRow("Segments",
		fmt.Sprintf("%d", a.IndexBuild.SegmentCount),
		fmt.Sprintf("%d", b.IndexBuild.SegmentCount),
		int64(a.IndexBuild.SegmentCount),
		int64(b.IndexBuild.SegmentCount),
		false)

	fmt.Println()

	// Query Performance comparison
	fmt.Println("QUERY PERFORMANCE (p50 latency, microseconds)")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-12s %-18s %10s %12s %10s %10s\n", "Category", "Test", a.Metadata.Branch, b.Metadata.Branch, "Delta", "Change")
	fmt.Println(strings.Repeat("-", 80))

	// Build lookup maps
	aQueries := make(map[string]QueryResult)
	for _, q := range a.QueryResults {
		aQueries[q.Name] = q
	}

	bQueries := make(map[string]QueryResult)
	for _, q := range b.QueryResults {
		bQueries[q.Name] = q
	}

	// Print all queries (preferring order from b, but including all from a)
	printed := make(map[string]bool)
	allQueries := append([]QueryResult{}, b.QueryResults...)
	for _, q := range a.QueryResults {
		if _, ok := bQueries[q.Name]; !ok {
			allQueries = append(allQueries, q)
		}
	}

	for _, q := range allQueries {
		if printed[q.Name] {
			continue
		}
		printed[q.Name] = true

		aq, aok := aQueries[q.Name]
		bq, bok := bQueries[q.Name]

		if aok && bok {
			printQueryRow(aq.Category, aq.Name,
				int64(aq.P50Us), int64(bq.P50Us))
		} else if aok {
			fmt.Printf("%-12s %-18s %10d %12s %10s %10s\n",
				aq.Category, aq.Name, aq.P50Us, "N/A", "N/A", "N/A")
		} else if bok {
			fmt.Printf("%-12s %-18s %10s %12d %10s %10s\n",
				bq.Category, bq.Name, "N/A", bq.P50Us, "N/A", "N/A")
		}
	}

	fmt.Println()

	// Concurrent Results comparison (if present)
	if len(a.ConcurrentResults) > 0 || len(b.ConcurrentResults) > 0 {
		fmt.Println("CONCURRENT THROUGHPUT (queries per second)")
		fmt.Println(strings.Repeat("-", 80))
		fmt.Printf("%-12s %12s %12s %12s %10s\n", "Concurrency", a.Metadata.Branch, b.Metadata.Branch, "Delta", "Change")
		fmt.Println(strings.Repeat("-", 80))

		aConcurrent := make(map[int]ConcurrentResult)
		for _, c := range a.ConcurrentResults {
			aConcurrent[c.Concurrency] = c
		}

		bConcurrent := make(map[int]ConcurrentResult)
		for _, c := range b.ConcurrentResults {
			bConcurrent[c.Concurrency] = c
		}

		concurrencies := []int{1, 4, 8, 16}
		for _, conc := range concurrencies {
			ac, aok := aConcurrent[conc]
			bc, bok := bConcurrent[conc]

			if aok && bok {
				delta := bc.QPS - ac.QPS
				pct := 0.0
				if ac.QPS > 0 {
					pct = (delta / ac.QPS) * 100
				}
				sign := "+"
				if delta < 0 {
					sign = ""
				}
				fmt.Printf("%-12d %12.1f %12.1f %11s%.1f %9s%.1f%%\n",
					conc, ac.QPS, bc.QPS, sign, delta, sign, pct)
			} else if aok {
				fmt.Printf("%-12d %12.1f %12s %12s %10s\n",
					conc, ac.QPS, "N/A", "N/A", "N/A")
			} else if bok {
				fmt.Printf("%-12d %12s %12.1f %12s %10s\n",
					conc, "N/A", bc.QPS, "N/A", "N/A")
			}
		}
		fmt.Println()
	}

	// Result Verification
	fmt.Println("RESULT VERIFICATION")
	fmt.Println(strings.Repeat("-", 80))

	mismatches := 0
	for _, bq := range b.QueryResults {
		if aq, ok := aQueries[bq.Name]; ok {
			if aq.ResultHash != bq.ResultHash {
				fmt.Printf("✗ %s: result hash mismatch\n", bq.Name)
				fmt.Printf("  %s: %s (count: %d)\n", a.Metadata.Branch, aq.ResultHash[:16], aq.ResultCount)
				fmt.Printf("  %s: %s (count: %d)\n", b.Metadata.Branch, bq.ResultHash[:16], bq.ResultCount)
				mismatches++
			}
		}
	}

	if mismatches == 0 {
		matchCount := 0
		for _, bq := range b.QueryResults {
			if _, ok := aQueries[bq.Name]; ok {
				matchCount++
			}
		}
		fmt.Printf("✓ All %d query result hashes match between branches\n", matchCount)
	} else {
		fmt.Printf("\n⚠ %d query result hash mismatches found\n", mismatches)
	}
}

func truncateCommit(commit string) string {
	if len(commit) > 7 {
		return commit[:7]
	}
	return commit
}

func formatDuration(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	return fmt.Sprintf("%.1fs", float64(ms)/1000)
}

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func printMetricRow(name, aVal, bVal string, aNum, bNum int64, lowerIsBetter bool) {
	delta := bNum - aNum
	pct := 0.0
	if aNum != 0 {
		pct = (float64(delta) / float64(aNum)) * 100
	}

	sign := "+"
	if delta < 0 {
		sign = ""
	}

	// Color indicator: green if improvement, red if regression
	indicator := ""
	if lowerIsBetter {
		if delta < 0 {
			indicator = "↓" // improvement
		} else if delta > 0 {
			indicator = "↑" // regression
		}
	} else {
		if delta > 0 {
			indicator = "↑" // improvement
		} else if delta < 0 {
			indicator = "↓" // regression
		}
	}

	fmt.Printf("%-25s %15s %15s %11s%d %9s%.1f%% %s\n",
		name, aVal, bVal, sign, delta, sign, pct, indicator)
}

func printQueryRow(category, name string, aP50, bP50 int64) {
	delta := bP50 - aP50
	pct := 0.0
	if aP50 != 0 {
		pct = (float64(delta) / float64(aP50)) * 100
	}

	sign := "+"
	if delta < 0 {
		sign = ""
	}

	// Lower latency is better
	indicator := ""
	if delta < 0 {
		indicator = "↓"
	} else if delta > 0 {
		indicator = "↑"
	}

	fmt.Printf("%-12s %-18s %10d %12d %9s%d %9s%.1f%% %s\n",
		category, name, aP50, bP50, sign, delta, sign, pct, indicator)
}
