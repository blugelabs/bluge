# Bluge Benchmark Tool

A comprehensive performance benchmarking tool for comparing bluge branches.

## Quick Start

```bash
# Build the CLI
go build -o bluge ./cmd/bluge

# Run benchmark (uses temp directory, outputs to stdout)
./bluge benchmark --docs 10000

# Run with custom settings
./bluge benchmark --docs 100000 --output results.json --seed 42
```

## Command Reference

### `bluge benchmark`

Run comprehensive performance benchmarks.

```
Flags:
  --docs int          Number of documents to index (default 100000)
  --path string       Index path (default: temp directory, auto-cleaned)
  --output string     JSON results file (default: stdout)
  --seed int64        Random seed for reproducibility (default 42)
  --query-iters int   Query iterations per query (default 100)
  --warmup int        Warmup iterations before timing (default 10)
```

### `bluge benchmark compare <file1.json> <file2.json>`

Compare two benchmark result files side-by-side.

## Comparing Two Branches

### Method 1: Using the Compare Script

```bash
# From the repository root
./scripts/benchmark-compare.sh master embed-ice-zip-only 100000
```

### Method 2: Manual Process

1. **Build and run on branch A:**
   ```bash
   git checkout master
   go build -o bluge-master ./cmd/bluge
   ./bluge-master benchmark --docs 100000 --output master-results.json
   ```

2. **Build and run on branch B:**
   ```bash
   git checkout embed-ice-zip-only
   go build -o bluge-branch ./cmd/bluge
   ./bluge-branch benchmark --docs 100000 --output branch-results.json
   ```

3. **Compare results:**
   ```bash
   ./bluge-branch benchmark compare master-results.json branch-results.json
   ```

## Important Notes

### Branch Compatibility

The benchmark code needs to be present on each branch you test. If testing against
an older branch that doesn't have the benchmark code:

1. **Copy the benchmark files** to the target branch:
   ```bash
   # Save benchmark files
   cp cmd/bluge/cmd/benchmark*.go /tmp/

   # Switch to target branch
   git checkout <target-branch>

   # Copy benchmark files over
   cp /tmp/benchmark*.go cmd/bluge/cmd/
   ```

2. **Add the Stats() method** to `writer.go` if missing:
   ```go
   // Stats returns statistics about the index.
   func (w *Writer) Stats() index.Stats {
       return w.chill.Stats()
   }
   ```

3. **Fix the stats.go bug** (if present) - in `index/stats.go`, ensure the
   `Stats()` method returns `rv` not `s.stats`:
   ```go
   func (s *Writer) Stats() Stats {
       rv := s.stats
       numFilesOnDisk, numBytesUsedDisk := s.directory.Stats()
       rv.CurOnDiskBytes = numBytesUsedDisk
       rv.CurOnDiskFiles = numFilesOnDisk
       return rv  // NOT s.stats
   }
   ```

### Reproducibility

- Use the same `--seed` value for both branches (default: 42)
- Use the same `--docs` count
- Run on the same machine with similar load conditions
- The document generator produces identical documents for the same seed

### Benchmark Phases

1. **Index Build**: Creates synthetic documents and indexes them in batches of 1000
2. **Merge Wait**: Waits for background merges to stabilize (up to 5 minutes)
3. **Query Benchmarks**: Runs 19 different query types with warmup + timed iterations
4. **Concurrent Test**: Stress tests with 1, 4, 8, and 16 concurrent goroutines

### Query Test Cases

| Category | Queries | Description |
|----------|---------|-------------|
| term_low | 2 | Rare terms (~100 matches) |
| term_med | 2 | Medium frequency (~38k matches) |
| term_high | 2 | High frequency (~95k matches) |
| fuzzy | 2 | Edit distance 1 and 2 |
| phrase | 2 | 2-word and 3-word phrases |
| boolean | 3 | AND, OR, complex combinations |
| numeric | 1 | Range query on numeric field |
| prefix | 1 | 3-character prefix |
| wildcard | 1 | Suffix wildcard (*ology) |
| match | 1 | Analyzed match query |
| date | 1 | Date range query |
| keyword | 1 | Exact match on keyword field |

### Output Format

Results are saved as JSON with:
- **metadata**: Branch, commit, timestamp, Go version, OS, document count, seed
- **index_build**: Duration, size, segment count, memory allocations
- **query_results**: Per-query latency percentiles (p50, p95, p99), result counts, result hashes
- **concurrent_results**: QPS and latency at each concurrency level

### Result Verification

The benchmark computes a SHA256 hash of sorted document IDs for each query.
When comparing branches, matching hashes confirm that both branches return
identical results for the same queries.

## Troubleshooting

### "batch.Size undefined"
The `Batch` type doesn't have a `Size()` method. The benchmark tracks batch
size with a counter variable instead.

### "match is nil" infinite loop
When iterating search results, check for both error AND nil match:
```go
for {
    match, err := iter.Next()
    if err != nil || match == nil {
        break
    }
    // process match
}
```

### Index size shows 0
This is a bug in older versions of `index/stats.go`. The function sets
`rv.CurOnDiskBytes` but then returns `s.stats` instead of `rv`. Fix by
returning `rv`.

### High query latencies
The benchmark collects ALL results (up to 1 million) to compute result hashes.
For high-frequency terms, this can take 200ms+ per query. This is expected
behavior for accurate comparison.
