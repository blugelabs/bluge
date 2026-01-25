#!/bin/bash
#
# Benchmark comparison script for bluge
#
# Usage: ./scripts/benchmark-compare.sh <branch-a> <branch-b> [doc-count]
#
# Example: ./scripts/benchmark-compare.sh master embed-ice-zip-only 100000
#

set -e

BRANCH_A="${1:-master}"
BRANCH_B="${2:-HEAD}"
DOC_COUNT="${3:-100000}"
SEED="${4:-42}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/benchmark-results"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Bluge Benchmark Comparison ===${NC}"
echo "Branch A: $BRANCH_A"
echo "Branch B: $BRANCH_B"
echo "Documents: $DOC_COUNT"
echo "Seed: $SEED"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Save current branch
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
ORIGINAL_STASH=""

# Function to restore original state
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    git checkout -- . 2>/dev/null || true
    git checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
    if [ -n "$ORIGINAL_STASH" ]; then
        git stash pop 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Stash any local changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo -e "${YELLOW}Stashing local changes...${NC}"
    git stash push -m "benchmark-compare temp stash"
    ORIGINAL_STASH="yes"
fi

# Save benchmark files from current location (they may not exist on older branches)
TEMP_DIR=$(mktemp -d)
echo -e "${YELLOW}Saving benchmark files to $TEMP_DIR${NC}"
cp "$REPO_ROOT/cmd/bluge/cmd/benchmark"*.go "$TEMP_DIR/" 2>/dev/null || true
cp "$REPO_ROOT/writer.go" "$TEMP_DIR/writer_with_stats.go" 2>/dev/null || true

# Function to prepare a branch for benchmarking
prepare_branch() {
    local branch=$1
    local output_name=$2

    echo -e "\n${GREEN}=== Preparing $branch ===${NC}"

    git checkout "$branch"

    # Copy benchmark files if they don't exist
    if [ ! -f "$REPO_ROOT/cmd/bluge/cmd/benchmark.go" ]; then
        echo -e "${YELLOW}Copying benchmark files to $branch...${NC}"
        cp "$TEMP_DIR/benchmark"*.go "$REPO_ROOT/cmd/bluge/cmd/" 2>/dev/null || {
            echo -e "${RED}Error: No benchmark files available${NC}"
            return 1
        }
    fi

    # Check if Stats() method exists in writer.go
    if ! grep -q "func (w \*Writer) Stats()" "$REPO_ROOT/writer.go"; then
        echo -e "${YELLOW}Adding Stats() method to writer.go...${NC}"
        # Append Stats method
        cat >> "$REPO_ROOT/writer.go" << 'EOF'

// Stats returns statistics about the index.
func (w *Writer) Stats() index.Stats {
	return w.chill.Stats()
}
EOF
    fi

    # Fix stats.go bug if present
    if grep -q "return s.stats" "$REPO_ROOT/index/stats.go"; then
        echo -e "${YELLOW}Fixing stats.go bug...${NC}"
        sed -i 's/return s.stats/return rv/' "$REPO_ROOT/index/stats.go"
    fi

    # Build
    echo "Building..."
    go build -o "$REPO_ROOT/bluge-$output_name" "$REPO_ROOT/cmd/bluge"

    echo -e "${GREEN}Build complete: bluge-$output_name${NC}"
}

# Function to run benchmark
run_benchmark() {
    local binary=$1
    local output_file=$2

    echo -e "\n${GREEN}=== Running benchmark with $binary ===${NC}"
    echo "Output: $output_file"
    echo "This may take several minutes..."

    "$REPO_ROOT/$binary" benchmark \
        --docs "$DOC_COUNT" \
        --seed "$SEED" \
        --output "$output_file" \
        2>&1 | tee "${output_file%.json}.log"

    echo -e "${GREEN}Benchmark complete${NC}"
}

# Prepare and run branch A
prepare_branch "$BRANCH_A" "branch-a"
RESULT_A="$RESULTS_DIR/${BRANCH_A//\//-}-$(date +%Y%m%d-%H%M%S).json"
run_benchmark "bluge-branch-a" "$RESULT_A"

# Clean up changes before switching
git checkout -- .

# Prepare and run branch B
prepare_branch "$BRANCH_B" "branch-b"
RESULT_B="$RESULTS_DIR/${BRANCH_B//\//-}-$(date +%Y%m%d-%H%M%S).json"
run_benchmark "bluge-branch-b" "$RESULT_B"

# Compare results
echo -e "\n${GREEN}=== Comparison ===${NC}"
"$REPO_ROOT/bluge-branch-b" benchmark compare "$RESULT_A" "$RESULT_B"

# Summary
echo -e "\n${GREEN}=== Summary ===${NC}"
echo "Branch A results: $RESULT_A"
echo "Branch B results: $RESULT_B"
echo ""
echo "To re-run comparison:"
echo "  ./bluge benchmark compare $RESULT_A $RESULT_B"

# Clean up binaries
rm -f "$REPO_ROOT/bluge-branch-a" "$REPO_ROOT/bluge-branch-b"
rm -rf "$TEMP_DIR"

echo -e "\n${GREEN}Done!${NC}"
