package similarity

import (
	"math"
	"testing"
)

func TestBM25SimilarityIdfMatchesBM25Formula(t *testing.T) {
	sim := NewBM25Similarity()

	const docFreq = 10
	const docCount = 100

	want := math.Log(1.0 + (float64(docCount-docFreq)+0.5)/(float64(docFreq)+0.5))
	got := sim.Idf(docFreq, docCount)

	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("unexpected idf: got %v want %v", got, want)
	}
}

func TestBM25SimilarityIdfDoesNotReturnNegativeValue(t *testing.T) {
	sim := NewBM25Similarity()

	got := sim.Idf(101, 100)

	if got != 0 {
		t.Fatalf("unexpected idf: got %v want 0", got)
	}
}
