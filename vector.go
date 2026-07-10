// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package bluge

import "errors"

var ErrVectorUnsupported = errors.New("vector search is not enabled")

type VectorSimilarity string

const (
	VectorL2     VectorSimilarity = "l2_norm"
	VectorDot    VectorSimilarity = "dot_product"
	VectorCosine VectorSimilarity = "cosine"
)

type VectorFieldSpec struct {
	Name       string
	Dims       int
	Similarity VectorSimilarity
}

type VectorHit struct {
	ID    Identifier
	Score float64
}

type VectorIndex interface {
	Search(field string, query []float32, k int, filter Query) ([]VectorHit, error)
	Close() error
}

type VectorBackend interface {
	Name() string
	Open(config Config) (VectorIndex, error)
}

func (config Config) WithVectorBackend(backend VectorBackend) Config {
	config.VectorBackend = backend
	return config
}
