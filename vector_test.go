// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package bluge

import (
	"errors"
	"testing"
)

func TestDefaultVectorBackendUnsupported(t *testing.T) {
	config := InMemoryOnlyConfig()
	if config.VectorBackend == nil {
		t.Fatal("expected default vector backend")
	}
	if config.VectorBackend.Name() != "unsupported" {
		t.Fatalf("expected unsupported backend, got %s", config.VectorBackend.Name())
	}
	_, err := config.VectorBackend.Open(config)
	if !errors.Is(err, ErrVectorUnsupported) {
		t.Fatalf("expected ErrVectorUnsupported, got %v", err)
	}
}
