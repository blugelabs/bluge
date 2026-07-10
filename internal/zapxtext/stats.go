// Copyright (c) 2026 The Bluge Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package zap

type fieldStats struct {
	documentCount         uint64
	sumTotalTermFrequency uint64
}

func (s *fieldStats) add(other fieldStats) {
	s.documentCount += other.documentCount
	s.sumTotalTermFrequency += other.sumTotalTermFrequency
}

// FieldStats returns the persisted collection statistics for field.
func (sb *SegmentBase) FieldStats(field string) (documentCount, sumTotalTermFrequency uint64, ok bool) {
	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 == 0 {
		return 0, 0, false
	}
	fieldID := int(fieldIDPlus1 - 1)
	if fieldID >= len(sb.fieldStats) {
		return 0, 0, false
	}
	stats := sb.fieldStats[fieldID]
	return stats.documentCount, stats.sumTotalTermFrequency, true
}
