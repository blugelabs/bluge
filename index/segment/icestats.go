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

package segment

type collectionStats struct {
	totalDocCount    uint64
	docCount         uint64
	sumTotalTermFreq uint64
}

func (c *collectionStats) TotalDocumentCount() uint64 {
	return c.totalDocCount
}

func (c *collectionStats) DocumentCount() uint64 {
	return c.docCount
}

func (c *collectionStats) SumTotalTermFrequency() uint64 {
	return c.sumTotalTermFreq
}

func (c *collectionStats) Merge(other CollectionStats) {
	c.totalDocCount += other.TotalDocumentCount()
	c.docCount += other.DocumentCount()
	c.sumTotalTermFreq += other.SumTotalTermFrequency()
}
