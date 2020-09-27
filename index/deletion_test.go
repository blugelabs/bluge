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

package index

import (
	"fmt"
	"reflect"
	"testing"
)

func TestDeletableEpochs(t *testing.T) {
	tests := []struct {
		name            string
		n               int
		knownEpochs     []uint64
		deletableEpochs []uint64
	}{
		{
			name:            "empty",
			n:               1,
			knownEpochs:     nil,
			deletableEpochs: nil,
		},
		{
			name:            "one",
			n:               1,
			knownEpochs:     []uint64{1},
			deletableEpochs: nil,
		},
		{
			name:            "many",
			n:               1,
			knownEpochs:     []uint64{1, 2, 3, 4},
			deletableEpochs: []uint64{1, 2, 3},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%s-%d", test.name, test.n), func(t *testing.T) {
			policy := NewKeepNLatestDeletionPolicy(test.n)
			for _, epoch := range test.knownEpochs {
				policy.Commit(&Snapshot{epoch: epoch})
			}
			if !reflect.DeepEqual(policy.deletableEpochs, test.deletableEpochs) {
				t.Errorf("expected deletable: %#v, got %#v", test.deletableEpochs, policy.deletableEpochs)
			}
		})
	}
}
