//  Copyright (c) 2020 Couchbase, Inc.
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
	"testing"
)

func TestEventBatchIntroductionStart(t *testing.T) {
	testConfig, cleanup := CreateConfig("TestEventBatchIntroductionStart")
	defer func() {
		err := cleanup()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var count int
	testConfig.EventCallback = func(e Event) {
		if e.Kind == EventKindBatchIntroductionStart {
			count++
		}
	}

	idx, err := OpenWriter(testConfig)
	if err != nil {
		t.Fatal(err)
	}

	doc := &FakeDocument{
		NewFakeField("_id", "1", true, false, false),
		NewFakeField("name", "test", false, false, true),
	}

	b := NewBatch()
	b.Update(testIdentifier("1"), doc)
	err = idx.Batch(b)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	if count != 1 {
		t.Fatalf("expected to see 1 batch introduction event event, saw %d", count)
	}
}
