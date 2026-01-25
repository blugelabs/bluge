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
	"fmt"

	"github.com/blugelabs/bluge/index/segment"
	"github.com/spf13/cobra"
)

const segmentDictArgField = 2

var segmentDictCmd = &cobra.Command{
	Use:   "dict [path] [field]",
	Short: "prints the term dictionary for the specified field",
	Long:  `The dict command lets you print the term dictionary for the specified field.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < segmentDictArgField {
			return fmt.Errorf("must specify field")
		}

		return exploreSegmentDictionary(args[1])
	},
}

func exploreSegmentDictionary(field string) error {
	dict, err := seg.Dictionary(field)
	if err != nil {
		return fmt.Errorf("error accessing dictionary for field '%s': %w", field, err)
	}

	dictItr := dict.Iterator(nil, nil, nil)

	var dictEntry segment.DictionaryEntry
	dictEntry, err = dictItr.Next()
	for err == nil && dictEntry != nil {
		term := printValueStr(dictEntry.Term())
		if term == "" {
			term = fmt.Sprintf("%#x", dictEntry.Term())
		}
		fmt.Printf("%s %d\n", term, dictEntry.Count())
		dictEntry, err = dictItr.Next()
	}

	return nil
}

func init() {
	segmentCmd.AddCommand(segmentDictCmd)
}
