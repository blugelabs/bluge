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
	"strconv"

	"github.com/blugelabs/bluge/index/segment"
	"github.com/spf13/cobra"
)

const (
	modeExploreDictionary   = 2
	modeExplorePostingsList = 3
	modeExplorePosting      = 4

	segmentExploreArgField = 2
)

var segmentExploreCmd = &cobra.Command{
	Use:   "explore [path] [field] <term> <docNum>",
	Short: "explores the segment by field, then term (optional), and then docNum (optional)",
	Long:  `The explore command lets you explore the segment in order of field, then optionally by term, then optionally again by doc number.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < segmentExploreArgField {
			return fmt.Errorf("must specify field")
		}

		switch len(args) {
		case modeExploreDictionary:
			return exploreSegmentDictionary(args[1])
		case modeExplorePostingsList:
			return exploreSegmentPostingsList(args[1], args[2])
		case modeExplorePosting:
			docNum, err := strconv.ParseUint(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("unable to parse doc number: %v", err)
			}
			return exploreSegmentPosting(args[1], args[2], docNum)
		}

		return fmt.Errorf("unsupported number of arguments: %d", len(args))
	},
}

func exploreSegmentPostingsList(field, term string) error {
	dict, err := seg.Dictionary(field)
	if err != nil {
		return fmt.Errorf("error accessing dictionary for field '%s': %w", field, err)
	}

	postingsList, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil {
		return fmt.Errorf("error accessing postings list for field '%s' term '%s': %w", field, term, err)
	}

	postingsItr, err := postingsList.Iterator(true, true, false, nil)
	if err != nil {
		return fmt.Errorf("error building iterator: %v", err)
	}

	var posting segment.Posting
	posting, err = postingsItr.Next()
	for err == nil && posting != nil {
		fmt.Printf("%d %d %f\n", posting.Number(), posting.Frequency(), posting.Norm())
		posting, err = postingsItr.Next()
	}

	return nil
}

func exploreSegmentPosting(field, term string, docNum uint64) error {
	dict, err := seg.Dictionary(field)
	if err != nil {
		return fmt.Errorf("error accessing dictionary for field '%s': %w", field, err)
	}

	postingsList, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil {
		return fmt.Errorf("error accessing postings list for field '%s' term '%s': %w", field, term, err)
	}

	postingsItr, err := postingsList.Iterator(true, true, true, nil)
	if err != nil {
		return fmt.Errorf("error creating iterator: %v", err)
	}

	var posting segment.Posting
	posting, err = postingsItr.Advance(docNum)
	if err != nil {
		return err
	}
	if posting == nil || posting.Number() != docNum {
		return fmt.Errorf("docNum %d not found in postings list", docNum)
	}

	fmt.Printf("number: %d\n", posting.Number())
	fmt.Printf("freq: %d\n", posting.Frequency())
	fmt.Printf("norm: %f\n", posting.Norm())
	locs := posting.Locations()
	for i, loc := range locs {
		fmt.Printf("location %d: pos: %d start: %d end: %d field: %s\n", i, loc.Pos(), loc.Start(), loc.End(), loc.Field())
	}

	return nil
}

func init() {
	segmentCmd.AddCommand(segmentExploreCmd)
}
