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

	"github.com/spf13/cobra"
)

var segmentFieldsCmd = &cobra.Command{
	Use:   "fields [path]",
	Short: "prints the fields in the specified segment file",
	Long:  `The fields command lets you print the fields in the specified segment file.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fields := seg.Fields()
		for i, field := range fields {
			cs, err := seg.CollectionStats(field)
			if err != nil {
				return fmt.Errorf("error getting field collection stats: %v", err)
			}
			fmt.Printf("%d %s %d %d\n", i, field, cs.DocumentCount(), cs.SumTotalTermFrequency())
		}
		return nil
	},
}

func init() {
	segmentCmd.AddCommand(segmentFieldsCmd)
}
