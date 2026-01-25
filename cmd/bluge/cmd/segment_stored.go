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

	"github.com/spf13/cobra"
)

const segmentStoredArgDocNum = 2

var segmentStoredCmd = &cobra.Command{
	Use:   "stored [path] [docNum]",
	Short: "prints the stored section for a doc number",
	Long:  `The stored command will print the raw stored data bytes for the specified document number.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < segmentStoredArgDocNum {
			return fmt.Errorf("must specify doc number")
		}

		docNum, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse doc number: %v", err)
		}

		return seg.VisitStoredFields(docNum, func(field string, value []byte) bool {
			opt := printValue(value)
			if opt != "" {
				opt = "(" + opt + ")"
			}
			fmt.Printf("%s %#x %s\n", field, value, opt)
			return true
		})
	},
}

func init() {
	segmentCmd.AddCommand(segmentStoredCmd)
}
