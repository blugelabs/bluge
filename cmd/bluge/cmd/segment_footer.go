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

var segmentFooterCmd = &cobra.Command{
	Use:   "footer [path]",
	Short: "prints the contents of the segment footer",
	Long:  `The footer command will print the contents of the segment footer.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Format: ZIP\n")
		fmt.Printf("CRC: %#x\n", seg.CRC())
		fmt.Printf("Version: %d\n", seg.Version())
		fmt.Printf("Chunk Mode: %d\n", seg.ChunkMode())
		fmt.Printf("Num Docs: %d\n", seg.NumDocs())
		fmt.Printf("Fields: %v\n", seg.Fields())

		entries, err := seg.ZipEntries()
		if err != nil {
			return fmt.Errorf("error reading ZIP entries: %w", err)
		}

		fmt.Printf("\nZIP Entries (%d):\n", len(entries))
		for _, entry := range entries {
			fmt.Printf("  %s (compressed: %d, uncompressed: %d)\n",
				entry.Name, entry.CompressedSize, entry.UncompressedSize)
		}

		return nil
	},
}

func init() {
	segmentCmd.AddCommand(segmentFooterCmd)
}
