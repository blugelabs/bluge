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
	"os"
	"unicode"

	"github.com/blevesearch/mmap-go"
	"github.com/blugelabs/bluge/index/segment"
	"github.com/spf13/cobra"
)

var seg segment.Segment

const segmentArgFilename = 1

// segmentCmd represents the segment command
var segmentCmd = &cobra.Command{
	Use:   "segment [path]",
	Short: "interact with a segment file",
	Long:  `The segment command lets you interact with a segment file.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < segmentArgFilename {
			return fmt.Errorf("must specify path to segment file")
		}

		var err error
		var closeF closeFunc
		seg, closeF, err = openSegmentFromFile(args[0])
		if err != nil {
			return fmt.Errorf("error opening segment file: %v", err)
		}
		_ = closeF // closeF will be called in PersistentPostRunE if needed

		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func init() {
	RootCmd.AddCommand(segmentCmd)
}

func printValue(val []byte) string {
	return printValueStr(string(val))
}

func printValueStr(str string) string {
	for _, r := range str {
		if !unicode.IsPrint(r) {
			return ""
		}
	}
	return str
}

type closeFunc func() error

var noCloseFunc = func() error {
	return nil
}

func openSegmentFromFile(path string) (segment.Segment, closeFunc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, noCloseFunc, err
	}
	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		// mmap failed, try to close the file
		_ = f.Close()
		return nil, noCloseFunc, err
	}

	closeF := func() error {
		err2 := mm.Unmap()
		// try to close file even if unmap failed
		err3 := f.Close()
		if err2 == nil {
			// try to return first error
			err2 = err3
		}
		return err2
	}

	s, err := segment.Open(mm)
	if err != nil {
		_ = closeF()
		return nil, noCloseFunc, fmt.Errorf("error loading segment: %v", err)
	}

	return s, closeF, nil
}
