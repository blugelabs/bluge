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

import (
	"strings"
)

type FakeDocument []*FakeField

func (f *FakeDocument) FakeComposite(name string, exclude []string) {
	nff := &FakeField{
		N: name,
	}

	// walk all the fields and build new map of all included data
	reduced := make(map[string][]*FakeLocation)
	reducedNolocs := make(map[string]int)
	f.EachField(func(field Field) {
		// if we should skip this field return
		for _, excluded := range exclude {
			if field.Name() == excluded {
				return
			}
		}

		// for each term/location record entry
		// making copy of location, because we set F
		field.EachTerm(func(term FieldTerm) {
			var numLocs = 0
			term.EachLocation(func(location Location) {
				locCopy := &FakeLocation{
					F: field.Name(),
					P: location.Pos(),
					S: location.Start(),
					E: location.End(),
				}
				reduced[string(term.Term())] = append(reduced[string(term.Term())], locCopy)
				numLocs++
			})
			if numLocs == 0 {
				reducedNolocs[string(term.Term())] = reducedNolocs[string(term.Term())] + term.Frequency()
			}
		})
	})

	// walk the reduced term info across all fields and build the field
	for term, locations := range reduced {
		ft := &FakeTerm{
			T: term,
			F: len(locations),
			L: locations,
		}
		nff.T = append(nff.T, ft)
	}
	for term, freq := range reducedNolocs {
		ft := &FakeTerm{
			T: term,
			F: freq,
		}
		nff.T = append(nff.T, ft)
	}

	// add it to ourself
	*f = append(*f, nff)
}

func (f *FakeDocument) Analyze() {

}

func (f *FakeDocument) Len() int {
	return len(*f)
}

func (f *FakeDocument) EachField(vf VisitField) {
	for _, ff := range *f {
		vf(ff)
	}
}

type FakeField struct {
	N  string
	T  []*FakeTerm
	V  []byte
	S  bool
	DV bool
}

func NewFakeField(name, data string, store, termVec, docVals bool) *FakeField {
	rv := &FakeField{
		N:  name,
		V:  []byte(data),
		S:  store,
		DV: docVals,
	}

	// fake tokenize (split on space)
	terms := strings.Split(data, " ")

	// fake reducing repeated terms
	reduced := make(map[string][]*FakeLocation)
	var offset int
	for i, term := range terms {
		fl := &FakeLocation{
			F: "",
			P: i + 1,
			S: offset,
			E: offset + len(term),
		}
		offset += len(term) + 1
		reduced[term] = append(reduced[term], fl)
	}

	// build term list
	for term, locations := range reduced {
		ft := &FakeTerm{
			T: term,
			F: len(locations),
		}
		if termVec {
			ft.L = locations
		}
		rv.T = append(rv.T, ft)
	}

	return rv
}

func (f *FakeField) Name() string {
	return f.N
}

func (f *FakeField) Length() int {
	return len(f.T)
}

func (f *FakeField) EachTerm(vt VisitTerm) {
	for _, t := range f.T {
		vt(t)
	}
}

func (f *FakeField) Value() []byte {
	return f.V
}

func (f *FakeField) Store() bool {
	return f.S
}

func (f *FakeField) Index() bool {
	return true
}

func (f *FakeField) IndexDocValues() bool {
	return f.DV
}

type FakeTerm struct {
	T string
	F int
	L []*FakeLocation
}

func (f *FakeTerm) Term() []byte {
	return []byte(f.T)
}

func (f *FakeTerm) Frequency() int {
	return f.F
}

func (f *FakeTerm) EachLocation(vl VisitLocation) {
	for _, l := range f.L {
		vl(l)
	}
}

type FakeLocation struct {
	F string
	P int
	S int
	E int
}

func (f *FakeLocation) Field() string { return f.F }
func (f *FakeLocation) Pos() int      { return f.P }
func (f *FakeLocation) Start() int    { return f.S }
func (f *FakeLocation) End() int      { return f.E }
func (f *FakeLocation) Size() int     { return 0 }
