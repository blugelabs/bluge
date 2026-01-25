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
	"math/rand"
	"strings"
	"time"

	"github.com/blugelabs/bluge"
)

// BenchDoc represents a synthetic Wikipedia-like document for benchmarking.
type BenchDoc struct {
	ID         string
	Title      string
	Body       string
	Categories []string
	Author     string
	ViewCount  int
	Timestamp  time.Time
}

// Vocabulary with controlled frequency distribution
var (
	// High frequency terms (50 terms) - appear in 30-50% of docs
	highFreqTerms = []string{
		"information", "article", "system", "world", "time",
		"people", "year", "state", "work", "part",
		"first", "number", "water", "called", "made",
		"found", "used", "many", "years", "history",
		"also", "city", "area", "country", "name",
		"national", "government", "united", "american", "public",
		"life", "high", "school", "company", "group",
		"family", "general", "war", "order", "point",
		"place", "home", "local", "small", "large",
		"known", "development", "power", "form", "level",
	}

	// Medium frequency terms (500 terms) - appear in 1-10% of docs
	mediumFreqTerms = []string{
		"technology", "research", "university", "science", "political",
		"economic", "international", "social", "military", "cultural",
		"president", "member", "population", "significant", "important",
		"modern", "ancient", "western", "southern", "northern",
		"eastern", "central", "european", "african", "asian",
		"british", "french", "german", "spanish", "italian",
		"russian", "chinese", "japanese", "indian", "canadian",
		"australian", "brazilian", "mexican", "dutch", "swedish",
		"music", "film", "television", "radio", "media",
		"computer", "software", "internet", "network", "digital",
		"energy", "environment", "climate", "ocean", "mountain",
		"river", "island", "forest", "desert", "valley",
		"building", "architecture", "design", "construction", "engineering",
		"medicine", "health", "disease", "hospital", "treatment",
		"education", "student", "teacher", "college", "degree",
		"business", "market", "industry", "trade", "financial",
		"religion", "church", "christian", "muslim", "jewish",
		"philosophy", "theory", "concept", "principle", "method",
		"language", "literature", "writing", "poetry", "novel",
		"sport", "football", "basketball", "baseball", "olympic",
		"transportation", "railway", "airport", "highway", "vehicle",
		"agriculture", "farming", "crop", "livestock", "harvest",
		"geography", "region", "territory", "province", "district",
		"mathematics", "physics", "chemistry", "biology", "astronomy",
		"democracy", "election", "parliament", "constitution", "legislation",
		"museum", "gallery", "exhibition", "collection", "artifact",
		"festival", "celebration", "tradition", "ceremony", "ritual",
		"restaurant", "cuisine", "recipe", "ingredient", "cooking",
		"fashion", "clothing", "textile", "designer", "brand",
		"tourism", "vacation", "destination", "attraction", "resort",
		"immigration", "migration", "population", "census", "demographic",
		"infrastructure", "utility", "facility", "resource", "supply",
		"conservation", "wildlife", "species", "habitat", "ecosystem",
		"journalism", "newspaper", "magazine", "publication", "editor",
		"photography", "camera", "image", "portrait", "landscape",
		"sculpture", "painting", "drawing", "canvas", "gallery",
		"orchestra", "symphony", "concert", "composer", "musician",
		"theater", "drama", "performance", "actor", "director",
		"animation", "cartoon", "illustration", "graphic", "visual",
		"programming", "algorithm", "database", "interface", "protocol",
		"encryption", "security", "authentication", "privacy", "firewall",
		"satellite", "spacecraft", "rocket", "orbit", "mission",
		"archaeology", "excavation", "artifact", "civilization", "ancient",
		"genetics", "genome", "chromosome", "mutation", "inheritance",
		"psychology", "behavior", "cognitive", "emotion", "perception",
		"sociology", "community", "society", "institution", "organization",
		"economics", "inflation", "recession", "currency", "investment",
		"anthropology", "ethnicity", "culture", "tribe", "indigenous",
		"linguistics", "grammar", "syntax", "vocabulary", "phonetics",
		"meteorology", "weather", "forecast", "temperature", "precipitation",
	}

	// Low frequency terms (generated dynamically) - appear in <0.1% of docs
	lowFreqPrefixes = []string{
		"xeno", "quasi", "pseudo", "proto", "meta",
		"ultra", "hyper", "micro", "macro", "nano",
	}
	lowFreqRoots = []string{
		"morphic", "plastic", "graphic", "logic", "metric",
		"phobic", "philic", "genic", "tropic", "static",
	}

	// Categories (100 categories)
	categories = []string{
		"Science", "Technology", "Mathematics", "Physics", "Chemistry",
		"Biology", "Medicine", "Engineering", "Computer Science", "Astronomy",
		"Geography", "History", "Politics", "Economics", "Philosophy",
		"Religion", "Art", "Music", "Literature", "Film",
		"Sports", "Education", "Business", "Law", "Military",
		"Transportation", "Architecture", "Agriculture", "Environment", "Energy",
		"Health", "Food", "Fashion", "Tourism", "Media",
		"Entertainment", "Gaming", "Animals", "Plants", "Geology",
		"Oceanography", "Meteorology", "Archaeology", "Anthropology", "Sociology",
		"Psychology", "Linguistics", "Statistics", "Ecology", "Genetics",
		"Neuroscience", "Pharmacology", "Biochemistry", "Microbiology", "Zoology",
		"Botany", "Paleontology", "Cosmology", "Quantum Physics", "Thermodynamics",
		"Mechanics", "Optics", "Acoustics", "Electromagnetism", "Relativity",
		"Calculus", "Algebra", "Geometry", "Number Theory", "Probability",
		"Cryptography", "Robotics", "Artificial Intelligence", "Machine Learning", "Data Science",
		"Networking", "Cybersecurity", "Cloud Computing", "Web Development", "Mobile Apps",
		"Ancient History", "Medieval History", "Modern History", "World War", "Cold War",
		"Renaissance", "Industrial Revolution", "Civil Rights", "Colonialism", "Imperialism",
		"Democracy", "Communism", "Capitalism", "Socialism", "Nationalism",
		"Buddhism", "Christianity", "Islam", "Hinduism", "Judaism",
	}

	// Author names (1000 generated from first/last name combinations)
	firstNames = []string{
		"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
		"William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
		"Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
		"Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
		"Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
		"Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
		"Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon",
		"Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
	}
	lastNames = []string{
		"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
		"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
		"Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
		"Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
		"Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
		"Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
		"Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
		"Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
	}
)

// DocGenerator generates synthetic documents with controlled vocabulary distribution.
type DocGenerator struct {
	rng         *rand.Rand
	authors     []string
	docCount    int
	baseTime    time.Time
	lowFreqPool []string
}

// NewDocGenerator creates a new document generator with the given seed.
func NewDocGenerator(seed int64) *DocGenerator {
	rng := rand.New(rand.NewSource(seed))

	// Pre-generate author names
	authors := make([]string, 0, len(firstNames)*len(lastNames))
	for _, first := range firstNames {
		for _, last := range lastNames {
			authors = append(authors, first+" "+last)
		}
	}

	// Pre-generate low frequency terms
	lowFreqPool := make([]string, 0, 5000)
	for i := 0; i < 500; i++ {
		for _, prefix := range lowFreqPrefixes {
			for _, root := range lowFreqRoots {
				lowFreqPool = append(lowFreqPool, fmt.Sprintf("%s%s%d", prefix, root, i))
			}
		}
	}

	return &DocGenerator{
		rng:         rng,
		authors:     authors,
		baseTime:    time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		lowFreqPool: lowFreqPool,
	}
}

// Generate creates a synthetic document.
func (g *DocGenerator) Generate() BenchDoc {
	g.docCount++

	doc := BenchDoc{
		ID:         fmt.Sprintf("doc_%08d", g.docCount),
		Title:      g.generateTitle(),
		Body:       g.generateBody(),
		Categories: g.generateCategories(),
		Author:     g.authors[g.rng.Intn(len(g.authors))],
		ViewCount:  g.rng.Intn(1000001), // 0 to 1,000,000
		Timestamp:  g.generateTimestamp(),
	}

	return doc
}

func (g *DocGenerator) generateTitle() string {
	// Title: 5-15 words
	wordCount := 5 + g.rng.Intn(11)
	words := make([]string, wordCount)
	for i := 0; i < wordCount; i++ {
		words[i] = g.selectWord(0.6, 0.3, 0.1)
	}
	// Capitalize first letter
	if len(words) > 0 && len(words[0]) > 0 {
		words[0] = strings.ToUpper(words[0][:1]) + words[0][1:]
	}
	return strings.Join(words, " ")
}

func (g *DocGenerator) generateBody() string {
	// Body: 200-500 words
	wordCount := 200 + g.rng.Intn(301)
	words := make([]string, wordCount)
	for i := 0; i < wordCount; i++ {
		words[i] = g.selectWord(0.5, 0.35, 0.15)
	}

	// Format into sentences (roughly 10-20 words per sentence)
	var sentences []string
	start := 0
	for start < len(words) {
		sentenceLen := 10 + g.rng.Intn(11)
		if start+sentenceLen > len(words) {
			sentenceLen = len(words) - start
		}
		sentence := words[start : start+sentenceLen]
		if len(sentence) > 0 {
			// Capitalize first word
			sentence[0] = strings.ToUpper(sentence[0][:1]) + sentence[0][1:]
			sentences = append(sentences, strings.Join(sentence, " ")+".")
		}
		start += sentenceLen
	}

	return strings.Join(sentences, " ")
}

func (g *DocGenerator) selectWord(highProb, medProb, _ float64) string {
	r := g.rng.Float64()
	if r < highProb {
		return highFreqTerms[g.rng.Intn(len(highFreqTerms))]
	} else if r < highProb+medProb {
		return mediumFreqTerms[g.rng.Intn(len(mediumFreqTerms))]
	}
	return g.lowFreqPool[g.rng.Intn(len(g.lowFreqPool))]
}

func (g *DocGenerator) generateCategories() []string {
	// 1-5 categories
	count := 1 + g.rng.Intn(5)
	cats := make([]string, count)
	used := make(map[int]bool)
	for i := 0; i < count; i++ {
		for {
			idx := g.rng.Intn(len(categories))
			if !used[idx] {
				used[idx] = true
				cats[i] = categories[idx]
				break
			}
		}
	}
	return cats
}

func (g *DocGenerator) generateTimestamp() time.Time {
	// Spread over 25 years from baseTime
	days := g.rng.Intn(25 * 365)
	return g.baseTime.AddDate(0, 0, days)
}

// ToDocument converts a BenchDoc to a bluge.Document.
func (d BenchDoc) ToDocument() *bluge.Document {
	doc := bluge.NewDocument(d.ID)

	// Title: text field with term vectors, stored
	doc.AddField(bluge.NewTextField("title", d.Title).
		StoreValue().
		HighlightMatches())

	// Body: text field with term vectors (for phrase queries)
	doc.AddField(bluge.NewTextField("body", d.Body).
		HighlightMatches())

	// Categories: keyword fields, stored
	for _, cat := range d.Categories {
		doc.AddField(bluge.NewKeywordField("category", cat).StoreValue())
	}

	// Author: keyword field
	doc.AddField(bluge.NewKeywordField("author", d.Author))

	// ViewCount: numeric field
	doc.AddField(bluge.NewNumericField("viewCount", float64(d.ViewCount)))

	// Timestamp: datetime field
	doc.AddField(bluge.NewDateTimeField("timestamp", d.Timestamp))

	return doc
}
