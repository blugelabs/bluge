package bluge

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/blugelabs/bluge/index"
)

func TestInMemoryWriterDataRace(t *testing.T) {
	cfg := InMemoryOnlyConfig()
	w, err := OpenWriter(cfg)
	if err != nil {
		t.Fatalf("unable to open in memory writer: %+v", err)
	}
	for i := 0; i < 5; i++ {
		b := batchAddDocs(2)
		err = w.Batch(b)
		if err != nil {
			t.Fatalf("failed to add random docs: %+v", err)
		}
	}
}

func batchAddDocs(docCount int) *index.Batch {
	batch := NewBatch()

	for i := 0; i < docCount; i++ {
		doc := randomDoc()
		batch.Update(doc.ID(), doc)
	}
	return batch
}

var (
	field1 = randStr()
	field2 = randStr()
)

func randomDoc() *Document {
	return NewDocument(randStr()).
		AddField(NewTextField(field1, randStr())).
		AddField(NewTextField(field2, randStr()))
}

const charset = "01234567890abcdefghijklmnopqrstuvwxyz<>{}[];'"
const maxRandStrLen = 30

func randStrn(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(charset[rand.Intn(len(charset))])
	}

	return b.String()
}

func randStr() string {
	return randStrn(maxRandStrLen)
}
