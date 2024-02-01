package search

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestV1(t *testing.T) {
	index := "hello"

	V1Index(nil, index)
	V1Index(nil, index)
	V1Index(nil, index)
	V1Index(nil, index)
	V1Index(nil, index)
	V1Index(nil, "s")
	V1Index(nil, "sss")

	V1Put(nil, &V1Request{
		Index: index,
		ID:    "123",
		Keywords: map[string]string{
			"hello": "world hello world",
		},
		Source: map[string]interface{}{
			"nothing": "here",
		},
	})

	response := V1(nil, &V1Request{Index: index, Query: &V1RequestQuery{Raw: "orl", Reg: regexp.MustCompile(`.+rl`)}})

	if assert.Equal(t, true, response.Hits.Total > 0) {
		assert.Equal(t, "123", response.Hits.Hits[0])
	}
}
