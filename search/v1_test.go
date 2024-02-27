package search

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/collate"
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

	response := V1(nil, &V1Request{Index: index, Query: &V1RequestQuery{}})

	if assert.Equal(t, true, response.Hits.Total > 0) {
		assert.Equal(t, "123", response.Hits.Hits[0])
	}
}

func TestV2(t *testing.T) {
	request := &V1Request{
		Query: &V1RequestQuery{
			SortBys:  "name,description",
			SortMode: "desc",
		},
	}

	var recalls []*V1Doc

	recalls = []*V1Doc{
		{
			SortableID: 1,
			Keywords: map[string]string{
				"name":        "a",
				"description": "2",
			},
		},
		{
			SortableID: 2,

			Keywords: map[string]string{
				"name":        "a",
				"description": "1",
			},
		},
	}

	sort.SliceStable(recalls, func(i, j int) bool {
		for _, sortBy := range strings.Split(request.Query.SortBys, ",") {
			vi := recalls[i].Keywords[sortBy]
			vj := recalls[j].Keywords[sortBy]

			if vi == vj {
				continue
			}

			if request.Query.SortMode == "asc" {
				return vi < vj
			}

			return vi > vj
		}

		if request.Query.SortMode == "asc" {
			return recalls[i].SortableID < recalls[j].SortableID
		}

		return recalls[i].SortableID > recalls[j].SortableID
	})

	assert.Equal(t, int64(1), recalls[0].SortableID)
}

func TestCollate(t *testing.T) {
	collateLess := collate.IndexString("ZH-HANS_CI")

	candidates := []string{"姚", "明", "啊"}

	// Ascending
	sort.SliceStable(candidates, func(i, j int) bool {
		return collateLess(candidates[i], candidates[j])
	})
	assert.Equal(t, []string{"啊", "明", "姚"}, candidates)

	// Descending
	sort.SliceStable(candidates, func(i, j int) bool {
		return !collateLess(candidates[i], candidates[j])
	})
	assert.Equal(t, []string{"姚", "明", "啊"}, candidates)
}
