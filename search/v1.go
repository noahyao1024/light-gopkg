package search

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const v1IndexCapacity = 32

var (
	v1Indices      []*v1IndexWrapper
	v1IndexLock    *sync.RWMutex
	v1IndexMapping map[string]int
)

func init() {
	v1Indices = make([]*v1IndexWrapper, v1IndexCapacity)
	for i := 0; i < v1IndexCapacity; i++ {
		v1Indices[i] = &v1IndexWrapper{
			Naive: make(map[string]*V1Doc),
		}
	}

	v1IndexLock = &sync.RWMutex{}

	v1IndexMapping = make(map[string]int)
}

type v1IndexWrapper struct {
	Initialized bool              `json:"initialized"`
	Lock        *sync.RWMutex     `json:"lock"`
	Naive       map[string]*V1Doc `json:"naive"`
}

type V1Doc struct {
	ID         string                 `json:"_id"`
	SortableID int64                  `json:"_sortable_id"`
	Keywords   map[string]string      `json:"_keywords"`
	Source     map[string]interface{} `json:"_source"`
	Index      string                 `json:"_index"`
	ModifiedAt int64                  `json:"_modified_at"`
	CreatedAt  int64                  `json:"_created_at"`
}

// V1Request is the request of search v1
type V1Request struct {
	Query    *V1RequestQuery        `json:"query,omitempty"`
	Index    string                 `json:"index,omitempty"`
	From     int64                  `json:"from"`
	Size     int64                  `json:"size"`
	ID       string                 `json:"id,omitempty"`
	Keywords map[string]string      `json:"keywords,omitempty"`
	Source   map[string]interface{} `json:"source,omitempty"`
}

// V1Response is the response of search v1
type V1Response struct {
	Took int64          `json:"took"`
	Hits V1ResponseHits `json:"hits"`
}

type V1RequestQuery struct {
	RawAnds  []string                  `json:"raw,omitempty"`
	RawOrs   []string                  `json:"raw_ors,omitempty"`
	RegsAnd  map[string]*regexp.Regexp `json:"regs_and,omitempty"`
	RegsOr   map[string]*regexp.Regexp `json:"regs_or,omitempty"`
	Filters  map[string]string         `json:"filters,omitempty"`
	SortMode string                    `json:"sort_mode,omitempty"`
	SortBys  string                    `json:"sort_bys,omitempty"`
}

// Hits is the hits of search v1
type V1ResponseHits struct {
	From     int      `json:"from"`
	Size     int      `json:"size"`
	Total    int      `json:"total"`
	MaxScore int64    `json:"max_score"`
	Hits     []*V1Doc `json:"hits"`
}

// V1ResponseHit is the hit of search v1
type V1ResponseHit struct {
	ID         string                 `json:"_id"`
	Source     map[string]interface{} `json:"_source"`
	Score      int64                  `json:"_score"`
	Index      string                 `json:"_index"`
	Highlights []*V1ResponseHighlight `json:"_highlights"`
}

type V1ResponseHighlight struct {
	Field   string   `json:"field"`
	Offsets []string `json:"offsets"`
}

func V1Index(c *gin.Context, index string) error {
	// check if index exists
	if offset := V1GetIndexMapping(index); offset >= 0 {
		return nil
	}

	v1IndexLock.Lock()
	defer v1IndexLock.Unlock()

	// check if index exists again
	for i := 0; i < v1IndexCapacity; i++ {
		if !v1Indices[i].Initialized {
			v1Indices[i].Initialized = true
			v1Indices[i].Lock = &sync.RWMutex{}
			v1IndexMapping[index] = i
			return nil
		}
	}

	return fmt.Errorf("index capacity exceeded")
}

func V1GetIndexMapping(index string) int {
	v1IndexLock.RLock()
	defer v1IndexLock.RUnlock()

	if offset, found := v1IndexMapping[index]; found {
		return offset
	}

	return -1
}

func V1(ctx *gin.Context, request *V1Request) *V1Response {
	offset := V1GetIndexMapping(request.Index)
	if offset < 0 {
		return &V1Response{}
	}

	v1Indices[offset].Lock.RLock()
	defer v1Indices[offset].Lock.RUnlock()

	recalls := make([]*V1Doc, 0)

	for _, doc := range v1Indices[offset].Naive {
		matchedAndCount := 0
		matchedOrCount := 0

		matchedAnd := true
		matchedOr := true

		matchedFilter := true
		if len(request.Query.Filters) > 0 {
			matchedFilter = false
		}

		for k, v := range doc.Keywords {
			if reg := request.Query.RegsAnd[k]; reg != nil {
				if reg.MatchString(v) {
					matchedAndCount++
				}
			}

			if reg := request.Query.RegsOr[k]; reg != nil {
				if reg.MatchString(v) {
					matchedOrCount++
				}
			}

			if filter := request.Query.Filters[k]; len(filter) > 0 {
				filterBuckets := make(map[string]bool, 0)
				for _, f := range strings.Split(filter, ",") {
					filterBuckets[f] = true
				}

				if _, exists := filterBuckets[v]; exists {
					matchedFilter = true
				}
			}
		}

		if len(request.Query.RegsAnd) > 0 {
			matchedAnd = matchedAndCount == len(request.Query.RegsAnd)
		}

		if len(request.Query.RegsOr) > 0 {
			matchedOr = matchedOrCount > 0
		}

		if matchedAnd && matchedOr && matchedFilter {
			recalls = append(recalls, doc)
		}
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

	if request.From < 0 || request.From > int64(len(recalls)) {
		request.From = 0
	}

	if request.Size <= 0 || request.Size > 200 {
		request.Size = 10
	}

	response := &V1Response{
		Hits: V1ResponseHits{
			From:  int(request.From),
			Size:  int(request.Size),
			Total: len(recalls),
		},
	}

	if response.Hits.Total > 0 {
		if request.From+request.Size > int64(len(recalls)) {
			response.Hits.Hits = recalls[request.From:]
		} else {
			response.Hits.Hits = recalls[request.From : request.From+request.Size]
		}
	}

	return response
}

func V1Put(ctx *gin.Context, request *V1Request) error {
	offset := V1GetIndexMapping(request.Index)
	if offset < 0 {
		V1Index(ctx, request.Index)
		offset = V1GetIndexMapping(request.Index)
	}

	v1Indices[offset].Lock.Lock()
	defer v1Indices[offset].Lock.Unlock()

	// Merge keywords into source
	if request.Source == nil {
		request.Source = make(map[string]interface{})
	}

	for k, v := range request.Keywords {
		request.Source[k] = v
	}

	sortableID, _ := strconv.ParseInt(request.ID, 10, 64)
	if sortableID == 0 {
		sortableID = time.Now().UnixNano()
	}

	v1Indices[offset].Naive[request.ID] = &V1Doc{
		ID:         request.ID,
		SortableID: sortableID,
		Keywords:   request.Keywords,
		Source:     request.Source,
		Index:      request.Index,
		ModifiedAt: time.Now().Unix(),
	}

	return nil
}

func V1Reset(ctx *gin.Context, index string) string {
	offset := V1GetIndexMapping(index)
	if offset < 0 {
		return "Index not found"
	}

	v1Indices[offset].Lock.Lock()
	defer v1Indices[offset].Lock.Unlock()

	v1Indices[offset].Naive = make(map[string]*V1Doc)

	return "OK"
}

func V1Peak(ctx *gin.Context, index string) map[string]interface{} {
	offset := V1GetIndexMapping(index)
	if offset < 0 {
		return map[string]interface{}{
			"message": "Index not found",
		}
	}

	v1Indices[offset].Lock.RLock()
	defer v1Indices[offset].Lock.RUnlock()

	return map[string]interface{}{
		"index":       index,
		"initialized": v1Indices[offset].Initialized,
		"total":       len(v1Indices[offset].Naive),
	}
}
