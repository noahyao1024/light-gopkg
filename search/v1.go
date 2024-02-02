package search

import (
	"fmt"
	"regexp"
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
	From     int64                  `json:"from,omitempty"`
	Size     int64                  `json:"size,omitempty"`
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
	Raw  string                    `json:"raw,omitempty"`
	Regs map[string]*regexp.Regexp `json:"regs,omitempty"`
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
		for k, v := range doc.Keywords {
			if reg := request.Query.Regs[k]; reg != nil {
				if reg.MatchString(v) {
					recalls = append(recalls, doc)
				}
			}
		}
	}

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

	v1Indices[offset].Naive[request.ID] = &V1Doc{
		ID:         request.ID,
		Keywords:   request.Keywords,
		Source:     request.Source,
		Index:      request.Index,
		ModifiedAt: time.Now().Unix(),
	}

	return nil
}
