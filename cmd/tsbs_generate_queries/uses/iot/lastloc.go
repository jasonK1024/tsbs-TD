package iot

import (
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// LastLocPerTruck contains info for filling in last location queries.
type LastLocPerTruck struct {
	core utils.QueryGenerator
}

// NewLastLocPerTruck creates a new last location query filler.
func NewLastLocPerTruck(core utils.QueryGenerator) utils.QueryFiller {
	return &LastLocPerTruck{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *LastLocPerTruck) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(LastLocFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.LastLocPerTruck(q, zipNum, latestNum, newOrOld)
	return q
}
