package devops

import (
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// GroupByOrderByLimit produces a filler for queries in the devops groupby-orderby-limit case.
type GroupByOrderByLimit struct {
	core utils.QueryGenerator
}

// NewGroupByOrderByLimit returns a new GroupByOrderByLimit for given paremeters
func NewGroupByOrderByLimit(core utils.QueryGenerator) utils.QueryFiller {
	return &GroupByOrderByLimit{core}
}

// Fill fills in the query.Query with query details
func (d *GroupByOrderByLimit) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(GroupbyOrderbyLimitFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.GroupByOrderByLimit(q)
	return q
}
