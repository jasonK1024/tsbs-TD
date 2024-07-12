package iot

import (
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

type IoTQueries struct {
	core utils.QueryGenerator
}

func NewIoTQueries(core utils.QueryGenerator) utils.QueryFiller {
	return &IoTQueries{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *IoTQueries) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(IoTQueriesFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.IoTQueries(q, zipNum, latestNum, newOrOld)
	return q
}
