package iot

import (
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// TrucksWithLongDailySession contains info for filling in trucks with longer driving session queries.
type TrucksWithLongDailySession struct {
	core utils.QueryGenerator
}

// NewTruckWithLongDailySession creates a new trucks with longer driving session query filler.
func NewTruckWithLongDailySession(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLongDailySession{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *TrucksWithLongDailySession) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(TruckLongDailySessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLongDailySessions(q, zipNum, latestNum, newOrOld)
	return q
}
