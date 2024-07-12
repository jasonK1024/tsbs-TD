package iot

import (
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// StationaryTrucks contains info for filling in stationary trucks queries.
type StationaryTrucks struct {
	core utils.QueryGenerator
}

// NewStationaryTrucks creates a new stationary trucks query filler.
func NewStationaryTrucks(core utils.QueryGenerator) utils.QueryFiller {
	return &StationaryTrucks{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *StationaryTrucks) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(StationaryTrucksFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.StationaryTrucks(q, zipNum, latestNum, newOrOld)
	return q
}
