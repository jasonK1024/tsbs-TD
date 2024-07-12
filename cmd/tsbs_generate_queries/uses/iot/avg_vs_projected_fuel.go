package iot

import (
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// AvgVsProjectedFuelConsumption contains info for filling in avg vs projected fuel consumption queries.
type AvgVsProjectedFuelConsumption struct {
	core utils.QueryGenerator
}

// NewAvgVsProjectedFuelConsumption creates a new avg vs projected fuel consumption query filler.
func NewAvgVsProjectedFuelConsumption(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgVsProjectedFuelConsumption{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *AvgVsProjectedFuelConsumption) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(AvgVsProjectedFuelConsumptionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgVsProjectedFuelConsumption(q, zipNum, latestNum, newOrOld)
	return q
}
