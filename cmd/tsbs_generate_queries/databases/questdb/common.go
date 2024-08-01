package questdb

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for QuestDB
type BaseGenerator struct {
}

//var RandomTag bool = true
//var TagNum int = 10

// GenerateEmptyQuery returns an empty query.QuestDB.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	sss := strings.Split(sql, ";")
	sql = sss[0]

	v := url.Values{}
	v.Set("count", "false")
	v.Set("query", sql)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.RawQuery = []byte(sql)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/exec?%s", v.Encode()))
	q.Body = nil
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
