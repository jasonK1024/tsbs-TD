package tdengine_client

import (
	"fmt"
	"math"
	"regexp"
	"strings"
)

// RemainQueryString 根据 cache 返回结果中的时间范围构造一个剩余查询语句
func RemainQueryString(queryString string, queryTemplate string, flagArr []uint8, timeRangeArr [][]int64, tagArr [][]string) (string, int64, int64) {
	if len(flagArr) == 0 || len(timeRangeArr) == 0 {
		return "", 0, 0
	}

	var maxTime int64 = 0
	var minTime int64 = math.MaxInt64

	// select 语句	SELECT _wstart as ts,tbname,avg(latitude),avg(longitude) FROM readings
	matchStr := `(?i)(.+)WHERE.+`
	conditionExpr := regexp.MustCompile(matchStr)
	if ok, _ := regexp.MatchString(matchStr, queryString); !ok {
		return "", 0, 0
	}
	condExprMatch := conditionExpr.FindStringSubmatch(queryString)
	selectExpr := condExprMatch[1]

	// 聚合分类	 tbname INTERVAL(%s) order by tbname,ts
	matchStr = `(?i)partition by `
	conditionExpr = regexp.MustCompile(matchStr)
	condExprMatch = conditionExpr.FindStringSubmatch(queryString)
	AggrExpr := condExprMatch[1]

	// 谓词条件
	predicate := ""
	if !strings.Contains(strings.ToLower(queryTemplate), "interval") {
		predicate = " "
		startIdx := strings.Index(queryTemplate, "AND")
		endIdx := strings.Index(queryTemplate, "ts")
		predicateClause := queryTemplate[startIdx:endIdx]
		predicateClause = predicateClause[:len(predicate)-4]
	}

	selects := make([]string, 0)
	for i := 0; i < len(flagArr); i++ {
		if flagArr[i] == 1 {
			if minTime > timeRangeArr[i][0] {
				minTime = timeRangeArr[i][0]
			}
			if maxTime < timeRangeArr[i][1] {
				maxTime = timeRangeArr[i][1]
			}
			tmpCondition := ""
			tmpCondition = fmt.Sprintf("%sWHERE %s = '%s'%s AND ts >= %d AND ts < %d partition by %s", selectExpr, tagArr[i][0], tagArr[i][1], predicate, timeRangeArr[i][0], timeRangeArr[i][1], AggrExpr)

			selects = append(selects, tmpCondition)
		}
	}
	remainQuerys := strings.Join(selects, " UNION ALL ")

	return remainQuerys, minTime, maxTime
}
