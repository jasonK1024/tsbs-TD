package tdengine_client

import (
	"database/sql/driver"
	"fmt"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

func MergeRemainResponse(convResp []*async.ExecResult, remainResp *async.ExecResult, timeRangeArr [][]int64) []*async.ExecResult {
	if len(convResp) == 0 {
		return []*async.ExecResult{remainResp}
	}

	if ResponseIsEmpty(remainResp) {
		return convResp
	}

	var results []*async.ExecResult

	index1 := 0
	index2 := 0
	for index1 < len(convResp) && index2 < len(remainResp.Data) {
		if len(convResp[index1].Data) == 0 {
			index1++
			continue
		}
		tag1 := convResp[index1].Data[0][1].(string)
		tag2 := remainResp.Data[index2][1].(string)

		cmp := strings.Compare(tag1, tag2)

		if cmp == -1 { // 存入 resp1
			results = append(results, convResp[index1])
			index1++
		} else if cmp == 1 { // 存入 resp2
			var data [][]driver.Value
			for index2 < len(remainResp.Data) {
				tagT := remainResp.Data[index2][1].(string)
				if tagT == tag2 {
					data = append(data, remainResp.Data[index2])
					index2++
				} else {
					break
				}
			}
			tmpResp := &async.ExecResult{
				AffectedRows: 0,
				FieldCount:   len(remainResp.Data[0]),
				Header:       convResp[0].Header,
				Data:         data,
			}
			results = append(results, tmpResp)
		} else { // 合并 resp1,resp2
			data, offset := mergeResp(convResp[index1].Data, remainResp.Data[index2:], timeRangeArr[index1])
			tmpResp := &async.ExecResult{
				AffectedRows: 0,
				FieldCount:   len(remainResp.Data[0]),
				Header:       convResp[0].Header,
				Data:         data,
			}
			results = append(results, tmpResp)
			index1++
			index2 += offset
		}

	}

	for index1 < len(convResp) {
		results = append(results, convResp[index1])
		index1++
	}

	for index2 < len(remainResp.Data) {
		var data [][]driver.Value
		tag2 := remainResp.Data[index2][1].(string)
		for index2 < len(remainResp.Data) {
			tagT := remainResp.Data[index2][1].(string)
			if tagT == tag2 {
				data = append(data, remainResp.Data[index2])
				index2++
			} else {
				break
			}
		}
		tmpResp := &async.ExecResult{
			AffectedRows: 0,
			FieldCount:   len(remainResp.Data[0]),
			Header:       convResp[0].Header,
			Data:         data,
		}
		results = append(results, tmpResp)
	}

	return results
}

func mergeResp(series1, series2 [][]driver.Value, timeRange []int64) ([][]driver.Value, int) {
	//series2 = searchInsertValue(series2, timeRange)

	st1, et1, err1 := GetSeriesTimeRange(series1)
	if err1 != nil {
		return series2, len(series2)
	}
	//st2, et2, err2 := GetSeriesTimeRange(series2)
	//if err2 != nil {
	//	return series1, 0
	//}

	st2 := timeRange[0]
	et2 := timeRange[1]

	index := 0
	if st2 > et1 { // 2 合并到 1 后面
		tag1 := series1[0][1].(string)
		for index < len(series2) {
			tag2 := series2[index][1].(string)
			if tag1 == tag2 {
				series1 = append(series1, series2[index])
				index++
			} else {
				break
			}
		}

		return series1, index
	} else if st1 > et2 { // 1 合并到 2 后面
		tag1 := series1[0][1].(string)
		tmpSer := make([][]driver.Value, 0)
		for index < len(series2) {
			tag2 := series2[index][1].(string)
			if tag1 == tag2 {
				tmpSer = append(tmpSer, series2[index])
				index++
			} else {
				break
			}
		}
		tmpSer = append(tmpSer, series1...)

		return tmpSer, index
	} else if st1 < st2 {
		tmpSeries := make([][]driver.Value, 0)
		insPos := searchInsertPosition(series1, series2)
		// 1 的前半部分
		for i := 0; i < insPos; i++ {
			tmpSeries = append(tmpSeries, series1[i])
		}
		// 2 插入
		tag1 := series1[0][1].(string)
		for i := 0; i < len(series2); i++ {
			tag2 := series2[i][1].(string)
			if tag1 == tag2 {
				tmpSeries = append(tmpSeries, series2[i])
				index++
			} else {
				break
			}
		}
		// 1 的后半部分
		for i := insPos; i < len(series1); i++ {
			tmpSeries = append(tmpSeries, series1[i])
		}
		return tmpSeries, index
	} else {
		tmpSeries := make([][]driver.Value, 0)
		insPos := searchInsertPosition(series2, series1)
		tag1 := series1[0][1].(string)
		// 2 的前半部分
		for i := 0; i < insPos; i++ {
			tag2 := series2[i][1].(string)
			if tag1 == tag2 {
				tmpSeries = append(tmpSeries, series2[i])
				index++
			} else {
				break
			}
		}
		// 1 插入
		for i := 0; i < len(series1); i++ {
			tmpSeries = append(tmpSeries, series1[i])
		}
		// 2 的后半部分
		for i := insPos; i < len(series2); i++ {
			tag2 := series2[i][1].(string)
			if tag1 == tag2 {
				tmpSeries = append(tmpSeries, series2[i])
				index++
			} else {
				break
			}
		}
		return tmpSeries, index
	}

}

func GetSeriesTimeRange(series [][]driver.Value) (int64, int64, error) {
	if len(series) == 0 {
		return 0, 0, fmt.Errorf("empty series")
	}

	stime := reflect.ValueOf(series[0][0]).Int()
	etime := reflect.ValueOf(series[len(series)-1][0]).Int()

	return stime, etime, nil
}

// searchInsertPosition 找到 val2 应该插入到 val1 中的位置
func searchInsertPosition(values1, values2 [][]driver.Value) int {
	index := 0

	// val2 的起始时间
	if len(values2) == 0 || len(values1) == 0 {
		return 0
	}

	st2 := reflect.ValueOf(values2[0][0]).Int()

	//loop := 0
	left := 0
	right := len(values1) - 1
	for left < right {
		mid := (left + right) / 2

		//loop++
		st1 := reflect.ValueOf(values1[mid][0]).Int()

		if st1 < st2 {
			index = mid
			left = mid + 1
		} else if st1 > st2 {
			index = mid
			right = mid - 1
		} else {
			index = mid
			return index
		}

		//if loop >= 15 {
		//	break
		//}
	}

	return index
}

// 用 cache 返回的剩余时间范围去掉剩余查询结果中的重复数据，首尾都可能重复
func searchInsertValue(values [][]driver.Value, timeRange []int64) [][]driver.Value {
	if len(values) == 0 || timeRange[0] == 0 || timeRange[1] == 0 {
		return nil
	}

	startTime := timeRange[0]
	endTime := timeRange[1]

	//loop := 0
	sIdx := 0
	left := 0
	right := len(values) - 1
	for left < right {
		mid := (left + right) / 2

		//loop++
		st1 := reflect.ValueOf(values[mid][0]).Int()

		if st1 < startTime {
			sIdx = mid
			left = mid + 1
		} else if st1 > startTime {
			sIdx = mid
			right = mid - 1
		} else {
			sIdx = mid
			break
		}

		//if loop >= 15 {
		//	break
		//}
	}

	//loop = 0
	eIdx := len(values) - 1
	left = 0
	right = len(values) - 1
	for left < right {
		mid := (left + right) / 2

		//loop++
		et1 := reflect.ValueOf(values[mid][0]).Int()

		if et1 > endTime {
			eIdx = mid
			right = mid - 1
		} else if et1 < endTime {
			eIdx = mid
			left = mid + 1
		} else {
			eIdx = mid
			break
		}

		//if loop >= 15 {
		//	break
		//}
	}

	if sIdx > eIdx {
		return nil
	}

	return values[sIdx : eIdx+1]
}

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

	// 聚合分类	 partition by tbname INTERVAL(%s) order by tbname,ts
	matchStr = `(?i)partition by (.+) order by `
	conditionExpr = regexp.MustCompile(matchStr)
	condExprMatch = conditionExpr.FindStringSubmatch(strings.ToLower(queryString))
	AggrExpr := condExprMatch[1]

	// order 排序
	//matchStr = `(?i)order by (.+)`
	//conditionExpr = regexp.MustCompile(matchStr)
	//condExprMatch = conditionExpr.FindStringSubmatch(strings.ToLower(queryString))
	//orderExpr := condExprMatch[1]

	// 谓词条件
	predicate := ""
	if !strings.Contains(strings.ToLower(queryTemplate), "interval") {
		predicate += " "
		startIdx := strings.Index(queryTemplate, "AND")
		endIdx := strings.Index(queryTemplate, "ts >=")
		predicateClause := queryTemplate[startIdx:endIdx]
		predicate += predicateClause[:len(predicateClause)-4]
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
			//tmpCondition = fmt.Sprintf("%sWHERE %s = '%s'%s AND ts >= %d AND ts < %d partition by %s", selectExpr, tagArr[i][0], tagArr[i][1], predicate, timeRangeArr[i][0], timeRangeArr[i][1], AggrExpr)
			tmpCondition = fmt.Sprintf("%sWHERE tbname = '%s'%s AND ts >= %d AND ts < %d partition by %s", selectExpr, tagArr[i][1], predicate, timeRangeArr[i][0], timeRangeArr[i][1], AggrExpr)

			selects = append(selects, tmpCondition)
		}
	}
	remainQuery := strings.Join(selects, " UNION ALL ")
	//remainQuerys += fmt.Sprintf(" order by %s", orderExpr)
	remainQuery += " order by tbname,ts"
	//tagName := tagArr[0][0]
	//remainQuery = strings.ReplaceAll(remainQuery, "tbname", tagName)

	return remainQuery, minTime, maxTime
}

func RemainQuery(queryTemplate string, flagArr []uint8, timeRangeArr [][]int64, tagArr [][]string) (string, int64, int64) {
	if len(flagArr) == 0 || len(timeRangeArr) == 0 {
		return "", 0, 0
	}

	var maxTime int64 = 0
	var minTime int64 = math.MaxInt64

	for i := 0; i < len(flagArr); i++ {
		if flagArr[i] == 1 {
			if minTime > timeRangeArr[i][0] {
				minTime = timeRangeArr[i][0]
			}
			if maxTime < timeRangeArr[i][1] {
				maxTime = timeRangeArr[i][1]
			}
		}
	}

	tagClause := ""
	tags := make([]string, 0)
	for _, tag := range tagArr {
		tags = append(tags, fmt.Sprintf("'%s'", tag[1]))
	}
	tagClause = strings.Join(tags, ",")

	startTime := strconv.FormatInt(minTime, 10)
	endTime := strconv.FormatInt(maxTime, 10)
	queryTemplate = strings.Replace(queryTemplate, "?", tagClause, 1)
	queryTemplate = strings.Replace(queryTemplate, "?", startTime, 1)
	queryTemplate = strings.Replace(queryTemplate, "?", endTime, 1)

	minTime /= 1000
	maxTime /= 1000
	return queryTemplate, minTime, maxTime
}
