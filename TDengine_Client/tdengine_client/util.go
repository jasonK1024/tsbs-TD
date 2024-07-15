package tdengine_client

import (
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TDengine 数据类型
const (
	TSDB_DATA_TYPE_NULL       = 0  // 1 bytes
	TSDB_DATA_TYPE_BOOL       = 1  // 1 bytes
	TSDB_DATA_TYPE_TINYINT    = 2  // 1 byte
	TSDB_DATA_TYPE_SMALLINT   = 3  // 2 bytes
	TSDB_DATA_TYPE_INT        = 4  // 4 bytes
	TSDB_DATA_TYPE_BIGINT     = 5  // 8 bytes
	TSDB_DATA_TYPE_FLOAT      = 6  // 4 bytes
	TSDB_DATA_TYPE_DOUBLE     = 7  // 8 bytes
	TSDB_DATA_TYPE_BINARY     = 8  // string
	TSDB_DATA_TYPE_TIMESTAMP  = 9  // 8 bytes
	TSDB_DATA_TYPE_NCHAR      = 10 // unicode string
	TSDB_DATA_TYPE_UTINYINT   = 11 // 1 byte
	TSDB_DATA_TYPE_USMALLINT  = 12 // 2 bytes
	TSDB_DATA_TYPE_UINT       = 13 // 4 bytes
	TSDB_DATA_TYPE_UBIGINT    = 14 // 8 bytes
	TSDB_DATA_TYPE_JSON       = 15
	TSDB_DATA_TYPE_VARBINARY  = 16
	TSDB_DATA_TYPE_DECIMAL    = 17
	TSDB_DATA_TYPE_BLOB       = 18
	TSDB_DATA_TYPE_MEDIUMBLOB = 19
	TSDB_DATA_TYPE_GEOMETRY   = 20
)

func ResponseIsEmpty(data *async.ExecResult) bool {
	return len(data.Data) == 0 || data.FieldCount == 0 || len(data.Header.ColTypes) == 0 || len(data.Header.ColNames) == 0 || len(data.Header.ColLength) == 0
}

// GetQueryTemplate 取出时间范围和 tag，替换为查询模版
func GetQueryTemplate(queryString string) (string, int64, int64, []string) {
	var startTime int64
	var endTime int64
	var tags []string

	/* 替换时间 */
	// 1640995320000
	timeReg := regexp.MustCompile("[0-9]{13}")
	replacement := "?"

	times := timeReg.FindAllString(queryString, -1)
	if len(times) == 0 {
		startTime = 0
		endTime = 0
	} else if len(times) == 1 {
		startTime, _ = strconv.ParseInt(times[0], 10, 64)
		endTime = startTime
	} else {
		startTime, _ = strconv.ParseInt(times[0], 10, 64)
		endTime, _ = strconv.ParseInt(times[1], 10, 64)
	}

	result := timeReg.ReplaceAllString(queryString, replacement)

	/* 替换 tag */
	tagReg := `(?i)WHERE tbname IN \((.+)\) AND`
	conditionExpr := regexp.MustCompile(tagReg)
	if ok, _ := regexp.MatchString(tagReg, queryString); !ok {
		return "", 0, 0, nil
	}
	tagExprMatch := conditionExpr.FindStringSubmatch(result) // 获取 WHERE 后面的所有表达式，包括谓词和时间范围
	tagString := tagExprMatch[1]
	result = strings.ReplaceAll(result, tagString, replacement)

	tagString = strings.ReplaceAll(tagString, "\"", "")
	tagString = strings.ReplaceAll(tagString, "'", "")
	tagString = strings.ReplaceAll(tagString, " ", "")

	tags = strings.Split(tagString, ",")
	sort.Strings(tags)

	name := "hostname="
	if strings.Contains(tags[0], "truck") {
		name = "name="
	}
	for i := range tags {
		tags[i] = name + tags[i]
	}

	return result, startTime, endTime, tags
}

func DataTypeFromColumn(colTypes []uint8) []string {
	results := make([]string, len(colTypes))

	for i, col := range colTypes {
		if col == 1 {
			results[i] = "bool"
		} else if col >= 2 && col <= 5 || col == 9 {
			results[i] = "int64"
		} else if col >= 6 && col <= 7 {
			results[i] = "float64"
		} else if col == 8 {
			results[i] = "string"
		} else {
			results[i] = "string"
		}
	}

	return results
}

func ColumnTypeFromDatatype(datatypes []string) []uint8 {
	results := make([]uint8, len(datatypes))

	for i, dt := range datatypes {
		switch dt {
		case "bool":
			results[i] = 1
			break
		case "int64":
			results[i] = 9
			break
		case "float64":
			results[i] = 7
			break
		case "string":
			results[i] = 8
			break
		default:
			results[i] = 7
			break
		}
	}
	return results
}

// GetDataTypeArrayFromSF  从列名和数据类型组成的字符串中提取出每一列的数据类型
// time[int64],index[int64],location[string],randtag[string]
// 列名和数据类型都存放在数组中，顺序是固定的，不用手动排序，直接取出来就行
func GetDataTypeArrayFromSF(sfString string) []string {
	datatypes := make([]string, 0)
	columns := strings.Split(sfString, ",")

	for _, col := range columns {
		startIdx := strings.Index(col, "[") + 1
		endIdx := strings.Index(col, "]")
		datatypes = append(datatypes, col[startIdx:endIdx])
	}

	return datatypes
}

func TimeInt64ToString(number int64) string {
	layout := "2006-01-02 15:04:05.000"
	number *= 1e6
	t := time.Unix(0, number)
	timestamp := t.Format(layout)

	return timestamp
}

func TimeStringToInt64(timestamp string) int64 {
	layout := "2006-01-02 15:04:05.000"
	timeT, _ := time.Parse(layout, timestamp)
	numberN := timeT.UnixMilli()
	numberN -= 8 * time.Hour.Milliseconds()

	return numberN
}
