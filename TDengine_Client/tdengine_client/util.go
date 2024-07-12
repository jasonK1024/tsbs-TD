package tdengine_client

import (
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
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

func ColumnDataType(colTypes []uint8) []string {
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

func TimeInt64ToString(number int64) string {
	layout := "2006-01-02 15:04:05.000"
	number *= 1e6
	t := time.Unix(0, number).UTC()
	timestamp := t.Format(layout)

	return timestamp
}

func TimeStringToInt64(timestamp string) int64 {
	layout := "2006-01-02 15:04:05.000"
	timeT, _ := time.Parse(layout, timestamp)
	//numberN := timeT.UnixNano()
	numberN := timeT.Unix()

	return numberN
}
