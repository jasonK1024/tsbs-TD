package timescaledb_client

import (
	"database/sql"
	"fmt"
	stscache "github.com/taosdata/tsbs/TDengine_Client/stscache_client"
	"log"
	"strings"
	"sync"
	"time"
)

const pgxDriver = "pgx"

var DB = "devops_small"
var DbName = ""

var STsCacheURL string

var UseCache = "db"

var MaxThreadNum = 64

const STRINGBYTELENGTH = 32

var CacheHash = make(map[string]int)

//var MapMutex sync.Mutex

// GetCacheHashValue 根据 fields 选择不同的 cache
func GetCacheHashValue(fields string) int {
	CacheNum := len(STsConnArr)
	if CacheNum == 0 {
		CacheNum = 1
	}
	if _, ok := CacheHash[fields]; !ok {
		value := len(CacheHash) % CacheNum
		CacheHash[fields] = value
	}
	hashValue := CacheHash[fields]
	return hashValue
}

var mtx sync.Mutex

var STsConnArr []*stscache.Client

func InitStsConnsArr(urlArr []string) []*stscache.Client {
	conns := make([]*stscache.Client, 0)
	for i := 0; i < len(urlArr); i++ {
		conns = append(conns, stscache.New(urlArr[i]))
	}
	return conns
}

// STsCacheClientSeg 传入语义段
func STsCacheClientSeg(conn *sql.DB, queryString string, semanticSegment string) ([][][]interface{}, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	/* 原始查询语句替换掉时间之后的的模版 */
	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString) // 时间用 '?' 代替

	//fmt.Println(semanticSegment)

	partialSegment := ""
	fields := ""
	metric := ""
	partialSegment, fields, metric = SplitPartialSegment(semanticSegment)

	// 用于 Set 的语义段
	starSegment := GetStarSegment(metric, partialSegment)

	// 分布式缓存
	// CacheIndex := GetCacheHashValue(fields)

	CacheIndex := 0
	fields = "time[int64],name[string]," + fields
	colLen := strings.Split(fields, ",")
	datatypes := DataTypeFromColumn(len(colLen))

	/* 向 cache 查询数据 */
	values, _, err := STsConnArr[CacheIndex].Get(semanticSegment, startTime, endTime)
	if err != nil { // 缓存未命中
		/* 向数据库查询全部数据，存入 cache */
		rows, err := conn.Query(queryString)
		if err != nil {
			log.Println(queryString)
		}

		var dataArray [][]interface{} = nil
		if !ResponseIsEmpty(rows) {
			dataArray = RowsToInterface(rows, len(datatypes))
			remainValues, numOfTab := ResponseInterfaceToByteArrayWithParams(dataArray, datatypes, tags, metric, partialSegment)
			//remainValues, numOfTab := ResponseToByteArrayWithParams(rows, datatypes, tags, metric, partialSegment)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: remainValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})

			if err != nil {
				//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
			} else {
				//log.Printf("STORED.")
			}

		} else { // 查数据库为空
			// todo 对于数据库中没有的数据，向cache中插入空值
			singleSemanticSegment := GetSingleSegment(metric, partialSegment, tags)
			emptyValues := make([]byte, 0)
			for _, ss := range singleSemanticSegment {
				zero, _ := Int64ToByteArray(int64(0))
				emptyValues = append(emptyValues, []byte(ss)...)
				emptyValues = append(emptyValues, []byte(" ")...)
				emptyValues = append(emptyValues, zero...)
			}

			numOfTab := int64(len(singleSemanticSegment))
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: emptyValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
			if err != nil {
				//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
			} else {
				//log.Printf("STORED.")
			}

			//fmt.Printf("\tdatabase miss 1:%s\n", queryString)
		}

		return [][][]interface{}{dataArray}, byteLength, hitKind

	} else { // 缓存部分命中或完全命中
		/* 把查询结果从字节流转换成 Response 结构 */
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

		if flagNum == 0 { // 全部命中
			//log.Printf("GET.")
			hitKind = 2
			//log.Printf("bytes get:%d\n", len(values))
			return convertedResponse, byteLength, hitKind

		} else { // 部分命中，剩余查询
			hitKind = 1

			//remainQueryString, minTime, maxTime := RemainQueryString(queryString, queryTemplate, flagArr, timeRangeArr, tagArr)
			remainQueryString, minTime, maxTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)

			// tagArr 是要查询的所有 tag ，remainTags 是部分命中的 tag
			remainTags := make([]string, 0)
			for i, tag := range tagArr {
				if flagArr[i] == 1 {
					remainTags = append(remainTags, fmt.Sprintf("%s=%s", tag[0], tag[1]))
				}

			}
			//fmt.Println("\t", remainQueryString)

			// 太小的剩余查询区间直接略过
			if maxTime-minTime <= int64(time.Minute.Seconds()) {
				hitKind = 2

				//fmt.Printf("\tremain resp too small 2:%s\n", queryString)

				return convertedResponse, byteLength, hitKind
			}

			remainRows, err := conn.Query(remainQueryString)
			//fmt.Println(remainQueryString)
			if err != nil {
				log.Println(remainQueryString)
			}

			// 查数据库为空
			if ResponseIsEmpty(remainRows) {
				hitKind = 2

				// todo 对于数据库中没有的数据，向cache中插入空值
				singleSemanticSegment := GetSingleSegment(metric, partialSegment, remainTags)
				emptyValues := make([]byte, 0)
				for _, ss := range singleSemanticSegment {
					zero, _ := Int64ToByteArray(int64(0))
					emptyValues = append(emptyValues, []byte(ss)...)
					emptyValues = append(emptyValues, []byte(" ")...)
					emptyValues = append(emptyValues, zero...)
				}

				numOfTab := int64(len(singleSemanticSegment))
				err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: emptyValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
				if err != nil {
					//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
				} else {
					//log.Printf("STORED.")
				}

				//fmt.Printf("\tdatabase miss 2:%s\n", remainQueryString)

				return convertedResponse, byteLength, hitKind
			}

			//if strings.Contains(remainQueryString, "90") {
			//	fmt.Println(remainQueryString)
			//}
			remainDataArray := RowsToInterface(remainRows, len(datatypes))
			remainByteArr, numOfTableR := ResponseInterfaceToByteArrayWithParams(remainDataArray, datatypes, remainTags, metric, partialSegment)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: remainByteArr, Time_start: minTime, Time_end: maxTime, NumOfTables: int64(numOfTableR)})
			//fmt.Println(minTime, maxTime)
			if err != nil {
				//log.Printf("partial get Set fail: %v\tvalue length:%d\tthread:%d\nQUERY STRING:\t%s\n", err, len(remainByteArr), workerNum, remainQueryString)
			} else {
				//log.Printf("bytes set:%d\n", len(remainByteArr))
			}

			// 剩余结果合并

			totalResp := MergeRemainResponse(convertedResponse, remainDataArray, timeRangeArr)

			return totalResp, byteLength, hitKind
		}

	}

}
