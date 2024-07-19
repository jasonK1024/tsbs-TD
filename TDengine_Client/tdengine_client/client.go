package tdengine_client

import (
	"database/sql/driver"
	"fmt"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"log"
	"sync"
	"time"
	"unsafe"

	stscache "github.com/taosdata/tsbs/TDengine_Client/stscache_client"
)

const (
	host = "192.168.1.101"
	user = "root"
	pass = "taosdata"
	port = 6030
)

var DB = "devops_small"
var DbName = "influxdb"

var TaosConnection, _ = wrapper.TaosConnect(host, user, pass, DB, port)

var STsCacheURL string

// 查询模版对应除 SM 之外的部分语义段
var QueryTemplateToPartialSegment = make(map[string]string)
var SegmentToMetric = make(map[string]string)

var QueryTemplates = make(map[string]string) // 存放查询模版及其语义段；查询模板只替换了时间范围，语义段没变
var SegmentToFields = make(map[string]string)
var SeprateSegments = make(map[string][]string) // 完整语义段和单独语义段的映射

var UseCache = "db"

var MaxThreadNum = 64

const STRINGBYTELENGTH = 32

var CacheHash = make(map[string]int)

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
func STsCacheClientSeg(conn unsafe.Pointer, queryString string, semanticSegment string) ([]*async.ExecResult, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	/* 原始查询语句替换掉时间之后的的模版 */
	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString) // 时间用 '?' 代替

	partialSegment := ""
	fields := ""
	metric := ""
	partialSegment, fields, metric = SplitPartialSegment(semanticSegment)

	// 用于 Set 的语义段
	starSegment := GetStarSegment(metric, partialSegment)

	CacheIndex := GetCacheHashValue(fields)
	fields = "ts[int64],tbname[string]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	/* 向 cache 查询数据 */
	values, _, err := STsConnArr[CacheIndex].Get(semanticSegment, startTime, endTime)
	if err != nil { // 缓存未命中
		/* 向数据库查询全部数据，存入 cache */
		data, err := async.GlobalAsync.TaosExec(conn, queryString, func(ts int64, precision int) driver.Value {
			return ts
		})
		if err != nil {
			log.Println(queryString)
		}

		if !ResponseIsEmpty(data) {
			remainValues, numOfTab := ResponseToByteArrayWithParams(data, datatypes, tags, metric, partialSegment)

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

		return []*async.ExecResult{data}, byteLength, hitKind

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

			remainResp, err := async.GlobalAsync.TaosExec(conn, remainQueryString, func(ts int64, precision int) driver.Value {
				return ts
			})
			if err != nil {
				log.Println(remainQueryString)
			}

			// 查数据库为空
			if ResponseIsEmpty(remainResp) {
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

			//remainByteArr, numOfTableR, _ := RemainResponseToByteArrayWithParams(remainData, datatypes, tags, metric, partialSegment)
			remainByteArr, numOfTableR := ResponseToByteArrayWithParams(remainResp, datatypes, tags, metric, partialSegment)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: remainByteArr, Time_start: minTime, Time_end: maxTime, NumOfTables: int64(numOfTableR)})

			if err != nil {
				//log.Printf("partial get Set fail: %v\tvalue length:%d\tthread:%d\nQUERY STRING:\t%s\n", err, len(remainByteArr), workerNum, remainQueryString)
			} else {
				//log.Printf("bytes set:%d\n", len(remainByteArr))
			}

			// 剩余结果合并

			totalResp := MergeRemainResponse(convertedResponse, remainResp, timeRangeArr)

			return totalResp, byteLength, hitKind
		}

	}

}
