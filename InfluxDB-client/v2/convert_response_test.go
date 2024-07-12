package influxdb_client

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/taosdata/tsbs/InfluxDB-client/memcache"
	stscache "github.com/taosdata/tsbs/InfluxDB-client/memcache"
	"log"
	"strings"
	"testing"
)

func TestByteArrayToResponseWithDatatype(t *testing.T) {
	queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE "name"='truck_0' or "name"='truck_1' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:01:00Z' GROUP BY "name"`
	queryToBeGet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE "name"='truck_0' or "name"='truck_1' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:02:00Z' GROUP BY "name"`

	urlString := "192.168.1.101:11211"
	urlArr := strings.Split(urlString, ",")
	conns := InitStsConnsArr(urlArr)
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")

	query := NewQuery(queryToBeSet, "iot_small", "s")
	resp, _ := c.Query(query)

	fmt.Println()
	fmt.Printf("\tresp to be set:\n%s\n", resp.ToString())

	semanticSegment, fields := GetSemanticSegmentAndFields(queryToBeSet)
	startTime, endTime := GetQueryTimeRange(queryToBeSet)
	numberOfTable := GetNumOfTable(resp)
	fmt.Printf("111 set time: %s %s\n", TimeInt64ToString(startTime), TimeInt64ToString(endTime))
	values := ResponseToByteArray(resp, queryToBeSet)

	datatypes := GetDataTypeArrayFromSF(fields)
	fmt.Println("datatypes")
	for _, dt := range datatypes {
		fmt.Printf("%s ", dt)
	}
	fmt.Println()

	err := conns[0].Set(&stscache.Item{
		Key:         semanticSegment,
		Value:       values,
		Time_start:  startTime,
		Time_end:    endTime,
		NumOfTables: int64(numberOfTable),
	})
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("SET.")
		fmt.Printf("bytes set:%d\n", len(values))
	}

	qgst, qget := GetQueryTimeRange(queryToBeGet)
	fmt.Println()
	fmt.Printf("111 get time: %s %s\n", TimeInt64ToString(qgst), TimeInt64ToString(qget))
	getValues, _, err := conns[0].Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(getValues))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	cr, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponse(getValues)
	fmt.Println()
	fmt.Println("\tresp get:")
	fmt.Println(cr.ToString())

	//for i := 0; i < len(flagArr); i++ {
	//	fmt.Printf("number:%d:\tflag:%d\ttime range:%d-%d\ttag:%s:%s\n", i, flagArr[i], timeRangeArr[i][0], timeRangeArr[i][1], tagArr[i][0], tagArr[i][1])
	//}

	if flagNum > 0 {
		remainQueryString, minTime, maxTime := RemainQueryString(queryToBeSet, flagArr, timeRangeArr, tagArr)
		//fmt.Printf("remain query string:\n%s\n", remainQueryString)
		//fmt.Printf("remain min time:\n%d\t%s\n", minTime, TimeInt64ToString(minTime))
		//fmt.Printf("remain max time:\n%d\t%s\n", maxTime, TimeInt64ToString(maxTime))

		remainQuery := NewQuery(remainQueryString, "iot_small", "s")
		remainResp, _ := c.Query(remainQuery)
		remainByteArr := ResponseToByteArray(remainResp, queryToBeGet)
		numOfTableR := len(remainResp.Results[0].Series)

		fmt.Println()
		fmt.Printf("222 set time: %s %s\n", TimeInt64ToString(minTime), TimeInt64ToString(maxTime))
		fmt.Printf("\tresp to be set:\n%s\n", remainResp.ToString())

		err = conns[0].Set(&stscache.Item{
			Key:         semanticSegment,
			Value:       remainByteArr,
			Time_start:  minTime,
			Time_end:    maxTime,
			NumOfTables: int64(numOfTableR),
		})
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Println("SET.")
			fmt.Printf("bytes set:%d\n", len(remainByteArr))
		}
	}
	fmt.Println()
	qgst, qget = GetQueryTimeRange(queryToBeGet)
	fmt.Printf("222 get time: %s %s\n", TimeInt64ToString(qgst), TimeInt64ToString(qget))
	getValues, _, err = conns[0].Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(getValues))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	cr, flagNum, flagArr, timeRangeArr, tagArr = ByteArrayToResponse(getValues)
	fmt.Println()
	fmt.Println("\tresp get :")
	fmt.Println(cr.ToString())

}

func TestEmptyResponseToByteArray(t *testing.T) {
	queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE "name"='truck_0' or "name"='truck_1' AND TIME >= '2018-01-01T00:00:01Z' AND TIME < '2018-01-01T00:00:05Z' GROUP BY "name"`
	queryToBeGet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE "name"='truck_0' or "name"='truck_1' AND TIME >= '2018-01-01T00:00:02Z' AND TIME < '2018-01-01T00:00:04Z' GROUP BY "name"`

	urlString := "192.168.1.102:11211"
	urlArr := strings.Split(urlString, ",")
	conns := InitStsConnsArr(urlArr)
	log.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")

	query := NewQuery(queryToBeSet, "iot_small", "s")
	resp, _ := c.Query(query)

	log.Println(resp.ToString())

	semanticSegment := GetSemanticSegment(queryToBeSet)
	startTime, endTime := GetQueryTimeRange(queryToBeSet)
	seperateSemanticSegment := GetSeperateSemanticSegment(queryToBeSet)
	emptyValues := make([]byte, 0)
	for _, ss := range seperateSemanticSegment {
		zero, _ := Int64ToByteArray(int64(0))
		emptyValues = append(emptyValues, []byte(ss)...)
		emptyValues = append(emptyValues, []byte(" ")...)
		emptyValues = append(emptyValues, zero...)
	}

	numOfTab := int64(len(seperateSemanticSegment))
	err := conns[0].Set(&stscache.Item{
		Key:         semanticSegment,
		Value:       emptyValues,
		Time_start:  startTime,
		Time_end:    endTime,
		NumOfTables: int64(numOfTab),
	})
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("SET.")
		log.Printf("bytes set:%d\n", len(emptyValues))
	}

	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := conns[0].Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	cr, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponse(values)
	log.Printf("flag number:%d\n", flagNum)
	log.Printf("flag arr length:%d\n", len(flagArr))
	log.Printf("time range arr length:%d\n", len(timeRangeArr))
	log.Printf("tag arr length:%d\n", len(tagArr))

	log.Println(cr.ToString())

	//for i := 0; i < len(flagArr); i++ {
	//	fmt.Printf("number:%d:\tflag:%d\ttime range:%d-%d\ttag:%s:%s\n", i, flagArr[i], timeRangeArr[i][0], timeRangeArr[i][1], tagArr[i][0], tagArr[i][1])
	//}

	if flagNum > 0 {
		remainQueryString, minTime, maxTime := RemainQueryString(queryToBeSet, flagArr, timeRangeArr, tagArr)
		fmt.Printf("remain query string:\n%s\n", remainQueryString)
		fmt.Printf("remain min time:\n%d\t%s\n", minTime, TimeInt64ToString(minTime))
		fmt.Printf("remain max time:\n%d\t%s\n", maxTime, TimeInt64ToString(maxTime))

		remainQuery := NewQuery(remainQueryString, "iot", "s")
		remainResp, _ := c.Query(remainQuery)
		remainByteArr := ResponseToByteArray(remainResp, queryToBeGet)
		numOfTableR := len(remainResp.Results[0].Series)
		err = conns[0].Set(&stscache.Item{
			Key:         semanticSegment,
			Value:       remainByteArr,
			Time_start:  minTime,
			Time_end:    maxTime,
			NumOfTables: int64(numOfTableR),
		})
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("SET.")
			log.Printf("bytes set:%d\n", len(emptyValues))
		}
	}

}

func TestResponse_ToByteArray(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://10.170.48.244:8086",
		//Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"
	//queryMemcache := "SELECT randtag,index FROM h2o_quality limit 5"
	queryMemcache := "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag"
	qm := NewQuery(queryMemcache, MyDB, "s")
	respCache, _ := c.Query(qm)

	semanticSegment := GetSemanticSegment(queryMemcache)
	respCacheByte := ResponseToByteArray(respCache, queryMemcache)

	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTable := GetNumOfTable(respCache)

	fmt.Printf("byte array:\n%d\n\n", respCacheByte)

	var str string
	str = respCache.ToString()
	fmt.Printf("To be set:\n%s\n\n", str)

	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTable})

	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	}

	// 从缓存中获取值
	itemValues, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, memcache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		//log.Printf("Value: %s", item.Value)
	}

	fmt.Println("len:", len(itemValues))
	fmt.Printf("Get:\n")
	fmt.Printf("%d", itemValues)

	fmt.Printf("\nGet equals Set:%v\n", bytes.Equal(respCacheByte, itemValues[:len(itemValues)-2]))

	fmt.Println()

	/* 查询结果转换成字节数组的格式如下
		seprateSM1 len1
		values
		seprateSM2 len2
		values
		......

	seprateSM: 每张表的 tags 和整个查询的其余元数据组合成的 每张表的元数据	string，到空格符为止
	len: 每张表中数据的总字节数		int64，空格符后面的8个字节
	values: 数据，没有换行符
	*/
	// {(h2o_quality.randtag=1)}#{time[int64],index[int64]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 48]
	// 2019-08-18T00:06:00Z 66
	// 2019-08-18T00:18:00Z 91
	// 2019-08-18T00:24:00Z 29
	// {(h2o_quality.randtag=2)}#{time[int64],index[int64]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 16]
	// 2019-08-18T00:12:00Z 78
	// {(h2o_quality.randtag=3)}#{time[int64],index[int64]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 32]
	// 2019-08-18T00:00:00Z 85
	// 2019-08-18T00:30:00Z 75
}

func TestDetailQuery(t *testing.T) {
	queryToBeSet := `SELECT mean(latitude),mean(longitude),mean(elevation) FROM "readings" WHERE ("name" = 'truck_0' or "name" = 'truck_1' or "name" = 'truck_2' or "name" = 'truck_3' or "name" = 'truck_4' or "name" = 'truck_5' or "name" = 'truck_6' or "name" = 'truck_7' or "name" = 'truck_8' or "name" = 'truck_9' or "name" = 'truck_10' or "name" = 'truck_11' or "name" = 'truck_12' or "name" = 'truck_13' or "name" = 'truck_14' or "name" = 'truck_15' or "name" = 'truck_16' or "name" = 'truck_17' or "name" = 'truck_18' or "name" = 'truck_19' or "name" = 'truck_20' or "name" = 'truck_21' or "name" = 'truck_22' or "name" = 'truck_23' or "name" = 'truck_24' or "name" = 'truck_25' or "name" = 'truck_26' or "name" = 'truck_27' or "name" = 'truck_28' or "name" = 'truck_29') AND TIME >= '2022-12-17T12:00:00Z' AND TIME <= '2022-12-19T00:00:00Z' GROUP BY "name", time(10m)`
	urlString := "192.168.1.102:11211"
	urlArr := strings.Split(urlString, ",")
	conns := InitStsConnsArr(urlArr)
	log.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")

	query := NewQuery(queryToBeSet, "iot_small", "s")
	resp, _ := c.Query(query)
	semanticSegment := GetSemanticSegment(queryToBeSet)
	st, et := GetQueryTimeRange(queryToBeSet)
	numOfTable := len(resp.Results[0].Series)
	val := ResponseToByteArray(resp, queryToBeSet)
	err := conns[0].Set(&stscache.Item{
		Key:         semanticSegment,
		Value:       val,
		Time_start:  st,
		Time_end:    et,
		NumOfTables: int64(numOfTable),
	})
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("SET.")
		log.Printf("bytes set:%d\n", len(val))
	}
}

func TestIoTField(t *testing.T) {
	querySet := `SELECT mean(nominal_fuel_consumption) FROM "readings" WHERE TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:10:00Z' GROUP BY time(2m) `
	queryGet := `SELECT mean(nominal_fuel_consumption) FROM "readings" WHERE TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:20:00Z' GROUP BY time(2m) `

	cacheUrlString := "192.168.1.101:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_small"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	respSet, _, _ := STsCacheClient(dbConn, querySet)

	query1 := NewQuery(querySet, "iot_small", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp1:\n", resp1.ToString())
	}
	fmt.Println("\tresp set:")
	fmt.Println(respSet.ToString())

	respGet, _, _ := STsCacheClient(dbConn, queryGet)

	query2 := NewQuery(queryGet, "iot_small", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp2:\n", resp2.ToString())
	}
	fmt.Println("\tresp get:")
	fmt.Println(respGet.ToString())

}

func TestCPU(t *testing.T) {
	querySet := `SELECT mean(usage_user),mean(usage_system),mean(usage_idle) FROM "cpu" WHERE ("hostname" = 'host_0' or "hostname" = 'host_1') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "hostname",time(15m)`
	queryGet := `SELECT mean(usage_user),mean(usage_system),mean(usage_idle) FROM "cpu" WHERE ("hostname" = 'host_0' or "hostname" = 'host_1') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "hostname",time(15m)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "devops_small"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "devops_small")
	Fields = GetFieldKeys(c, "devops_small")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	respSet, _, _ := STsCacheClient(dbConn, querySet)

	query1 := NewQuery(querySet, "devops_small", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp1:\n", resp1.ToString())
	}
	fmt.Println("\tresp set:")
	fmt.Println(respSet.ToString())

	respGet, _, _ := STsCacheClient(dbConn, queryGet)

	query2 := NewQuery(queryGet, "devops_small", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp2:\n", resp2.ToString())
	}
	fmt.Println("\tresp get:")
	fmt.Println(respGet.ToString())

}

func TestCPU2(t *testing.T) {
	querySet := `SELECT usage_user,usage_system,usage_idle FROM "cpu" WHERE ("hostname" = 'host_0' or "hostname" = 'host_1') AND usage_user > 60 AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:01:00Z' GROUP BY "hostname"`
	queryGet := `SELECT usage_user,usage_system,usage_idle FROM "cpu" WHERE ("hostname" = 'host_0' or "hostname" = 'host_1') AND usage_user > 60 AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:02:00Z' GROUP BY "hostname"`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "devops_small"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "devops_small")
	Fields = GetFieldKeys(c, "devops_small")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	respSet, _, _ := STsCacheClient(dbConn, querySet)

	query1 := NewQuery(querySet, "devops_small", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp1:\n", resp1.ToString())
	}
	fmt.Println("\tresp set:")
	fmt.Println(respSet.ToString())

	respGet, _, _ := STsCacheClient(dbConn, queryGet)

	query2 := NewQuery(queryGet, "devops_small", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp2:\n", resp2.ToString())
	}
	fmt.Println("\tresp get:")
	fmt.Println(respGet.ToString())

}

func TestRemainResponseToByteArrayWithParams(t *testing.T) {
	querySet := `SELECT mean(latitude),mean(longitude),mean(elevation),mean(grade),mean(heading),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:10:00Z' group by time(1m)`
	queryGet := `SELECT mean(latitude),mean(longitude),mean(elevation),mean(grade),mean(heading),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:15:00Z' group by time(1m)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_medium"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_medium")
	Fields = GetFieldKeys(c, "iot_medium")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	respSet, _, _ := STsCacheClient(dbConn, querySet)

	query1 := NewQuery(querySet, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp1:\n", resp1.ToString())
	}
	fmt.Println("\tresp set:")
	fmt.Println(respSet.ToString())

	respGet, _, _ := STsCacheClient(dbConn, queryGet)

	query2 := NewQuery(queryGet, "iot_medium", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp2:\n", resp2.ToString())
	}
	fmt.Println("\tresp get:")
	fmt.Println(respGet.ToString())

}

func TestRemainResponseToByteArrayWithParams2(t *testing.T) {
	querySet := `SELECT velocity,fuel_consumption,grade FROM "readings" WHERE ("name" = 'truck_0' or "name"='truck_1') AND velocity < 10 AND fuel_consumption > 20 AND grade < 40 AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-02T01:30:00Z' GROUP BY "name"`
	queryGet := `SELECT velocity,fuel_consumption,grade FROM "readings" WHERE ("name" = 'truck_0' or "name"='truck_1') AND velocity < 10 AND fuel_consumption > 20 AND grade < 40 AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-02T02:00:00Z' GROUP BY "name"`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_medium"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_medium")
	Fields = GetFieldKeys(c, "iot_medium")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	//respSet, _, _ := STsCacheClient(dbConn, querySet)

	query1 := NewQuery(querySet, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp1:\n", resp1.ToString())
	}
	fmt.Println("\tresp set:")
	//fmt.Println(respSet.ToString())

	respGet, _, _ := STsCacheClient(dbConn, queryGet)

	query2 := NewQuery(queryGet, "iot_medium", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp2:\n", resp2.ToString())
	}
	fmt.Println("\tresp get:")
	fmt.Println(respGet.ToString())

}

func TestRemainQueryString(t *testing.T) {
	queryToBeSet := `SELECT mean(latitude),mean(longitude),mean(elevation),mean(grade),mean(heading),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:10:00Z' GROUP BY "name",time(1m)`
	queryToBeGet := `SELECT mean(latitude),mean(longitude),mean(elevation),mean(grade),mean(heading),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:15:00Z' GROUP BY "name",time(1m)`

	urlString := "192.168.1.102:11211"
	urlArr := strings.Split(urlString, ",")
	conns := InitStsConnsArr(urlArr)
	log.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")

	query := NewQuery(queryToBeSet, "iot_small", "s")
	resp, _ := c.Query(query)
	semanticSegment := GetSemanticSegment(queryToBeSet)
	st, et := GetQueryTimeRange(queryToBeSet)
	numOfTable := len(resp.Results[0].Series)
	val := ResponseToByteArray(resp, queryToBeSet)
	err := conns[0].Set(&stscache.Item{
		Key:         semanticSegment,
		Value:       val,
		Time_start:  st,
		Time_end:    et,
		NumOfTables: int64(numOfTable),
	})
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("SET.")
		log.Printf("bytes set:%d\n", len(val))
	}

	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := conns[0].Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	_, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponse(values)
	log.Printf("flag number:%d\n", flagNum)
	log.Printf("flag arr length:%d\n", len(flagArr))
	log.Printf("time range arr length:%d\n", len(timeRangeArr))
	log.Printf("tag arr length:%d\n", len(tagArr))

	//for i := 0; i < len(flagArr); i++ {
	//	fmt.Printf("number:%d:\tflag:%d\ttime range:%d-%d\ttag:%s:%s\n", i, flagArr[i], timeRangeArr[i][0], timeRangeArr[i][1], tagArr[i][0], tagArr[i][1])
	//}

	if flagNum > 0 {
		remainQueryString, minTime, maxTime := RemainQueryString(queryToBeSet, flagArr, timeRangeArr, tagArr)
		fmt.Printf("remain query string:\n%s\n", remainQueryString)
		fmt.Printf("remain min time:\n%d\t%s\n", minTime, TimeInt64ToString(minTime))
		fmt.Printf("remain max time:\n%d\t%s\n", maxTime, TimeInt64ToString(maxTime))

		remainQuery := NewQuery(remainQueryString, "iot_small", "s")
		remainResp, _ := c.Query(remainQuery)
		remainByteArr := ResponseToByteArray(remainResp, queryToBeGet)
		numOfTableR := len(remainResp.Results[0].Series)
		err = conns[0].Set(&stscache.Item{
			Key:         semanticSegment,
			Value:       remainByteArr,
			Time_start:  minTime,
			Time_end:    maxTime,
			NumOfTables: int64(numOfTableR),
		})
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("SET.")
			log.Printf("bytes set:%d\n", len(val))
		}
	}

}

func TestByteArrayToResponse(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{ // 	在由字节数组转换为结果类型时，谓词中的tag会被错误当作GROUP BY tag; 要用谓词tag的话最好把它也写进GROUP BY tag，这样就能保证转换前后结果的结构一致
			name:        "one table two columns",
			queryString: "SELECT index,location FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z' GROUP BY location limit 5",
			expected: "{(h2o_quality.location=coyote_creek)}#{index[int64],location[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 4 0]\r\n" +
				"[1566086400000000000 85]\r\n" +
				"[1566086760000000000 66]\r\n" +
				"......(共64条数据)",
		},
		{
			name:        "three tables two columns",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected: "{(h2o_quality.randtag=1)}#{index[int64]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 48]\r\n" +
				"[1566086760000000000 66]\r\n" +
				"[1566087480000000000 91]\r\n" +
				"[1566087840000000000 29]\r\n" +
				"{(h2o_quality.randtag=2)}#{index[int64]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 16]\r\n" +
				"[1566087120000000000 78]\r\n" +
				"{(h2o_quality.randtag=3)}#{index[int64]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 32]\r\n" +
				"[1566086400000000000 85]\r\n" +
				"[1566088200000000000 75]\r\n",
		},
		{ // length of key out of range(309 bytes) 不能超过250字节?
			name:        "three tables four columns",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 198]\r\n" +
				"[1566086760000000000 66 coyote_creek 1]\r\n" +
				"[1566087480000000000 91 coyote_creek 1]\r\n" +
				"[1566087840000000000 29 coyote_creek 1]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
				"[1566087120000000000 78 coyote_creek 2]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 132]\r\n" +
				"[1566086400000000000 85 coyote_creek 3]\r\n" +
				"[1566088200000000000 75 coyote_creek 3]\r\n",
		},
		{
			name:        "one table four columns",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{(randtag='2'[string])(index>50[int64])}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
				"[1566087120000000000 78 coyote_creek 2]\r\n",
		},
		{
			name:        "two tables four columns",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 198]\r\n" +
				"[1566086760000000000 66 coyote_creek 1]\r\n" +
				"[1566087480000000000 91 coyote_creek 1]\r\n" +
				"[1566087840000000000 29 coyote_creek 1]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
				"[1566087120000000000 78 coyote_creek 2]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 132]\r\n" +
				"[1566086400000000000 85 coyote_creek 3]\r\n" +
				"[1566088200000000000 75 coyote_creek 3]\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 连接数据库
			var c, _ = NewHTTPClient(HTTPConfig{
				//Addr: "http://10.170.48.244:8086",
				Addr: "http://localhost:8086",
			})
			MyDB := "NOAA_water_database"

			query := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(query)
			if err != nil {
				t.Errorf(err.Error())
			}

			/* Set() 存入cache */
			semanticSegment := GetSemanticSegment(tt.queryString)
			startTime, endTime := GetResponseTimeRange(resp)
			respString := resp.ToString()
			respCacheByte := ResponseToByteArray(resp, tt.queryString)
			tableNumbers := int64(len(resp.Results[0].Series))
			err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: tableNumbers})

			if err != nil {
				log.Fatalf("Set error: %v", err)
			}
			fmt.Println("Set successfully")

			/* Get() 从cache取出 */
			valueBytes, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
			if err == stscache.ErrCacheMiss {
				log.Printf("Key not found in cache")
			} else if err != nil {
				log.Fatalf("Error getting value: %v", err)
			}
			fmt.Println("Get successfully")

			/* 字节数组转换为结果类型 */
			respConverted, _, _, _, _ := ByteArrayToResponse(valueBytes)
			fmt.Println("Convert successfully")

			if strings.Compare(respString, respConverted.ToString()) != 0 {
				t.Errorf("fail to convert:different response")
			}
			fmt.Println("Same before and after convert")

			fmt.Println("resp:\n", *resp)
			fmt.Println("resp converted:\n", *respConverted)
			fmt.Println("resp:\n", resp.ToString())
			fmt.Println("resp converted:\n", respConverted.ToString())
			fmt.Println()
			fmt.Println()
		})
	}

}

func TestIoT(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "DiagnosticsLoad",
			queryString: `SELECT current_load,load_capacity FROM "diagnostics" WHERE "name"='truck_0' OR "name"='truck_1' AND TIME >= '2022-01-01T00:01:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "DiagnosticsFuel",
			queryString: `SELECT fuel_capacity,fuel_state,nominal_fuel_consumption FROM "diagnostics" WHERE TIME >= '2022-01-01T00:01:00Z' AND time < '2022-01-01T03:00:00Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "ReadingsPosition",
			queryString: `SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2022-01-01T00:01:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "ReadingsFuel",
			queryString: `SELECT fuel_capacity,fuel_consumption,nominal_fuel_consumption FROM "readings" WHERE TIME >= '2022-01-01T00:01:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "ReadingsVelocity",
			queryString: `SELECT velocity,heading FROM "readings" WHERE TIME >= '2022-01-01T00:01:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "ReadingsAvgFuelConsumption",
			queryString: `SELECT mean(fuel_consumption) FROM "readings" WHERE model='G-2000' AND TIME >= '2022-01-01T00:01:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "name",time(1h)`,
			expected:    "",
		},
		{
			name:        "ReadingsMaxVelocity",
			queryString: `SELECT max(velocity) FROM "readings" WHERE model='G-2000' AND  TIME >= '2022-01-01T00:01:00Z' AND TIME < '2022-01-01T03:00:00Z' GROUP BY "name",time(1h)`,
			expected:    "",
		},
		{
			name:        "real queries",
			queryString: `SELECT current_load,load_capacity FROM "diagnostics" WHERE ("name" = 'truck_1') AND TIME >= '2022-01-01T08:46:50Z' AND TIME < '2022-01-01T20:46:50Z' GROUP BY "name"`,
			expected:    "",
		},
	}

	var c, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://10.170.48.244:8086",
		//Addr: "http://localhost:8086",
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := NewQuery(tt.queryString, IOTDB, "s")

			resp, err := c.Query(query)

			if err != nil {
				t.Errorf(err.Error())
			}

			respString := resp.ToString()
			fmt.Println(respString)
			respBytes := ResponseToByteArray(resp, tt.queryString)

			respConvert, _, _, _, _ := ByteArrayToResponse(respBytes)
			respConvertString := respConvert.ToString()
			fmt.Println(respConvertString)
			fmt.Println(respBytes)
			fmt.Println(len(respBytes))
		})
	}
}

func TestBoolToByteArray(t *testing.T) {
	bvs := []bool{true, false}
	expected := [][]byte{{1}, {0}}

	for i := range bvs {
		byteArr, err := BoolToByteArray(bvs[i])
		if err != nil {
			fmt.Println(err)
		} else {
			if !bytes.Equal(byteArr, expected[i]) {
				t.Errorf("byte array%b", byteArr)
				t.Errorf("exected:%b", expected[i])
			}
			fmt.Println(byteArr)
		}
	}

}

func TestByteArrayToBool(t *testing.T) {
	expected := []bool{true, false}
	byteArray := [][]byte{{1}, {0}}

	for i := range byteArray {
		b, err := ByteArrayToBool(byteArray[i])
		if err != nil {
			fmt.Println(err)
		} else {
			if b != expected[i] {
				t.Errorf("bool:%v", b)
				t.Errorf("expected:%v", expected[i])
			}
			fmt.Println(b)
		}
	}

}

func TestStringToByteArray(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		expected []byte
	}{
		{
			name:     "empty",
			str:      "",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "normal",
			str:      "SCHEMA ",
			expected: []byte{83, 67, 72, 69, 77, 65, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "white spaces",
			str:      "          ",
			expected: []byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:     "CRLF",
			str:      "a\r\ns\r\nd\r\n",
			expected: []byte{97, 13, 10, 115, 13, 10, 100, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:     "normal2",
			str:      "asd zxc",
			expected: []byte{97, 115, 100, 32, 122, 120, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "symbols",
			str:      "-=.,/\\][()!@#$%^&*?\":",
			expected: []byte{45, 61, 46, 44, 47, 92, 93, 91, 40, 41, 33, 64, 35, 36, 37, 94, 38, 42, 63, 34, 58, 0, 0, 0, 0},
		},
		{
			name:     "length out of range(25)",
			str:      "AaaaBbbbCcccDdddEeeeFfffGggg",
			expected: []byte{65, 97, 97, 97, 66, 98, 98, 98, 67, 99, 99, 99, 68, 100, 100, 100, 69, 101, 101, 101, 70, 102, 102, 102, 71},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byteArray := StringToByteArray(tt.str)

			if !bytes.Equal(byteArray, tt.expected) {
				t.Errorf("byte array:%d", byteArray)
				t.Errorf("expected:%b", tt.expected)
			}

			fmt.Printf("expected:%d\n", tt.expected)
		})
	}

}

func TestByteArrayToString(t *testing.T) {
	tests := []struct {
		name      string
		expected  string
		byteArray []byte
	}{
		{
			name:      "empty",
			expected:  "",
			byteArray: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "normal",
			expected:  "SCHEMA ",
			byteArray: []byte{83, 67, 72, 69, 77, 65, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "white spaces",
			expected:  "          ",
			byteArray: []byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:      "CRLF",
			expected:  "a\r\ns\r\nd\r\n",
			byteArray: []byte{97, 13, 10, 115, 13, 10, 100, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:      "normal2",
			expected:  "asd zxc",
			byteArray: []byte{97, 115, 100, 32, 122, 120, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "symbols",
			expected:  "-=.,/\\][()!@#$%^&*?\":",
			byteArray: []byte{45, 61, 46, 44, 47, 92, 93, 91, 40, 41, 33, 64, 35, 36, 37, 94, 38, 42, 63, 34, 58, 0, 0, 0, 0},
		},
		{
			name:      "length out of range(25)",
			expected:  "AaaaBbbbCcccDdddEeeeFfffG",
			byteArray: []byte{65, 97, 97, 97, 66, 98, 98, 98, 67, 99, 99, 99, 68, 100, 100, 100, 69, 101, 101, 101, 70, 102, 102, 102, 71},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := ByteArrayToString(tt.byteArray)

			if strings.Compare(str, tt.expected) != 0 {
				t.Errorf("string:%s", str)
				t.Errorf("expected:%s", tt.expected)
			}

			fmt.Printf("string:%s\n", str)
		})
	}
}

func TestInt64ToByteArray(t *testing.T) {
	numbers := []int64{123, 2000300, 100020003000, 10000200030004000, 101001000100101010, 9000800070006000500, 1566088200000000000}
	expected := [][]byte{
		{123, 0, 0, 0, 0, 0, 0, 0},
		{172, 133, 30, 0, 0, 0, 0, 0},
		{184, 32, 168, 73, 23, 0, 0, 0},
		{32, 163, 120, 2, 33, 135, 35, 0},
		{146, 251, 236, 220, 223, 211, 102, 1},
		{116, 203, 4, 179, 249, 67, 233, 124},
		{0, 80, 238, 159, 235, 220, 187, 21},
	}

	for i := range numbers {
		bytesArray, err := Int64ToByteArray(numbers[i])
		if err != nil {
			fmt.Errorf(err.Error())
		}
		if !bytes.Equal(bytesArray, expected[i]) {
			t.Errorf("byte array:%d", bytesArray)
			t.Errorf("expected:%d", expected[i])
		}
		//fmt.Printf("bytesArray:%d\n", bytesArray)
		//fmt.Printf("expected:%d\n", expected[i])
	}
}

func TestByteArrayToInt64(t *testing.T) {
	expected := []int64{0, 0, 123, 2000300, 100020003000, 10000200030004000, 101001000100101010, 9000800070006000500}
	byteArrays := [][]byte{
		{0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{123, 0, 0, 0, 0, 0, 0, 0},
		{172, 133, 30, 0, 0, 0, 0, 0},
		{184, 32, 168, 73, 23, 0, 0, 0},
		{32, 163, 120, 2, 33, 135, 35, 0},
		{146, 251, 236, 220, 223, 211, 102, 1},
		{116, 203, 4, 179, 249, 67, 233, 124},
	}

	for i := range byteArrays {
		number, err := ByteArrayToInt64(byteArrays[i])
		if err != nil {
			fmt.Printf(err.Error())
		}
		if number != expected[i] {
			t.Errorf("number:%d", number)
			t.Errorf("expected:%d", expected[i])
		}
		fmt.Printf("number:%d\n", number)
	}

}

func TestFloat64ToByteArray(t *testing.T) {
	numbers := []float64{0, 123, 123.4, 12.34, 123.456, 1.2345, 12.34567, 123.456789, 123.4567890, 0.00}
	expected := [][]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 192, 94, 64},
		{154, 153, 153, 153, 153, 217, 94, 64},
		{174, 71, 225, 122, 20, 174, 40, 64},
		{119, 190, 159, 26, 47, 221, 94, 64},
		{141, 151, 110, 18, 131, 192, 243, 63},
		{169, 106, 130, 168, 251, 176, 40, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := range numbers {
		bytesArray, err := Float64ToByteArray(numbers[i])
		if err != nil {
			fmt.Println(err.Error())
		}
		if !bytes.Equal(bytesArray, expected[i]) {
			t.Errorf("byte array:%b", bytesArray)
			t.Errorf("expected:%b", expected[i])
		}
		//fmt.Printf("bytesArray:%d\n", bytesArray)
		//fmt.Printf("expected:%d\n", expected[i])
	}

}

func TestByteArrayToFloat64(t *testing.T) {
	expected := []float64{0, 123, 123.4, 12.34, 123.456, 1.2345, 12.34567, 123.456789, 123.4567890, 0.00, 0.0}
	byteArrays := [][]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 192, 94, 64},
		{154, 153, 153, 153, 153, 217, 94, 64},
		{174, 71, 225, 122, 20, 174, 40, 64},
		{119, 190, 159, 26, 47, 221, 94, 64},
		{141, 151, 110, 18, 131, 192, 243, 63},
		{169, 106, 130, 168, 251, 176, 40, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := range byteArrays {
		number, err := ByteArrayToFloat64(byteArrays[i])
		if err != nil {
			fmt.Println(err)
		}
		if number != expected[i] {
			t.Errorf("number:%f", number)
			t.Errorf("expected:%f", expected[i])
		}
		//fmt.Printf("number:%f\n", number)
	}

}

func TestTimeStringToInt64(t *testing.T) {
	timeStrings := []string{"2019-08-18T00:00:00Z", "2000-01-01T00:00:00Z", "2261-01-01T00:00:00Z"}
	expected := []int64{1566086400, 946684800, 9183110400}
	for i := range timeStrings {
		numberN := TimeStringToInt64(timeStrings[i])
		if numberN != expected[i] {
			t.Errorf("timestamp:%s", timeStrings[i])
			t.Errorf("number:%d", numberN)
		}
	}
}

func TestTimeInt64ToString(t *testing.T) {
	timeIntegers := []int64{1566086400, 946684800, 9183110400, 1640709000, -1,
		1640795430, 1640987960, 1640795400, 1640795390, 1640995110,
		1640763130, 1640788160, 1640534400, 1640707200,
		1514764800, 1671192000,
		1671278400, 1671408000}
	expected := []string{"2019-08-18T00:00:00Z", "2000-01-01T00:00:00Z", "2261-01-01T00:00:00Z", "2021-12-28T16:30:00Z", "1969-12-31T23:59:59Z",
		"2021-12-29T16:30:30Z", "2021-12-31T21:59:20Z", "2021-12-29T16:30:00Z",
		"2021-12-29T16:29:50Z", "2021-12-31T23:58:30Z", "2021-12-29T07:32:10Z", "2021-12-29T14:29:20Z", "2021-12-26T16:00:00Z", "2021-12-28T16:00:00Z",
		"2018-01-01T00:00:00Z", "2022-12-16T12:00:00Z",
		"2022-12-17T12:00:00Z", "2022-12-19T00:00:00Z",
	}
	for i := range timeIntegers {
		numberStr := TimeInt64ToString(timeIntegers[i])
		if numberStr != expected[i] {
			t.Errorf("time string:%s", numberStr)
			t.Errorf("expected:%s", expected[i])
		}
	}
}
