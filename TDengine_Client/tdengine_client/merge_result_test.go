package tdengine_client

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/tsbs/TDengine_Client/stscache_client"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"log"
	"testing"
)

func TestMergeResponseCPU(t *testing.T) {
	queryString := `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,1m}`

	//queryString := `SELECT _wstart AS ts,tbname,usage_user,usage_guest,usage_nice FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND usage_user > 10 AND usage_guest > 10 AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname ORDER BY tbname,ts`
	//semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{(usage_user>10[int64])(usage_guest>10[int64])}#{empty,empty}`

	host := "192.168.1.101"
	user := "root"
	pass := "taosdata"
	db := "devops_small"
	port := 6030
	TaosConnection, err := wrapper.TaosConnect(host, user, pass, db, port)
	if err != nil {
		log.Fatal("TDengine connection fail: ", err)
	}
	async.Init()

	data, err := async.GlobalAsync.TaosExec(TaosConnection, queryString, func(ts int64, precision int) driver.Value {
		return ts
	})

	datatypes := DataTypeFromColumn(data.Header.ColTypes)
	tags := []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"}
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(data, datatypes, tags, metric, particalSegment)

	var startTime int64 = 1640995320000
	var endTime int64 = 1640995800000
	stscacheConn := stscache_client.New("192.168.1.102:11211")
	err = stscacheConn.Set(&stscache_client.Item{
		Key:         semanticSegment,
		Value:       byteArray,
		Time_start:  startTime,
		Time_end:    endTime,
		NumOfTables: numberOfTable,
	})
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("SET.")
		fmt.Printf("bytes set:%d\n", len(byteArray))
	}

	endTime = 1640995560000
	endTime = 1640995980000
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, stscache_client.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(values))
	}

	convResp, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

	fmt.Println("\tresponse Get: ")
	for _, resp := range convResp {
		fmt.Println(ResultToString(resp))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	//remainQueryString, remainStartTime, remainEndTime := RemainQueryString(queryString, queryTemplate, flagArr, timeRangeArr, tagArr)
	remainQueryString, remainStartTime, remainEndTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
	fmt.Println("remain Query string: \n", remainQueryString)
	fmt.Println("remain Start time: ", remainStartTime)
	fmt.Println("remain End time: ", remainEndTime)

	remResp, err := async.GlobalAsync.TaosExec(TaosConnection, remainQueryString, func(ts int64, precision int) driver.Value {
		return ts
	})
	if err != nil {
		fmt.Println("remain query fail: ", err)
	}
	fmt.Println("remain Query result:\n", ResultToString(remResp))

	fmt.Println("merge Result:")
	mergedResp := MergeRemainResponse(convResp, remResp, timeRangeArr)
	for _, resp := range mergedResp {
		fmt.Println(ResultToString(resp))
	}
}

func TestRemainQueryStringCPU(t *testing.T) {
	//queryString := `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`
	//semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,1m}`

	queryString := `SELECT _wstart AS ts,tbname,usage_user,usage_guest,usage_nice FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND usage_user > 10 AND usage_guest > 10 AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname ORDER BY tbname,ts`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{(usage_user>10[int64])(usage_guest>10[int64])}#{empty,empty}`

	host := "192.168.1.101"
	user := "root"
	pass := "taosdata"
	db := "devops_small"
	port := 6030
	TaosConnection, err := wrapper.TaosConnect(host, user, pass, db, port)
	if err != nil {
		log.Fatal("TDengine connection fail: ", err)
	}
	async.Init()

	data, err := async.GlobalAsync.TaosExec(TaosConnection, queryString, func(ts int64, precision int) driver.Value {
		return ts
	})

	datatypes := DataTypeFromColumn(data.Header.ColTypes)
	tags := []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"}
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(data, datatypes, tags, metric, particalSegment)

	var startTime int64 = 1640995320
	var endTime int64 = 1640995800
	stscacheConn := stscache_client.New("192.168.1.102:11211")
	err = stscacheConn.Set(&stscache_client.Item{
		Key:         semanticSegment,
		Value:       byteArray,
		Time_start:  startTime,
		Time_end:    endTime,
		NumOfTables: numberOfTable,
	})
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("SET.")
		fmt.Printf("bytes set:%d\n", len(byteArray))
	}

	endTime = 1640995560
	endTime = 1640995980
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, stscache_client.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(values))
	}

	response, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

	fmt.Println("\tresponse Get: ")
	for _, resp := range response {
		fmt.Println(ResultToString(resp))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	//remainQueryString, remainStartTime, remainEndTime := RemainQueryString(queryString, queryTemplate, flagArr, timeRangeArr, tagArr)
	remainQueryString, remainStartTime, remainEndTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
	fmt.Println("remain Query string: \n", remainQueryString)
	fmt.Println("remain Start time: ", remainStartTime)
	fmt.Println("remain End time: ", remainEndTime)

	data, err = async.GlobalAsync.TaosExec(TaosConnection, remainQueryString, func(ts int64, precision int) driver.Value {
		return ts
	})
	if err != nil {
		fmt.Println("remain query fail: ", err)
	}
	fmt.Println("remain Query result:\n", ResultToString(data))
}

func TestRemainQueryStringIoT(t *testing.T) {
	queryString := `SELECT _wstart AS ts,tbname,avg(velocity),avg(fuel_consumption),avg(grade) FROM readings WHERE tbname IN ('r_truck_1','r_truck_45','r_truck_23') AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`
	semanticSegment := `{(readings.name=r_truck_1)(readings.name=r_truck_23)(readings.name=r_truck_45)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{empty}#{mean,1m}`

	//queryString := `SELECT _wstart AS ts,tbname,velocity,fuel_consumption,grade FROM readings WHERE tbname IN ('r_truck_1','r_truck_45','r_truck_23') AND velocity > 0 AND fuel_consumption > 0 AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname ORDER BY tbname,ts`
	//semanticSegment := `{(readings.name=r_truck_1)(readings.name=r_truck_23)(readings.name=r_truck_45)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>90[int64])(fuel_consumption>40[int64])}#{empty,empty}`

	host := "192.168.1.101"
	user := "root"
	pass := "taosdata"
	db := "iot_small"
	port := 6030
	TaosConnection, err := wrapper.TaosConnect(host, user, pass, db, port)
	if err != nil {
		log.Fatal("TDengine connection fail: ", err)
	}
	async.Init()

	data, err := async.GlobalAsync.TaosExec(TaosConnection, queryString, func(ts int64, precision int) driver.Value {
		return ts
	})

	datatypes := DataTypeFromColumn(data.Header.ColTypes)
	tags := []string{"name=r_truck_1", "name=r_truck_23", "name=r_truck_45"}
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(data, datatypes, tags, metric, particalSegment)

	var startTime int64 = 1640995320
	var endTime int64 = 1640995800
	stscacheConn := stscache_client.New("192.168.1.102:11211")
	err = stscacheConn.Set(&stscache_client.Item{
		Key:         semanticSegment,
		Value:       byteArray,
		Time_start:  startTime,
		Time_end:    endTime,
		NumOfTables: numberOfTable,
	})
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("SET.")
		fmt.Printf("bytes set:%d\n", len(byteArray))
	}

	endTime = 1640995560
	endTime = 1640995980
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, stscache_client.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(values))
	}

	response, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

	fmt.Println("\tresponse Get: ")
	for _, resp := range response {
		fmt.Println(ResultToString(resp))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	//remainQueryString, remainStartTime, remainEndTime := RemainQueryString(queryString, queryTemplate, flagArr, timeRangeArr, tagArr)
	remainQueryString, remainStartTime, remainEndTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
	fmt.Println("remain Query string: \n", remainQueryString)
	fmt.Println("remain Start time: ", remainStartTime)
	fmt.Println("remain End time: ", remainEndTime)

	data, err = async.GlobalAsync.TaosExec(TaosConnection, remainQueryString, func(ts int64, precision int) driver.Value {
		return ts
	})
	if err != nil {
		fmt.Println("remain query fail: ", err)
	}
	fmt.Println("remain Query result:\n", ResultToString(data))
}
