package timescaledb_client

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/taosdata/tsbs/TimescaleDB_Client/stscache_client"
	"log"
	"testing"
)

func TestMergeResponseCPU(t *testing.T) {
	queryString := `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}`

	//queryString := `SELECT time as bucket,hostname,usage_user,usage_guest,usage_nice FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' AND usage_user > 30 AND usage_guest > 30 ORDER BY hostname,bucket`
	//semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{(usage_user>10[int64])(usage_guest>10[int64])}#{empty,empty}`

	host := "192.168.1.101"
	user := "postgres"
	pass := "Dell@123"
	db := "devops_small"
	port := "5432"
	driver := pgxDriver

	connectString := fmt.Sprintf("host=%s dbname=%s user=%s port=%s password=%s", host, db, user, port, pass)
	dbConn, err := sql.Open(driver, connectString)
	if err != nil {
		log.Fatal("TimescaleDB connection fail: ", err)
	}

	rows, err := dbConn.Query(queryString)
	if err != nil {
		log.Fatal("Query fail: ", err)
	}

	//fmt.Println("resp Set:")
	//fmt.Println(ResultToString(rows))

	col, _ := rows.Columns()
	colTypes := DataTypeFromColumn(len(col))
	tags := []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"}
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(rows, colTypes, tags, metric, particalSegment)

	var startTime int64 = 1641024000
	var endTime int64 = 1641027600
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

	endTime = 1641024000
	endTime = 1641031200
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, stscache_client.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(values))
	}

	response, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, colTypes)

	for _, resp := range response {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	remainQueryString, remainStartTime, remainEndTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
	fmt.Println("remain Query string: \n", remainQueryString)
	fmt.Println("remain Start time: ", TimeInt64ToString(remainStartTime))
	fmt.Println("remain End time: ", TimeInt64ToString(remainEndTime))

	rows, err = dbConn.Query(remainQueryString)
	if err != nil {
		log.Fatal("Query fail: ", err)
	}

	dataArray := RowsToInterface(rows, len(col))

	fmt.Println("remain Query result:\n", ResultInterfaceToString(dataArray, colTypes))

	fmt.Println("merge Result:")
	mergedResp := MergeRemainResponse(response, dataArray, timeRangeArr)
	for _, resp := range mergedResp {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}
}

func TestRemainQueryStringCPU(t *testing.T) {
	queryString := `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}`

	//queryString := `SELECT time as bucket,hostname,usage_user,usage_guest,usage_nice FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' AND usage_user > 30 AND usage_guest > 30 ORDER BY hostname,bucket`
	//semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{(usage_user>10[int64])(usage_guest>10[int64])}#{empty,empty}`

	host := "192.168.1.101"
	user := "postgres"
	pass := "Dell@123"
	db := "devops_small"
	port := "5432"
	driver := pgxDriver

	connectString := fmt.Sprintf("host=%s dbname=%s user=%s port=%s password=%s", host, db, user, port, pass)
	dbConn, err := sql.Open(driver, connectString)
	if err != nil {
		log.Fatal("TimescaleDB connection fail: ", err)
	}

	rows, err := dbConn.Query(queryString)
	if err != nil {
		log.Fatal("Query fail: ", err)
	}

	//fmt.Println("resp Set:")
	//fmt.Println(ResultToString(rows))

	col, _ := rows.Columns()
	colTypes := DataTypeFromColumn(len(col))
	tags := []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"}
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(rows, colTypes, tags, metric, particalSegment)

	var startTime int64 = 1641024000
	var endTime int64 = 1641027600
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

	endTime = 1641024000
	endTime = 1641031200
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, stscache_client.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(values))
	}

	response, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, colTypes)

	for _, resp := range response {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	remainQueryString, remainStartTime, remainEndTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
	fmt.Println("remain Query string: \n", remainQueryString)
	fmt.Println("remain Start time: ", TimeInt64ToString(remainStartTime))
	fmt.Println("remain End time: ", TimeInt64ToString(remainEndTime))

	rows, err = dbConn.Query(remainQueryString)
	if err != nil {
		log.Fatal("Query fail: ", err)
	}

	fmt.Println("remain Query result:\n", ResultToString(rows))
}

func TestRemainQueryStringIoT(t *testing.T) {
	//queryString := `SELECT time_bucket('15 minute', time) as bucket,name,avg(velocity),avg(fuel_consumption),avg(grade) FROM readings WHERE name IN ('truck_1','truck_45','truck_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' GROUP BY name,bucket ORDER BY name,bucket`
	//semanticSegment := `{(readings.name=truck_1)(readings.name=truck_23)(readings.name=truck_45)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{empty}#{mean,15m}`

	queryString := `SELECT time as bucket,name,velocity,fuel_consumption,grade FROM readings WHERE name IN ('truck_1','truck_45','truck_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' AND velocity > 90 AND fuel_consumption > 40 ORDER BY name,bucket`
	semanticSegment := `{(readings.name=truck_1)(readings.name=truck_23)(readings.name=truck_45)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>90[int64])(fuel_consumption>40[int64])}#{empty,empty}`

	host := "192.168.1.101"
	user := "postgres"
	pass := "Dell@123"
	db := "iot_small"
	port := "5432"
	driver := pgxDriver

	connectString := fmt.Sprintf("host=%s dbname=%s user=%s port=%s password=%s", host, db, user, port, pass)
	dbConn, err := sql.Open(driver, connectString)
	if err != nil {
		log.Fatal("TimescaleDB connection fail: ", err)
	}

	rows, err := dbConn.Query(queryString)
	if err != nil {
		log.Fatal("Query fail: ", err)
	}

	//fmt.Println("resp Set:")
	//fmt.Println(ResultToString(rows))

	col, _ := rows.Columns()
	colTypes := DataTypeFromColumn(len(col))
	tags := []string{"name=truck_1", "name=truck_23", "name=truck_45"}
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(rows, colTypes, tags, metric, particalSegment)

	var startTime int64 = 1641024000
	var endTime int64 = 1641027600
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

	endTime = 1641024000
	endTime = 1641031200
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, stscache_client.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		fmt.Println("GET.")
		fmt.Printf("bytes get:%d\n", len(values))
	}

	response, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, colTypes)

	for _, resp := range response {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	remainQueryString, remainStartTime, remainEndTime := RemainQuery(queryTemplate, flagArr, timeRangeArr, tagArr)
	fmt.Println("remain Query string: \n", remainQueryString)
	fmt.Println("remain Start time: ", TimeInt64ToString(remainStartTime))
	fmt.Println("remain End time: ", TimeInt64ToString(remainEndTime))

	rows, err = dbConn.Query(remainQueryString)
	if err != nil {
		log.Fatal("Query fail: ", err)
	}

	fmt.Println("remain Query result:\n", ResultToString(rows))
}
