package timescaledb_client

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/taosdata/tsbs/TimescaleDB_Client/stscache_client"
	"log"
	"sort"
	"strings"
	"testing"
)

func TestSTsCacheClientSeg(t *testing.T) {
	queryString1 := `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket`
	semanticSegment1 := `{(cpu.hostname=host_10)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}`

	queryString2 := `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 10:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket`
	semanticSegment2 := `{(cpu.hostname=host_10)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}`

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

	urlString := "192.168.1.102:11211"
	urlArr := strings.Split(urlString, ",")
	STsConnArr = InitStsConnsArr(urlArr)

	data1, _, hitKind1 := STsCacheClientSeg(dbConn, queryString1, semanticSegment1)
	data2, _, hitKind2 := STsCacheClientSeg(dbConn, queryString2, semanticSegment2)
	data3, _, hitKind3 := STsCacheClientSeg(dbConn, queryString2, semanticSegment2)

	colTypes := []string{"int64", "string", "float64", "float64", "float64"}
	fmt.Println("resp 1")
	for _, resp := range data1 {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}

	fmt.Println("resp 2")
	for _, resp := range data2 {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}

	fmt.Println("resp 3")
	for _, resp := range data3 {
		fmt.Println(ResultInterfaceToString(resp, colTypes))
	}

	fmt.Println("hitKind 1: ", hitKind1)
	fmt.Println("hitKind 2: ", hitKind2)
	fmt.Println("hitKind 3: ", hitKind3)
}

func TestTimescaleDBCPUTagQuery(t *testing.T) {
	queryString := `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE hostname IN ('host_1','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket`
	//queryString := `SELECT time as bucket,hostname,usage_user,usage_guest,usage_nice FROM cpu WHERE hostname IN ('host_1','host_45','host_23') AND time >= '2022-01-01 08:00:00+08' AND time < '2022-01-01 09:00:00+08' ORDER BY hostname,bucket`

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
	defer rows.Close()

	fmt.Println(ResultToString(rows))

}

func TestTimescaleDBRemainQuery(t *testing.T) {
	queryString := `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE (hostname='host_23' AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000') OR (hostname='host_45' AND time >= '2022-01-01 09:00:00 +0000' AND time < '2022-01-01 10:00:00 +0000') OR (hostname='host_1' AND time >= '2022-01-01 18:00:00 +0000' AND time < '2022-01-01 19:00:00 +0000') GROUP BY hostname,bucket ORDER BY hostname,bucket`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,15m}`

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
	defer rows.Close()

	//fmt.Println(ResultToString(rows))

	col, _ := rows.Columns()
	colTypes := DataTypeFromColumn(len(col))
	tags := []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"}
	sort.Strings(tags)
	particalSegment, _, metric := SplitPartialSegment(semanticSegment)
	byteArray, numberOfTable := ResponseToByteArrayWithParams(rows, colTypes, tags, metric, particalSegment)

	var startTime int64 = TimeStringToInt64("2022-01-01 08:00:00 +0000")
	var endTime int64 = TimeStringToInt64("2022-01-01 19:00:00 +0000")
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
	startTime = TimeStringToInt64("2022-01-01 18:00:00 +0000")
	endTime = TimeStringToInt64("2022-01-01 19:00:00 +0000")
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

}

func TestTimescaleDBIOTTagQuery(t *testing.T) {
	//queryString := `SELECT time_bucket('15 minute', time) as bucket,name,avg(latitude),avg(longitude),avg(elevation) FROM readings WHERE name IN ('truck_1','truck_45','truck_23') AND time >= '2022-01-01 08:00:00+08' AND time < '2022-01-01 09:00:00+08' GROUP BY name,bucket ORDER BY name,bucket`
	queryString := `SELECT time as bucket,name,latitude,longitude,elevation FROM readings WHERE name IN ('truck_1','truck_45','truck_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' ORDER BY name,bucket`

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
	defer rows.Close()

	fmt.Println(ResultToString(rows))

}
