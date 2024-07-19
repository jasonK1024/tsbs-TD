package tdengine_client

import (
	"database/sql/driver"
	"fmt"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"log"
	"testing"
)

func TestTDengineTagQuery(t *testing.T) {
	queryString := `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= '2022-01-01 08:02:00.000' AND ts < '2022-01-01 08:10:00.000' PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`

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

	fmt.Println("data field count: ", data.FieldCount)
	fmt.Println("data AffectedRows: ", data.AffectedRows)
	fmt.Println("data Header Column Types: ", data.Header.ColTypes)
	fmt.Println("data Header Column Names: ", data.Header.ColNames)
	fmt.Println("data Header Column Length: ", data.Header.ColLength)
	fmt.Println("data Data: ", data.Data)

	fmt.Println(ResultToString(data))

}

func TestTDengineRemainQuery(t *testing.T) {
	//queryString := `SELECT _wstart AS ts, hostname, avg(usage_user), avg(usage_guest) FROM cpu WHERE hostname = 'host_1' AND ts >= '2022-01-01 08:02:00.000' AND ts < '2022-01-01 08:10:00.000' partition by hostname interval(1m) UNION ALL SELECT _wstart AS ts, hostname, avg(usage_user), avg(usage_guest) FROM cpu WHERE hostname = 'host_45' AND ts >= '2022-01-01 08:05:00.000' AND ts < '2022-01-01 08:11:00.000' partition by hostname interval(1m) UNION ALL SELECT _wstart AS ts, hostname, avg(usage_user), avg(usage_guest) FROM cpu WHERE hostname = 'host_21' AND ts >= '2022-01-01 08:02:00.000' AND ts < '2022-01-01 08:04:00.000'  partition by hostname interval(1m) order by hostname,ts`
	//queryString := `SELECT _wstart AS ts,hostname,avg(usage_user),avg(usage_guest) FROM cpu WHERE hostname = 'host_1' AND ts >= 1640995800000 AND ts < 1640995980000 partition by hostname interval(1m) UNION ALL SELECT _wstart AS ts,hostname,avg(usage_user),avg(usage_guest) FROM cpu WHERE hostname = 'host_23' AND ts >= 1640995800000 AND ts < 1640995980000 partition by hostname interval(1m) UNION ALL SELECT _wstart AS ts,hostname,avg(usage_user),avg(usage_guest) FROM cpu WHERE hostname = 'host_45' AND ts >= 1640995800000 AND ts < 1640995980000 partition by hostname interval(1m) order by hostname,ts`
	queryString := `SELECT _wstart AS ts,tbname,avg(velocity),avg(fuel_consumption),avg(grade) FROM readings WHERE tbname = 'r_truck_1' AND ts >= 1640995800000 AND ts < 1640995980000 partition by tbname interval(1m) UNION ALL SELECT _wstart AS ts,tbname,avg(velocity),avg(fuel_consumption),avg(grade) FROM readings WHERE tbname = 'r_truck_23' AND ts >= 1640995800000 AND ts < 1640995980000 partition by tbname interval(1m)  UNION ALL SELECT _wstart AS ts,tbname,avg(velocity),avg(fuel_consumption),avg(grade) FROM readings WHERE tbname = 'r_truck_67' AND ts >= 1640995800000 AND ts < 1640995980000 partition by tbname interval(1m) UNION ALL SELECT _wstart AS ts,tbname,avg(velocity),avg(fuel_consumption),avg(grade) FROM readings WHERE tbname = 'r_truck_45' AND ts >= 1640995800000 AND ts < 1640995980000 partition by tbname interval(1m)`
	host := "192.168.1.101"
	user := "root"
	pass := "taosdata"
	//db := "devops_small"
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
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(ResultToString(data))

}
