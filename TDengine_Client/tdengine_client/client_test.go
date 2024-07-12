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
	queryString := `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= '2022-01-01 08:02:00.000' AND ts < '2022-01-01 08:10:00.000' PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`

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

}
