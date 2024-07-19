package main

import (
	"database/sql/driver"
	"fmt"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/tsbs/TDengine_Client/tdengine_client"
	"log"
	"strings"
	"time"
	"unsafe"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/taosdata/tsbs/internal/utils"
	"github.com/taosdata/tsbs/pkg/query"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/commonpool"
)

var totalRowLength int64 = 0

var (
	user   string
	pass   string
	host   string
	port   int
	runner *query.BenchmarkRunner
)

var (
	daemonUrls []string
	DBConns    []unsafe.Pointer
)

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("user", "root", "User to connect to TDengine")
	pflag.String("pass", "taosdata", "Password for the user connecting to TDengine")
	pflag.String("host", "", "TDengine host")
	pflag.Int("port", 6030, "TDengine Port")

	pflag.String("db", "", "tdengine or influxdb")

	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	host = viper.GetString("host")
	port = viper.GetInt("port")

	tdengine_client.DB = viper.GetString("db-name")
	tdengine_client.DbName = viper.GetString("db")

	// todo	多数据库
	daemonUrls = strings.Split(host, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'host' flag")
	}
	DBConns = make([]unsafe.Pointer, len(daemonUrls))
	for i := range daemonUrls {
		DBConns[i], _ = wrapper.TaosConnect(daemonUrls[i], user, pass, tdengine_client.DB, port)
	}

	runner = query.NewBenchmarkRunner(config)
}
func main() {
	runner.Run(&query.TDenginePool, newProcessor)

	fmt.Println("\navg row num per query result: ", totalRowLength/200000)
}

type queryExecutorOptions struct {
	debug         bool
	printResponse bool
}

type processor struct {
	db        *commonpool.Conn
	opts      *queryExecutorOptions
	workerNum int
}

func (p *processor) Init(workerNum int) {
	async.Init()
	//db, err := commonpool.GetConnection(user, pass, host, port)
	//if err != nil {
	//	panic(err)
	//}
	//dbName := runner.DatabaseName()
	//err = async.GlobalAsync.TaosExecWithoutResult(db.TaosConnection, "use "+dbName)
	//if err != nil {
	//	panic(err)
	//}
	//p.db = db

	splitHosts := strings.Split(host, ",")
	for i := range DBConns {
		db, err := commonpool.GetConnection(user, pass, splitHosts[i], port)
		if err != nil {
			panic(err)
		}
		dbName := runner.DatabaseName()
		err = async.GlobalAsync.TaosExecWithoutResult(db.TaosConnection, "use "+dbName)
		if err != nil {
			panic(err)
		}
		p.db = db
	}

	p.opts = &queryExecutorOptions{
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
	p.workerNum = workerNum
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	tq := q.(*query.TDengine)

	var _ []*async.ExecResult
	byteLength := uint64(0)
	hitKind := uint8(0)

	qry := string(tq.SqlQuery)
	// 拆分SQL和语义段
	sss := strings.Split(qry, ";")
	queryString := sss[0]
	segment := ""
	if len(sss) == 2 {
		segment = sss[1]
	}
	segment += ""

	start := time.Now()

	if p.opts.debug {
		fmt.Println(qry)
	}
	// todo
	//querys := strings.Split(qry, ";")
	//if len(querys) > 1 {
	//	var preQuerys []string
	//	for i := 0; i < len(querys); i++ {
	//		if len(querys[i]) > 0 {
	//			preQuerys = append(preQuerys, querys[i])
	//		}
	//	}
	//	if len(preQuerys) > 1 {
	//		for i := 0; i < len(preQuerys)-1; i++ {
	//			//err := async.GlobalAsync.TaosExecWithoutResult(p.db.TaosConnection, preQuerys[i])
	//			err := async.GlobalAsync.TaosExecWithoutResult(DBConns[p.workerNum%len(DBConns)], preQuerys[i])
	//			if err != nil {
	//				return nil, err
	//			}
	//		}
	//	}
	//	qry = querys[len(preQuerys)-1]
	//}
	//data, err := async.GlobalAsync.TaosExec(p.db.TaosConnection, qry, func(ts int64, precision int) driver.Value {
	//	return ts
	//})
	//fmt.Printf("workerNum: %d  len(DBConns): %d \n", p.workerNum, len(DBConns))

	if strings.EqualFold(tdengine_client.UseCache, "stscache") {
		_, byteLength, hitKind = tdengine_client.STsCacheClientSeg(DBConns[p.workerNum%len(DBConns)], queryString, segment)
		//intervalLen := 0
		//for _, data := range results {
		//	totalRowLength += int64(len(data.Data))
		//	intervalLen += len(data.Data)
		//}
		if p.opts.printResponse {
			//fmt.Println("data row length: ", intervalLen)
		}
	} else {
		data, err := async.GlobalAsync.TaosExec(DBConns[p.workerNum%len(DBConns)], queryString, func(ts int64, precision int) driver.Value {
			return ts
		})

		if err != nil {
			return nil, err
		}
		if p.opts.printResponse {
			//fmt.Printf("%#v\n", data)
			fmt.Println("data row length: ", len(data.Data))
		}
		totalRowLength += int64(len(data.Data))
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()

	//stat.Init(q.HumanLabelName(), took, byteLength, hitKind)
	stat.InitWithParam(q.HumanLabelName(), took, byteLength, hitKind)

	var err error
	return []*query.Stat{stat}, err
}

func newProcessor() query.Processor { return &processor{} }
