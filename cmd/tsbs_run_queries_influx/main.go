// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"fmt"
	influxdb_client "github.com/taosdata/tsbs/InfluxDB-client/v2"
	"log"
	"strings"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/taosdata/tsbs/internal/utils"
	"github.com/taosdata/tsbs/pkg/query"
)

// Program option vars:
var (
	daemonUrls []string
	chunkSize  uint64
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

//	var DBConn, err = client.NewHTTPClient(client.HTTPConfig{
//		Addr: "http://192.168.1.103:8086",
//	})
var DBConn []influxdb_client.Client

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	var csvDaemonUrls string

	pflag.String("urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	pflag.Uint64("chunk-response-size", 0, "Number of series to chunk results into. 0 means no chunking.")
	pflag.String("db", "", "tdengine or influxdb")
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	csvDaemonUrls = viper.GetString("urls")
	chunkSize = viper.GetUint64("chunk-response-size")
	influxdb_client.DB = viper.GetString("db-name")
	influxdb_client.DbName = viper.GetString("db")

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	DBConn = make([]influxdb_client.Client, len(daemonUrls))
	// todo	多数据库
	for i := range daemonUrls {
		DBConn[i], _ = influxdb_client.NewHTTPClient(influxdb_client.HTTPConfig{Addr: daemonUrls[i]})
	}
	influxdb_client.TagKV = influxdb_client.GetTagKV(DBConn[0], influxdb_client.DB)
	influxdb_client.Fields = influxdb_client.GetFieldKeys(DBConn[0], influxdb_client.DB)

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	w         *HTTPClient
	opts      *HTTPClientDoOptions
	workerNum int
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.opts = &HTTPClientDoOptions{
		Debug:                runner.DebugLevel(),
		PrettyPrintResponses: runner.DoPrintResponses(),
		chunkSize:            chunkSize,
		database:             runner.DatabaseName(),
	}
	url := daemonUrls[workerNumber%len(daemonUrls)]
	p.w = NewHTTPClient(url)

	p.workerNum = workerNumber
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	if isWarm {
		return nil, nil
	}
	hq := q.(*query.HTTP)

	//println(string(hq.Path))
	//println(len(string(hq.Path)))
	//println(len(string(hq.RawQuery)))
	//println(string(hq.RawQuery))

	// todo
	lag, byteLength, hitKind, err := p.w.Do(hq, p.opts, p.workerNum)

	if err != nil {
		return nil, err
	}
	// todo
	stat := query.GetStat()
	stat.InitWithParam(q.HumanLabelName(), lag, byteLength, hitKind)
	return []*query.Stat{stat}, nil
}
