package main

import (
	"fmt"
	influxdb_client "github.com/taosdata/tsbs/InfluxDB-client/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/taosdata/tsbs/pkg/query"
)

var bytesSlash = []byte("/") // heap optimization

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	//tdengine_client     fasthttp.Client
	client     *http.Client
	Host       []byte
	HostString string
	uri        []byte
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
	chunkSize            uint64
	database             string
}

var httpClientOnce = sync.Once{}
var httpClient *http.Client

func getHttpClient() *http.Client {
	httpClientOnce.Do(func() {
		tr := &http.Transport{
			MaxIdleConnsPerHost: 1024,
		}
		httpClient = &http.Client{Transport: tr}
	})
	return httpClient
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string) *HTTPClient {
	return &HTTPClient{
		client:     getHttpClient(),
		Host:       []byte(host),
		HostString: host,
		uri:        []byte{}, // heap optimization
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions, workerNum int) (float64, uint64, uint8, error) {
	// populate uri from the reusable byte slice:
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.Host...)
	//w.uri = append(w.uri, bytesSlash...)
	w.uri = append(w.uri, q.Path...)
	w.uri = append(w.uri, []byte("&db="+url.QueryEscape(opts.database))...)
	if opts.chunkSize > 0 {
		s := fmt.Sprintf("&chunked=true&chunk_size=%d", opts.chunkSize)
		w.uri = append(w.uri, []byte(s)...)
	}

	lag := float64(0)
	byteLength := uint64(0)
	hitKind := uint8(0)
	err := error(nil)

	var resp *influxdb_client.Response

	//_, _, _, tags := influxdb_client.GetQueryTemplate(string(q.RawQuery))
	//partialSegment, _, metric := influxdb_client.GetPartialSegmentAndFields(string(q.RawQuery))
	//semanticSegment := influxdb_client.GetTotalSegment(metric, tags, partialSegment)
	//fmt.Printf("\t%s\n%s\n", string(q.RawQuery), semanticSegment)

	// Perform the request while tracking latency:

	sss := strings.Split(string(q.RawQuery), ";")
	queryString := sss[0]
	segment := ""
	if len(sss) == 2 {
		segment = sss[1]
	}
	segment += ""
	//fmt.Println("\tSql: ", queryString)
	//fmt.Println("\tSegment: ", segment)

	start := time.Now() // 发送请求之前的时间

	//log.Println(string(q.RawQuery))
	if strings.EqualFold(influxdb_client.UseCache, "stscache") {
		//
		//_, byteLength, hitKind = influxdb_client.STsCacheClient(DBConn[workerNum%len(DBConn)], string(q.RawQuery))

		if len(sss) == 1 {
			_, byteLength, hitKind = influxdb_client.STsCacheClient(DBConn[workerNum%len(DBConn)], queryString)
		} else {
			//fmt.Println("seg")
			_, byteLength, hitKind = influxdb_client.STsCacheClientSeg(DBConn[workerNum%len(DBConn)], queryString, segment)
		}

	} else if strings.EqualFold(influxdb_client.UseCache, "tscache") {

		_, byteLength, hitKind = influxdb_client.TSCacheClient(DBConn[workerNum%len(DBConn)], queryString)

	} else { // database

		qry := influxdb_client.NewQuery(queryString, influxdb_client.DB, "s")
		//resp, err := DBConn[workerNum%len(DBConn)].Query(qry)
		resp, err = DBConn[workerNum%len(DBConn)].Query(qry)
		if err != nil {
			panic(err)
		}

		//values := client.ResponseToByteArray(resp, string(q.RawQuery))
		////client.TotalGetByteLength += uint64(len(values))
		//log.Println(len(values))
		//byteLength = uint64(len(values))
		//hitKind = 0
		//log.Println(len(byteArr))

		//populate a request with data from the Query:
		//req, err := http.NewRequest(string(q.Method), string(w.uri), nil)
		////log.Println(string(w.uri))
		//if err != nil {
		//	panic(err)
		//}
		//resp, err := w.client.Do(req) // 向服务器发送 HTTP 请求，获取响应
		//if err != nil {
		//	panic(err)
		//}
		////var b []byte
		////log.Println(resp.Body.Read(b))
		////client.TotalGetByteLength += uint64(resp.ContentLength)
		//defer resp.Body.Close() // 延迟处理，关闭响应体
		//
		//if resp.StatusCode != http.StatusOK {
		//	panic("http request did not return status 200 OK")
		//}
		//
		//var body []byte
		//body, err = ioutil.ReadAll(resp.Body) // 获取查询结果
		//
		//if err != nil {
		//	panic(err)
		//}
		//if opts != nil {
		//	// Print debug messages, if applicable:
		//	switch opts.Debug {
		//	case 1:
		//		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
		//	case 2:
		//		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		//	case 3:
		//		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		//		fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
		//	case 4:
		//		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		//		fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
		//		fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(body))
		//	default:
		//	}

		// Pretty print JSON responses, if applicable:
		// if opts.PrettyPrintResponses {
		//	// Assumes the response is JSON! This holds for Influx
		//	// and Elastic.
		//
		//	prefix := fmt.Sprintf("ID %d: ", q.GetID())
		//	var v interface{}
		//	var line []byte
		//	full := make(map[string]interface{})
		//	full["influxql"] = string(q.RawQuery)
		//	json.Unmarshal(body, &v)
		//	full["response"] = v
		//	line, err = json.MarshalIndent(full, prefix, "  ")
		//	if err != nil {
		//		//return
		//		panic(err)
		//	}
		//	fmt.Println(string(line) + "\n")
		//}
		//}
	}

	//client.FatcacheClient(string(q.RawQuery))

	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds	// 计算出延迟	，查询请求发送前后的时间差	作为返回值

	rowNum := 0
	if opts.PrettyPrintResponses {
		for _, result := range resp.Results {
			for _, series := range result.Series {
				rowNum += len(series.Values)
			}
		}
		fmt.Println("data row number: ", rowNum)
	}

	return lag, byteLength, hitKind, err
}
