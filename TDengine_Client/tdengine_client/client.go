package tdengine_client

import (
	"github.com/taosdata/driver-go/v3/wrapper"
	"sync"

	stscache "github.com/taosdata/tsbs/TDengine_Client/stscache_client"
)

const (
	host = "192.168.1.101"
	user = "root"
	pass = "taosdata"
	port = 6030
)

var DB = "devops_small"
var DbName = ""

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

//var CacheHashMtx sync.Mutex

// GetCacheHashValue 根据 fields 选择不同的 cache
func GetCacheHashValue(fields string) int {
	//CacheHashMtx.Lock()
	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}
	if _, ok := CacheHash[fields]; !ok {
		value := len(CacheHash) % CacheNum
		CacheHash[fields] = value
	}
	hashValue := CacheHash[fields]
	// CacheHashMtx.Unlock()
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
