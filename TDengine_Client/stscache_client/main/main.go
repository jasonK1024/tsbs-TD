package main

import (
	"github.com/taosdata/tsbs/TDengine_Client/stscache_client"
	"log"
)

func main() {
	// 连接到memcache服务器
	mc := stscache_client.New("localhost:11213")

	// 在缓存中设置值
	err := mc.Set(&stscache_client.Item{Key: "mykey", Value: []byte("myvalue"), Expiration: 60, Time_start: 1314123, Time_end: 53421432123})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	}

	// 从缓存中获取值
	itemValues, item, err := mc.Get("mykey mykey1", 10, 20)
	if err == stscache_client.ErrCacheMiss {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("Value: %s", item.Value)
	}

	for i := range itemValues {
		//print(i)
		print(itemValues[i])

	}

	/*// 在缓存中删除值
	err = mc.Delete("mykey")
	if err != nil {
		log.Fatalf("Error deleting value: %v", err)
	}*/
}
