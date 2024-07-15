package tdengine_client

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/tsbs/TDengine_Client/stscache_client"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"log"
	"strings"
	"testing"
)

func TestBoolToByteArray(t *testing.T) {
	bvs := []bool{true, false}
	expected := [][]byte{{1}, {0}}

	for i := range bvs {
		byteArr, err := BoolToByteArray(bvs[i])
		if err != nil {
			fmt.Println(err)
		} else {
			if !bytes.Equal(byteArr, expected[i]) {
				t.Errorf("byte array%b", byteArr)
				t.Errorf("exected:%b", expected[i])
			}
			fmt.Println(byteArr)
		}
	}

}

func TestByteArrayToBool(t *testing.T) {
	expected := []bool{true, false}
	byteArray := [][]byte{{1}, {0}}

	for i := range byteArray {
		b, err := ByteArrayToBool(byteArray[i])
		if err != nil {
			fmt.Println(err)
		} else {
			if b != expected[i] {
				t.Errorf("bool:%v", b)
				t.Errorf("expected:%v", expected[i])
			}
			fmt.Println(b)
		}
	}

}

func TestStringToByteArray(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		expected []byte
	}{
		{
			name:     "empty",
			str:      "",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "normal",
			str:      "SCHEMA ",
			expected: []byte{83, 67, 72, 69, 77, 65, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "white spaces",
			str:      "          ",
			expected: []byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:     "CRLF",
			str:      "a\r\ns\r\nd\r\n",
			expected: []byte{97, 13, 10, 115, 13, 10, 100, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:     "normal2",
			str:      "asd zxc",
			expected: []byte{97, 115, 100, 32, 122, 120, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "symbols",
			str:      "-=.,/\\][()!@#$%^&*?\":",
			expected: []byte{45, 61, 46, 44, 47, 92, 93, 91, 40, 41, 33, 64, 35, 36, 37, 94, 38, 42, 63, 34, 58, 0, 0, 0, 0},
		},
		{
			name:     "length out of range(25)",
			str:      "AaaaBbbbCcccDdddEeeeFfffGggg",
			expected: []byte{65, 97, 97, 97, 66, 98, 98, 98, 67, 99, 99, 99, 68, 100, 100, 100, 69, 101, 101, 101, 70, 102, 102, 102, 71},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byteArray := StringToByteArray(tt.str)

			if !bytes.Equal(byteArray, tt.expected) {
				t.Errorf("byte array:%d", byteArray)
				t.Errorf("expected:%b", tt.expected)
			}

			//fmt.Printf("expected:%d\n", tt.expected)
		})
	}

}

func TestByteArrayToString(t *testing.T) {
	tests := []struct {
		name      string
		expected  string
		byteArray []byte
	}{
		{
			name:      "empty",
			expected:  "",
			byteArray: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "normal",
			expected:  "SCHEMA ",
			byteArray: []byte{83, 67, 72, 69, 77, 65, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "white spaces",
			expected:  "          ",
			byteArray: []byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:      "CRLF",
			expected:  "a\r\ns\r\nd\r\n",
			byteArray: []byte{97, 13, 10, 115, 13, 10, 100, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:      "normal2",
			expected:  "asd zxc",
			byteArray: []byte{97, 115, 100, 32, 122, 120, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "symbols",
			expected:  "-=.,/\\][()!@#$%^&*?\":",
			byteArray: []byte{45, 61, 46, 44, 47, 92, 93, 91, 40, 41, 33, 64, 35, 36, 37, 94, 38, 42, 63, 34, 58, 0, 0, 0, 0},
		},
		{
			name:      "length out of range(25)",
			expected:  "AaaaBbbbCcccDdddEeeeFfffG",
			byteArray: []byte{65, 97, 97, 97, 66, 98, 98, 98, 67, 99, 99, 99, 68, 100, 100, 100, 69, 101, 101, 101, 70, 102, 102, 102, 71},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := ByteArrayToString(tt.byteArray)

			if strings.Compare(str, tt.expected) != 0 {
				t.Errorf("string:%s", str)
				t.Errorf("expected:%s", tt.expected)
			}

			fmt.Printf("string:%s\n", str)
		})
	}
}

func TestInt64ToByteArray(t *testing.T) {
	numbers := []int64{123, 2000300, 100020003000, 10000200030004000, 101001000100101010, 9000800070006000500, 1566088200000000000}
	expected := [][]byte{
		{123, 0, 0, 0, 0, 0, 0, 0},
		{172, 133, 30, 0, 0, 0, 0, 0},
		{184, 32, 168, 73, 23, 0, 0, 0},
		{32, 163, 120, 2, 33, 135, 35, 0},
		{146, 251, 236, 220, 223, 211, 102, 1},
		{116, 203, 4, 179, 249, 67, 233, 124},
		{0, 80, 238, 159, 235, 220, 187, 21},
	}

	for i := range numbers {
		bytesArray, err := Int64ToByteArray(numbers[i])
		if err != nil {
			fmt.Errorf(err.Error())
		}
		if !bytes.Equal(bytesArray, expected[i]) {
			t.Errorf("byte array:%d", bytesArray)
			t.Errorf("expected:%d", expected[i])
		}
		//fmt.Printf("bytesArray:%d\n", bytesArray)
		//fmt.Printf("expected:%d\n", expected[i])
	}
}

func TestByteArrayToInt64(t *testing.T) {
	expected := []int64{0, 0, 123, 2000300, 100020003000, 10000200030004000, 101001000100101010, 9000800070006000500}
	byteArrays := [][]byte{
		{0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{123, 0, 0, 0, 0, 0, 0, 0},
		{172, 133, 30, 0, 0, 0, 0, 0},
		{184, 32, 168, 73, 23, 0, 0, 0},
		{32, 163, 120, 2, 33, 135, 35, 0},
		{146, 251, 236, 220, 223, 211, 102, 1},
		{116, 203, 4, 179, 249, 67, 233, 124},
	}

	for i := range byteArrays {
		number, err := ByteArrayToInt64(byteArrays[i])
		if err != nil {
			fmt.Printf(err.Error())
		}
		if number != expected[i] {
			t.Errorf("number:%d", number)
			t.Errorf("expected:%d", expected[i])
		}
		fmt.Printf("number:%d\n", number)
	}

}

func TestFloat64ToByteArray(t *testing.T) {
	numbers := []float64{0, 123, 123.4, 12.34, 123.456, 1.2345, 12.34567, 123.456789, 123.4567890, 0.00}
	expected := [][]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 192, 94, 64},
		{154, 153, 153, 153, 153, 217, 94, 64},
		{174, 71, 225, 122, 20, 174, 40, 64},
		{119, 190, 159, 26, 47, 221, 94, 64},
		{141, 151, 110, 18, 131, 192, 243, 63},
		{169, 106, 130, 168, 251, 176, 40, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := range numbers {
		bytesArray, err := Float64ToByteArray(numbers[i])
		if err != nil {
			fmt.Println(err.Error())
		}
		if !bytes.Equal(bytesArray, expected[i]) {
			t.Errorf("byte array:%b", bytesArray)
			t.Errorf("expected:%b", expected[i])
		}
		//fmt.Printf("bytesArray:%d\n", bytesArray)
		//fmt.Printf("expected:%d\n", expected[i])
	}

}

func TestByteArrayToFloat64(t *testing.T) {
	expected := []float64{0, 123, 123.4, 12.34, 123.456, 1.2345, 12.34567, 123.456789, 123.4567890, 0.00, 0.0}
	byteArrays := [][]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 192, 94, 64},
		{154, 153, 153, 153, 153, 217, 94, 64},
		{174, 71, 225, 122, 20, 174, 40, 64},
		{119, 190, 159, 26, 47, 221, 94, 64},
		{141, 151, 110, 18, 131, 192, 243, 63},
		{169, 106, 130, 168, 251, 176, 40, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := range byteArrays {
		number, err := ByteArrayToFloat64(byteArrays[i])
		if err != nil {
			fmt.Println(err)
		}
		if number != expected[i] {
			t.Errorf("number:%f", number)
			t.Errorf("expected:%f", expected[i])
		}
		//fmt.Printf("number:%f\n", number)
	}

}

func TestResponseToByteArrayWithParams(t *testing.T) {
	queryString := `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= '2022-01-01 08:02:00.000' AND ts < '2022-01-01 08:10:00.000' PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,1m}`

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
	byteArray, _ := ResponseToByteArrayWithParams(data, datatypes, tags, metric, particalSegment)

	fmt.Println(byteArray)
	fmt.Println(string(byteArray))

	bytesPerLine := BytesPerLine(datatypes)
	fmt.Println("bytes Per line: ", bytesPerLine)
}

func TestByteArrayToResponseWithDatatype(t *testing.T) {
	queryString := `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= '2022-01-01 08:02:00.000' AND ts < '2022-01-01 08:10:00.000' PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`
	semanticSegment := `{(cpu.hostname=host_1)(cpu.hostname=host_23)(cpu.hostname=host_45)}#{usage_user[int64],usage_guest[int64],usage_nice[int64]}#{empty}#{mean,1m}`

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

	response, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

	for _, resp := range response {
		fmt.Println(ResultToString(resp))
	}

	fmt.Println("flag num: ", flagNum)
	fmt.Println("flag array: ", flagArr)
	fmt.Println("time range array: ", timeRangeArr)
	fmt.Println("tag array: ", tagArr)

	//flag num:  3
	//flag array:  [1 1 1]
	//time range array:  [[1640995800000 1640995980000] [1640995800000 1640995980000] [1640995800000 1640995980000]]
	//tag array:  [[hostname host_1] [hostname host_23] [hostname host_45]]

}
